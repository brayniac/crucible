//! Native runtime server loop.

use crate::affinity::set_cpu_affinity;
use crate::config::Config;
use crate::connection::Connection;
use crate::metrics::{CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE, WorkerStats, WorkerStatsSnapshot};
use cache_core::Cache;
use io_driver::{CompletionKind, ConnId, Driver, IoDriver};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

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

    // Check if diagnostics are enabled via environment variable
    let diagnostics_enabled = std::env::var("CRUCIBLE_DIAGNOSTICS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let worker_config = WorkerConfig {
        listeners,
        backlog: 4096,
        read_buffer_size: 64 * 1024,
        buffer_size: config.uring.buffer_size,
        buffer_count: config.uring.buffer_count,
        sq_depth: config.uring.sq_depth,
        sqpoll: config.uring.sqpoll,
    };

    // Create per-worker stats
    let worker_stats: Arc<Vec<WorkerStats>> =
        Arc::new((0..num_workers).map(|_| WorkerStats::new()).collect());

    // Shutdown flag for diagnostics thread
    let shutdown = Arc::new(AtomicBool::new(false));

    // Start diagnostics thread if enabled
    let diagnostics_handle = if diagnostics_enabled {
        let stats = worker_stats.clone();
        let shutdown_flag = shutdown.clone();
        Some(
            std::thread::Builder::new()
                .name("diagnostics".to_string())
                .spawn(move || {
                    run_diagnostics(stats, shutdown_flag);
                })
                .expect("failed to spawn diagnostics thread"),
        )
    } else {
        None
    };

    // Spawn workers
    let mut handles = Vec::with_capacity(num_workers);
    for worker_id in 0..num_workers {
        let cpu_id = cpu_affinity
            .as_ref()
            .map(|cpus| cpus[worker_id % cpus.len()]);
        let cache = cache.clone();
        let worker_config = worker_config.clone();
        let stats = worker_stats.clone();

        let handle = std::thread::Builder::new()
            .name(format!("worker-{}", worker_id))
            .spawn(move || {
                if let Some(cpu) = cpu_id {
                    let _ = set_cpu_affinity(cpu);
                }

                if let Err(e) = run_worker(worker_id, worker_config, cache, &stats[worker_id]) {
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

    // Shutdown diagnostics thread
    shutdown.store(true, Ordering::SeqCst);
    if let Some(handle) = diagnostics_handle {
        let _ = handle.join();
    }

    Ok(())
}

/// Diagnostics thread that periodically reports per-worker stats.
fn run_diagnostics(stats: Arc<Vec<WorkerStats>>, shutdown: Arc<AtomicBool>) {
    let mut prev_snapshots: Vec<WorkerStatsSnapshot> = stats.iter().map(|s| s.snapshot()).collect();
    let mut last_report = Instant::now();
    let report_interval = Duration::from_secs(10);

    eprintln!("[diagnostics] Worker diagnostics enabled, reporting every 10s");

    while !shutdown.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_secs(1));

        if last_report.elapsed() >= report_interval {
            let current: Vec<WorkerStatsSnapshot> = stats.iter().map(|s| s.snapshot()).collect();

            eprintln!(
                "\n[diagnostics] === Worker Stats (last {}s) ===",
                report_interval.as_secs()
            );
            eprintln!(
                "{:>6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>12} {:>10}",
                "worker",
                "polls",
                "empty",
                "compls",
                "accepts",
                "recv",
                "send_rdy",
                "bytes_in",
                "bytes_out",
                "conns"
            );

            for (i, (curr, prev)) in current.iter().zip(prev_snapshots.iter()).enumerate() {
                let delta = curr.delta(prev);
                eprintln!(
                    "{:>6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>12} {:>10}",
                    i,
                    delta.poll_count,
                    delta.empty_polls,
                    delta.completions,
                    delta.accepts,
                    delta.recv_events,
                    delta.send_ready_events,
                    format_bytes(delta.bytes_received),
                    format_bytes(delta.bytes_sent),
                    curr.active_connections,
                );

                // Flag workers that might be stuck
                if delta.completions == 0 && curr.active_connections > 0 {
                    eprintln!(
                        "  ^ WARNING: Worker {} has {} connections but processed 0 completions!",
                        i, curr.active_connections
                    );
                }
                if delta.backpressure_events > 0 {
                    eprintln!(
                        "  ^ INFO: Worker {} hit backpressure {} times",
                        i, delta.backpressure_events
                    );
                }
            }

            prev_snapshots = current;
            last_report = Instant::now();
        }
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.1}GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.1}MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1}KB", bytes as f64 / 1_000.0)
    } else {
        format!("{}B", bytes)
    }
}

fn run_worker<C: Cache>(
    _worker_id: usize,
    config: WorkerConfig,
    cache: Arc<C>,
    stats: &WorkerStats,
) -> io::Result<()> {
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

    // Use Vec<Option<Connection>> indexed by ConnId for O(1) access.
    // The io-driver's ConnId is its internal Slab index, so indices are reused
    // when connections close. Using Vec with Option allows direct indexing while
    // handling sparse allocation efficiently.
    let mut connections: Vec<Option<Connection>> = Vec::with_capacity(4096);
    let mut recv_buf = vec![0u8; config.read_buffer_size];

    loop {
        let completions_count = driver.poll(Some(Duration::from_millis(1)))?;
        stats.inc_poll();
        if completions_count == 0 {
            stats.inc_empty_poll();
        }
        stats.inc_completions(completions_count as u64);

        for completion in driver.drain_completions() {
            match completion.kind {
                CompletionKind::Accept { conn_id, .. } => {
                    CONNECTIONS_ACCEPTED.increment();
                    CONNECTIONS_ACTIVE.increment();
                    stats.inc_accepts();

                    let idx = conn_id.as_usize();
                    // Grow the vec if needed
                    if idx >= connections.len() {
                        connections.resize_with(idx + 1, || None);
                    }
                    connections[idx] = Some(Connection::new(config.read_buffer_size));
                }

                CompletionKind::Recv { conn_id } => {
                    stats.inc_recv();
                    let idx = conn_id.as_usize();

                    // Check if connection exists and should read
                    let should_process = connections
                        .get(idx)
                        .and_then(|c| c.as_ref())
                        .map(|c| c.should_read())
                        .unwrap_or(false);

                    if !should_process {
                        stats.inc_backpressure();
                        continue;
                    }

                    let mut need_close = false;

                    'recv_loop: loop {
                        let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut()) else {
                            break;
                        };

                        match driver.recv(conn_id, &mut recv_buf) {
                            Ok(0) => {
                                need_close = true;
                                break;
                            }
                            Ok(n) => {
                                stats.add_bytes_received(n as u64);
                                conn.append_recv_data(&recv_buf[..n]);
                                conn.process(&*cache);

                                if conn.should_close() {
                                    need_close = true;
                                    break;
                                }

                                // Drain the write buffer as much as possible
                                while conn.has_pending_write() {
                                    let data = conn.pending_write_data();
                                    match driver.send(conn_id, data) {
                                        Ok(n) => {
                                            stats.add_bytes_sent(n as u64);
                                            conn.advance_write(n);
                                        }
                                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                                        Err(_) => {
                                            need_close = true;
                                            break 'recv_loop;
                                        }
                                    }
                                }

                                // Check backpressure after sending - if we can't keep up,
                                // stop reading and wait for SendReady to drain the buffer
                                if !conn.should_read() {
                                    stats.inc_backpressure();
                                    break;
                                }
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(_) => {
                                need_close = true;
                                break;
                            }
                        }
                    }

                    if need_close {
                        close_connection(&mut driver, &mut connections, conn_id, stats);
                    }
                }

                CompletionKind::SendReady { conn_id } => {
                    stats.inc_send_ready();
                    let idx = conn_id.as_usize();
                    let mut need_close = false;

                    // Loop to drain as much pending write data as possible
                    loop {
                        let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut()) else {
                            break;
                        };

                        if !conn.has_pending_write() {
                            break;
                        }

                        let data = conn.pending_write_data();
                        match driver.send(conn_id, data) {
                            Ok(n) => {
                                stats.add_bytes_sent(n as u64);
                                conn.advance_write(n);
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(_) => {
                                need_close = true;
                                break;
                            }
                        }
                    }

                    if need_close {
                        close_connection(&mut driver, &mut connections, conn_id, stats);
                        continue;
                    }

                    // After draining, check if we can now read and process pending data
                    let should_process = connections
                        .get(idx)
                        .and_then(|c| c.as_ref())
                        .map(|c| c.should_read())
                        .unwrap_or(false);

                    if !should_process {
                        continue;
                    }

                    // Process any pending read data now that we have room
                    {
                        let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut()) else {
                            continue;
                        };

                        conn.process(&*cache);

                        if conn.should_close() {
                            need_close = true;
                        }
                    }

                    if need_close {
                        close_connection(&mut driver, &mut connections, conn_id, stats);
                        continue;
                    }

                    // Try to send any newly generated responses
                    loop {
                        let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut()) else {
                            break;
                        };

                        if !conn.has_pending_write() {
                            break;
                        }

                        let data = conn.pending_write_data();
                        match driver.send(conn_id, data) {
                            Ok(n) => {
                                stats.add_bytes_sent(n as u64);
                                conn.advance_write(n);
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(_) => {
                                need_close = true;
                                break;
                            }
                        }
                    }

                    if need_close {
                        close_connection(&mut driver, &mut connections, conn_id, stats);
                    }
                }

                CompletionKind::Closed { conn_id } => {
                    close_connection(&mut driver, &mut connections, conn_id, stats);
                }

                CompletionKind::Error { conn_id, .. } => {
                    close_connection(&mut driver, &mut connections, conn_id, stats);
                }

                CompletionKind::ListenerError { error, .. } => {
                    eprintln!("Listener error: {}", error);
                }
            }
        }
    }
}

#[inline]
fn close_connection(
    driver: &mut Box<dyn IoDriver>,
    connections: &mut [Option<Connection>],
    conn_id: ConnId,
    stats: &WorkerStats,
) {
    let idx = conn_id.as_usize();
    if let Some(slot) = connections.get_mut(idx)
        && slot.take().is_some()
    {
        let _ = driver.close(conn_id);
        CONNECTIONS_ACTIVE.decrement();
        stats.inc_close();
    }
}
