//! Native runtime server loop.

use crate::affinity::set_cpu_affinity;
use crate::config::Config;
use crate::connection::Connection;
use crate::metrics::{
    CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE, CloseReason, WorkerStats, WorkerStatsSnapshot,
};
use cache_core::Cache;
use io_driver::{CompletionKind, ConnId, Driver, IoDriver, IoEngine};
use std::io;
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// Set the current thread's name (visible in ps, htop, etc).
/// Name is truncated to 15 chars (Linux limit).
#[cfg(target_os = "linux")]
fn set_thread_name(name: &str) {
    use std::ffi::CString;
    if let Ok(cname) = CString::new(name) {
        unsafe {
            libc::prctl(libc::PR_SET_NAME, cname.as_ptr());
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn set_thread_name(_name: &str) {
    // No-op on non-Linux
}

/// Configuration for each worker thread.
#[derive(Clone)]
struct WorkerConfig {
    io_engine: IoEngine,
    buffer_size: usize,
    buffer_count: u16,
    sq_depth: u32,
    sqpoll: bool,
    max_value_size: usize,
    allow_flush: bool,
}

/// Configuration for the acceptor thread.
#[derive(Clone)]
struct AcceptorConfig {
    io_engine: IoEngine,
    listeners: Vec<SocketAddr>,
    backlog: u32,
}

/// Run the native runtime server.
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

    // Check if diagnostics are enabled via environment variable
    let diagnostics_enabled = std::env::var("CRUCIBLE_DIAGNOSTICS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    // Compute global allow_flush: enabled if ANY listener allows it
    let allow_flush = config.listener.iter().any(|l| l.allow_flush);

    let worker_config = WorkerConfig {
        io_engine: config.io_engine,
        buffer_size: config.uring.buffer_size,
        buffer_count: config.uring.buffer_count,
        sq_depth: config.uring.sq_depth,
        sqpoll: config.uring.sqpoll,
        max_value_size: config.cache.max_value_size,
        allow_flush,
    };

    let acceptor_config = AcceptorConfig {
        io_engine: config.io_engine,
        listeners,
        backlog: 4096,
    };

    // Create per-worker stats
    let worker_stats: Arc<Vec<WorkerStats>> =
        Arc::new((0..num_workers).map(|_| WorkerStats::new()).collect());

    // Create channels for fd distribution (one per worker)
    // Using crossbeam for bounded channels with good performance
    let (senders, receivers): (Vec<_>, Vec<_>) = (0..num_workers)
        .map(|_| crossbeam_channel::bounded::<(RawFd, SocketAddr)>(1024))
        .unzip();

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
    let mut handles = Vec::with_capacity(num_workers + 1);
    for (worker_id, receiver) in receivers.into_iter().enumerate() {
        let cpu_id = cpu_affinity
            .as_ref()
            .map(|cpus| cpus[worker_id % cpus.len()]);
        let cache = cache.clone();
        let worker_config = worker_config.clone();
        let stats = worker_stats.clone();
        let worker_shutdown = shutdown.clone();

        let handle = std::thread::Builder::new()
            .name(format!("worker-{}", worker_id))
            .spawn(move || {
                set_thread_name(&format!("crucible-w{}", worker_id));
                if let Some(cpu) = cpu_id {
                    let _ = set_cpu_affinity(cpu);
                }

                if let Err(e) = run_worker(
                    worker_id,
                    worker_config,
                    cache,
                    &stats[worker_id],
                    receiver,
                    worker_shutdown,
                ) {
                    error!(worker_id, error = %e, "Worker error");
                }
            })
            .expect("failed to spawn worker thread");

        handles.push(handle);
    }

    // Spawn acceptor thread
    // Pin to first CPU in affinity list (shares with worker-0, but acceptor is lightweight)
    {
        let acceptor_cpu = cpu_affinity.as_ref().map(|cpus| cpus[0]);
        let shutdown_flag = shutdown.clone();
        let handle = std::thread::Builder::new()
            .name("acceptor".to_string())
            .spawn(move || {
                set_thread_name("crucible-accept");
                if let Some(cpu) = acceptor_cpu {
                    let _ = set_cpu_affinity(cpu);
                }
                if let Err(e) = run_acceptor(acceptor_config, senders, shutdown_flag) {
                    error!(error = %e, "Acceptor error");
                }
            })
            .expect("failed to spawn acceptor thread");

        handles.push(handle);
    }

    // Wait for shutdown signal (the main binary sets this via signal handler)
    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }

    info!("Shutdown signal received, draining connections...");

    // Wait for workers to drain with timeout
    let drain_start = Instant::now();
    let mut workers_stopped = vec![false; handles.len()];

    while drain_start.elapsed() < drain_timeout {
        let mut all_stopped = true;
        for (i, handle) in handles.iter().enumerate() {
            if !workers_stopped[i] && handle.is_finished() {
                workers_stopped[i] = true;
                debug!(worker_id = i, "Worker stopped");
            }
            if !workers_stopped[i] {
                all_stopped = false;
            }
        }

        if all_stopped {
            break;
        }

        std::thread::sleep(Duration::from_millis(100));
    }

    let active_conns = CONNECTIONS_ACTIVE.value();
    if active_conns > 0 {
        warn!(
            active_connections = active_conns,
            "Drain timeout reached, {} connections still active", active_conns
        );
    }

    // Join all threads (they should exit soon after shutdown flag is set)
    for handle in handles {
        let _ = handle.join();
    }

    // Shutdown diagnostics thread
    if let Some(handle) = diagnostics_handle {
        let _ = handle.join();
    }

    info!("Server shutdown complete");

    Ok(())
}

/// Diagnostics thread that periodically reports per-worker stats.
fn run_diagnostics(stats: Arc<Vec<WorkerStats>>, shutdown: Arc<AtomicBool>) {
    let mut prev_snapshots: Vec<WorkerStatsSnapshot> = stats.iter().map(|s| s.snapshot()).collect();
    let mut last_report = Instant::now();
    let report_interval = Duration::from_secs(10);

    debug!("Worker diagnostics enabled, reporting every 10s");

    while !shutdown.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_secs(1));

        if last_report.elapsed() >= report_interval {
            let current: Vec<WorkerStatsSnapshot> = stats.iter().map(|s| s.snapshot()).collect();

            eprintln!(
                "\n[diagnostics] === Worker Stats (last {}s) ===",
                report_interval.as_secs()
            );
            eprintln!(
                "{:>6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>12} {:>10}",
                "worker",
                "polls",
                "empty",
                "compls",
                "accepts",
                "ch_recv",
                "closes",
                "recv",
                "send_rdy",
                "bytes_in",
                "bytes_out",
                "conns"
            );

            for (i, (curr, prev)) in current.iter().zip(prev_snapshots.iter()).enumerate() {
                let delta = curr.delta(prev);
                eprintln!(
                    "{:>6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>12} {:>10}",
                    i,
                    delta.poll_count,
                    delta.empty_polls,
                    delta.completions,
                    delta.accepts,
                    delta.channel_receives,
                    delta.close_events,
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
                // Show close reason breakdown when there are closes
                if delta.close_events > 0 {
                    eprintln!(
                        "  ^ closes: {} (eof={} proto={} recv_err={} send_err={} closed={} error={})",
                        delta.close_events,
                        delta.closes_client_eof,
                        delta.closes_protocol,
                        delta.closes_recv_error,
                        delta.closes_send_error,
                        delta.closes_closed_event,
                        delta.closes_error_event,
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

/// Acceptor thread that accepts connections and distributes them to workers.
fn run_acceptor(
    config: AcceptorConfig,
    senders: Vec<crossbeam_channel::Sender<(RawFd, SocketAddr)>>,
    shutdown: Arc<AtomicBool>,
) -> io::Result<()> {
    // Use a small driver just for accepting connections
    let mut driver = Driver::builder()
        .engine(config.io_engine)
        .buffer_size(4096) // minimal, not used for data
        .buffer_count(16) // minimal
        .sq_depth(64) // smaller ring for accept-only
        .sqpoll(false) // no need for sqpoll on acceptor
        .build()?;

    // Start listening on all configured addresses using raw mode
    for addr in &config.listeners {
        driver.listen_raw(*addr, config.backlog)?;
    }

    let num_workers = senders.len();
    let mut next_worker = 0usize;

    while !shutdown.load(Ordering::Relaxed) {
        let _ = driver.poll(Some(Duration::from_millis(10)))?;

        for completion in driver.drain_completions() {
            match completion.kind {
                CompletionKind::AcceptRaw { raw_fd, addr, .. } => {
                    // Round-robin distribution to workers
                    // Both io_uring and mio emit AcceptRaw when using listen_raw()
                    let sender = &senders[next_worker];
                    next_worker = (next_worker + 1) % num_workers;

                    // Try to send the fd to the worker
                    if sender.try_send((raw_fd, addr)).is_err() {
                        // Channel full or disconnected, close the fd
                        unsafe { libc::close(raw_fd) };
                    }
                }
                CompletionKind::Accept { .. } => {
                    // Should not happen with listen_raw(), but handle gracefully
                    // by ignoring (connection will be closed when dropped)
                }
                CompletionKind::ListenerError { error, .. } => {
                    error!(error = %error, "Acceptor listener error");
                }
                _ => {}
            }
        }
    }

    Ok(())
}

fn run_worker<C: Cache>(
    worker_id: usize,
    config: WorkerConfig,
    cache: Arc<C>,
    stats: &WorkerStats,
    fd_receiver: crossbeam_channel::Receiver<(RawFd, SocketAddr)>,
    shutdown: Arc<AtomicBool>,
) -> io::Result<()> {
    // Set this thread's shard ID for metrics to avoid false sharing
    metrics::set_thread_shard(worker_id);

    let mut driver = Driver::builder()
        .engine(config.io_engine)
        .buffer_size(config.buffer_size)
        .buffer_count(config.buffer_count.next_power_of_two())
        .sq_depth(config.sq_depth)
        .sqpoll(config.sqpoll)
        .build()?;

    // Use Vec<Option<Connection>> indexed by ConnId for O(1) access.
    let mut connections: Vec<Option<Connection>> = Vec::with_capacity(4096);

    loop {
        // Check for shutdown signal
        let shutting_down = shutdown.load(Ordering::Relaxed);

        // Check for new connections from the acceptor (non-blocking)
        // Only accept new connections if we're not shutting down
        if !shutting_down {
            while let Ok((raw_fd, _addr)) = fd_receiver.try_recv() {
                match driver.register_fd(raw_fd) {
                    Ok(conn_id) => {
                        CONNECTIONS_ACCEPTED.increment();
                        CONNECTIONS_ACTIVE.increment();
                        stats.inc_channel_receive();

                        let idx = conn_id.slot();
                        if idx >= connections.len() {
                            connections.resize_with(idx + 1, || None);
                        }

                        // All connections use the same simple Connection type now
                        connections[idx] = Some(Connection::with_options(
                            config.max_value_size,
                            config.allow_flush,
                        ));
                    }
                    Err(_) => {
                        // Failed to register, fd is already closed by driver
                    }
                }
            }
        }

        let completions_count = driver.poll(Some(Duration::from_micros(100)))?;
        stats.inc_poll();
        if completions_count == 0 {
            stats.inc_empty_poll();
        }
        stats.inc_completions(completions_count as u64);

        for completion in driver.drain_completions() {
            match completion.kind {
                // Accept events can still come from mio fallback
                CompletionKind::Accept { conn_id, .. } => {
                    CONNECTIONS_ACCEPTED.increment();
                    CONNECTIONS_ACTIVE.increment();
                    stats.inc_accepts();

                    let idx = conn_id.slot();
                    if idx >= connections.len() {
                        connections.resize_with(idx + 1, || None);
                    }

                    connections[idx] = Some(Connection::with_options(
                        config.max_value_size,
                        config.allow_flush,
                    ));
                }

                // Unified recv handling using with_recv_buf
                CompletionKind::Recv { conn_id } => {
                    stats.inc_recv();
                    let idx = conn_id.slot();

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

                    let mut close_reason: Option<CloseReason> = None;
                    let mut bytes_received = 0u64;
                    let mut send_error = false;

                    // Process all commands in one call; process_from handles
                    // zero-copy GET responses internally via the send queue.
                    {
                        let result = driver.with_recv_buf(conn_id, &mut |buf| {
                            if let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut()) {
                                let initial_len = buf.len();
                                conn.process_from(buf, &*cache);
                                bytes_received += (initial_len - buf.len()) as u64;

                                if conn.should_close() {
                                    close_reason = Some(CloseReason::ProtocolClose);
                                }
                            }
                        });

                        // Check for EOF or other errors
                        if let Err(e) = result {
                            if e.kind() == io::ErrorKind::UnexpectedEof {
                                close_reason = Some(CloseReason::ClientEof);
                            } else if e.kind() != io::ErrorKind::NotFound {
                                close_reason = Some(CloseReason::RecvError);
                            }
                        }

                        // Send all accumulated response data using vectored I/O
                        if let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut())
                            && send_pending(&mut driver, conn, conn_id, stats).is_err()
                        {
                            send_error = true;
                        }
                    }

                    stats.add_bytes_received(bytes_received);

                    // Handle close if needed
                    if let Some(reason) = close_reason {
                        close_connection(&mut driver, &mut connections, conn_id, stats, reason);
                        continue;
                    }

                    if send_error {
                        close_connection(
                            &mut driver,
                            &mut connections,
                            conn_id,
                            stats,
                            CloseReason::SendError,
                        );
                    }
                }

                // RecvComplete is legacy - single-shot recv now emits Recv
                CompletionKind::RecvComplete { .. } => {}

                CompletionKind::SendReady { conn_id } => {
                    stats.inc_send_ready();
                    let idx = conn_id.slot();
                    let mut close_reason: Option<CloseReason> = None;

                    // Drain pending write data using vectored I/O
                    if let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut())
                        && send_pending(&mut driver, conn, conn_id, stats).is_err()
                    {
                        close_reason = Some(CloseReason::SendError);
                    }

                    if let Some(reason) = close_reason {
                        close_connection(&mut driver, &mut connections, conn_id, stats, reason);
                        continue;
                    }

                    // After draining, process any buffered recv data
                    let should_process = connections
                        .get(idx)
                        .and_then(|c| c.as_ref())
                        .map(|c| c.should_read())
                        .unwrap_or(false);

                    if !should_process {
                        continue;
                    }

                    // Process buffered data using same path as Recv completions
                    {
                        let result = driver.with_recv_buf(conn_id, &mut |buf| {
                            if let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut()) {
                                conn.process_from(buf, &*cache);

                                if conn.should_close() {
                                    close_reason = Some(CloseReason::ProtocolClose);
                                }
                            }
                        });

                        if result.is_err() || close_reason.is_some() {
                            // fall through to close handling below
                        }

                        // Send all accumulated response data using vectored I/O
                        if let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut())
                            && send_pending(&mut driver, conn, conn_id, stats).is_err()
                        {
                            close_reason = Some(CloseReason::SendError);
                        }
                    }

                    if let Some(reason) = close_reason {
                        close_connection(&mut driver, &mut connections, conn_id, stats, reason);
                    }
                }

                CompletionKind::Closed { conn_id } => {
                    close_connection(
                        &mut driver,
                        &mut connections,
                        conn_id,
                        stats,
                        CloseReason::ClosedEvent,
                    );
                }

                CompletionKind::Error { conn_id, error: _ } => {
                    close_connection(
                        &mut driver,
                        &mut connections,
                        conn_id,
                        stats,
                        CloseReason::ErrorEvent,
                    );
                }

                CompletionKind::ListenerError { error, .. } => {
                    error!(error = %error, "Listener error");
                }

                // AcceptRaw is only used by the acceptor thread, not workers
                CompletionKind::AcceptRaw { raw_fd, .. } => {
                    // This shouldn't happen in workers, close the fd
                    unsafe { libc::close(raw_fd) };
                }

                // UDP events not used in TCP server
                CompletionKind::UdpReadable { .. }
                | CompletionKind::RecvMsgComplete { .. }
                | CompletionKind::UdpWritable { .. }
                | CompletionKind::SendMsgComplete { .. }
                | CompletionKind::UdpError { .. } => {}
            }
        }

        // Check if we should exit (shutdown requested and no active connections)
        if shutting_down {
            let active = connections.iter().filter(|c| c.is_some()).count();
            if active == 0 {
                debug!(worker_id, "Worker exiting, no active connections");
                break;
            }
        }
    }

    Ok(())
}

/// Send all pending data for a connection using vectored I/O.
///
/// Returns `Ok(())` on success or WouldBlock, `Err(())` on send error.
#[inline]
fn send_pending(
    driver: &mut Box<dyn IoDriver>,
    conn: &mut Connection,
    conn_id: ConnId,
    stats: &WorkerStats,
) -> Result<(), ()> {
    while conn.has_pending_write() {
        let bufs = conn.collect_pending_writes();
        match driver.send_vectored_owned(conn_id, bufs) {
            Ok(n) => {
                stats.add_bytes_sent(n as u64);
                conn.advance_write(n);
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(_) => return Err(()),
        }
    }
    Ok(())
}

#[inline]
fn close_connection(
    driver: &mut Box<dyn IoDriver>,
    connections: &mut [Option<Connection>],
    conn_id: ConnId,
    stats: &WorkerStats,
    reason: CloseReason,
) {
    let idx = conn_id.slot();
    if let Some(slot) = connections.get_mut(idx)
        && slot.take().is_some()
    {
        let _ = driver.close(conn_id);
        CONNECTIONS_ACTIVE.decrement();
        stats.inc_close(reason);
    }
}
