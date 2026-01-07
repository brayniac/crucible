//! Native runtime server loop.

use crate::affinity::set_cpu_affinity;
use crate::config::Config;
use crate::connection::Connection;
use crate::metrics::{
    CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE, CloseReason, WorkerStats, WorkerStatsSnapshot,
};
use cache_core::Cache;
use io_driver::{BufferPool, CompletionKind, ConnId, Driver, IoDriver, IoEngine};
use std::io;
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

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
    /// Size of each buffer chunk in the pool
    pool_chunk_size: usize,
    /// Total pool size in bytes
    pool_total_size: usize,
    /// Assembly buffer size for fragmented messages
    assembly_size: usize,
    buffer_size: usize,
    buffer_count: u16,
    sq_depth: u32,
    sqpoll: bool,
}

/// Configuration for the acceptor thread.
#[derive(Clone)]
struct AcceptorConfig {
    io_engine: IoEngine,
    listeners: Vec<SocketAddr>,
    backlog: u32,
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
        io_engine: config.io_engine,
        pool_chunk_size: 32 * 1024,         // 32KB chunks
        pool_total_size: 128 * 1024 * 1024, // 128MB pool per worker
        assembly_size: 4 * 1024 * 1024,     // 4MB assembly buffer
        buffer_size: config.uring.buffer_size,
        buffer_count: config.uring.buffer_count,
        sq_depth: config.uring.sq_depth,
        sqpoll: config.uring.sqpoll,
    };

    let acceptor_config = AcceptorConfig {
        io_engine: config.io_engine,
        listeners,
        backlog: 4096,
    };

    // Create per-worker stats
    let worker_stats: Arc<Vec<WorkerStats>> =
        Arc::new((0..num_workers).map(|_| WorkerStats::new()).collect());

    // Shutdown flag for diagnostics and acceptor threads
    let shutdown = Arc::new(AtomicBool::new(false));

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

        let handle = std::thread::Builder::new()
            .name(format!("worker-{}", worker_id))
            .spawn(move || {
                set_thread_name(&format!("crucible-w{}", worker_id));
                if let Some(cpu) = cpu_id {
                    let _ = set_cpu_affinity(cpu);
                }

                if let Err(e) =
                    run_worker(worker_id, worker_config, cache, &stats[worker_id], receiver)
                {
                    eprintln!("Worker {} error: {}", worker_id, e);
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
                    eprintln!("Acceptor error: {}", e);
                }
            })
            .expect("failed to spawn acceptor thread");

        handles.push(handle);
    }

    // Wait for all threads
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
                    eprintln!("Acceptor listener error: {}", error);
                }
                _ => {}
            }
        }
    }

    Ok(())
}

/// Tracks pending recv buffer for io_uring submit_recv.
/// When we submit a recv, we "loan" a buffer to the kernel.
/// On RecvComplete, we get it back.
struct PendingRecv {
    buf_id: u16,
}

fn run_worker<C: Cache>(
    _worker_id: usize,
    config: WorkerConfig,
    cache: Arc<C>,
    stats: &WorkerStats,
    fd_receiver: crossbeam_channel::Receiver<(RawFd, SocketAddr)>,
) -> io::Result<()> {
    let mut driver = Driver::builder()
        .engine(config.io_engine)
        .buffer_size(config.buffer_size)
        .buffer_count(config.buffer_count.next_power_of_two())
        .sq_depth(config.sq_depth)
        .sqpoll(config.sqpoll)
        .build()?;

    // Create buffer pool for this worker
    let chunk_count = config.pool_total_size / config.pool_chunk_size;
    let mut pool = BufferPool::new(config.pool_chunk_size, chunk_count);

    // Assembly buffer for fragmented messages
    let mut assembly = vec![0u8; config.assembly_size];

    // Connections indexed by ConnId
    let mut connections: Vec<Option<Connection>> = Vec::with_capacity(4096);

    // Track pending recv buffers for io_uring (buffer loaned to kernel)
    let mut pending_recvs: Vec<Option<PendingRecv>> = Vec::with_capacity(4096);

    loop {
        // Check for new connections from the acceptor (non-blocking)
        while let Ok((raw_fd, _addr)) = fd_receiver.try_recv() {
            match driver.register_fd(raw_fd) {
                Ok(conn_id) => {
                    CONNECTIONS_ACCEPTED.increment();
                    CONNECTIONS_ACTIVE.increment();
                    stats.inc_channel_receive();

                    let idx = conn_id.as_usize();
                    if idx >= connections.len() {
                        connections.resize_with(idx + 1, || None);
                        pending_recvs.resize_with(idx + 1, || None);
                    }
                    connections[idx] = Some(Connection::new());

                    // Try to start recv with a pooled buffer
                    if let Some(buf_id) = pool.checkout() {
                        let buf = pool.get_mut(buf_id);
                        if driver.submit_recv(conn_id, buf).is_ok() {
                            pending_recvs[idx] = Some(PendingRecv { buf_id });
                        } else {
                            pool.checkin(buf_id);
                        }
                    }
                }
                Err(_) => {
                    // Failed to register, fd is already closed by driver
                }
            }
        }

        let completions_count = driver.poll(Some(Duration::from_millis(1)))?;
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

                    let idx = conn_id.as_usize();
                    if idx >= connections.len() {
                        connections.resize_with(idx + 1, || None);
                        pending_recvs.resize_with(idx + 1, || None);
                    }
                    connections[idx] = Some(Connection::new());

                    // Try to start recv with a pooled buffer
                    if let Some(buf_id) = pool.checkout() {
                        let buf = pool.get_mut(buf_id);
                        if driver.submit_recv(conn_id, buf).is_ok() {
                            pending_recvs[idx] = Some(PendingRecv { buf_id });
                        } else {
                            pool.checkin(buf_id);
                        }
                    }
                }

                // Mio path: socket is readable, recv into pooled buffer
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

                    let mut close_reason: Option<CloseReason> = None;

                    'recv_loop: loop {
                        // Checkout a buffer from pool
                        let Some(buf_id) = pool.checkout() else {
                            // Pool exhausted, stop reading
                            break;
                        };

                        let buf = pool.get_mut(buf_id);

                        match driver.recv(conn_id, buf) {
                            Ok(0) => {
                                pool.checkin(buf_id);
                                close_reason = Some(CloseReason::ClientEof);
                                break;
                            }
                            Ok(n) => {
                                stats.add_bytes_received(n as u64);

                                // Push buffer to connection's chain
                                let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut())
                                else {
                                    pool.checkin(buf_id);
                                    break;
                                };

                                conn.push_recv_buffer(buf_id, n);
                                conn.process(&*cache, &pool, &mut assembly);

                                // Return consumed buffers to pool
                                for id in conn.drain_consumed_buffers() {
                                    pool.checkin(id);
                                }

                                if conn.should_close() {
                                    close_reason = Some(CloseReason::ProtocolClose);
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
                                            close_reason = Some(CloseReason::SendError);
                                            break 'recv_loop;
                                        }
                                    }
                                }

                                // Check backpressure after sending
                                if !conn.should_read() {
                                    stats.inc_backpressure();
                                    break;
                                }
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                pool.checkin(buf_id);
                                break;
                            }
                            Err(_) => {
                                pool.checkin(buf_id);
                                close_reason = Some(CloseReason::RecvError);
                                break;
                            }
                        }
                    }

                    if let Some(reason) = close_reason {
                        close_connection(
                            &mut driver,
                            &mut connections,
                            &mut pending_recvs,
                            &mut pool,
                            conn_id,
                            stats,
                            reason,
                        );
                    }
                }

                CompletionKind::SendReady { conn_id } => {
                    stats.inc_send_ready();
                    let idx = conn_id.as_usize();
                    let mut close_reason: Option<CloseReason> = None;

                    // Drain pending write data
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
                                close_reason = Some(CloseReason::SendError);
                                break;
                            }
                        }
                    }

                    if let Some(reason) = close_reason {
                        close_connection(
                            &mut driver,
                            &mut connections,
                            &mut pending_recvs,
                            &mut pool,
                            conn_id,
                            stats,
                            reason,
                        );
                        continue;
                    }

                    // Process any pending read data now that we have room
                    let should_process = connections
                        .get(idx)
                        .and_then(|c| c.as_ref())
                        .map(|c| c.should_read() && !c.recv_is_empty())
                        .unwrap_or(false);

                    if should_process
                        && let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut())
                    {
                        conn.process(&*cache, &pool, &mut assembly);

                        // Return consumed buffers
                        for id in conn.drain_consumed_buffers() {
                            pool.checkin(id);
                        }

                        if conn.should_close() {
                            close_reason = Some(CloseReason::ProtocolClose);
                        }
                    }

                    if let Some(reason) = close_reason {
                        close_connection(
                            &mut driver,
                            &mut connections,
                            &mut pending_recvs,
                            &mut pool,
                            conn_id,
                            stats,
                            reason,
                        );
                        continue;
                    }

                    // Send any newly generated responses
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
                                close_reason = Some(CloseReason::SendError);
                                break;
                            }
                        }
                    }

                    if let Some(reason) = close_reason {
                        close_connection(
                            &mut driver,
                            &mut connections,
                            &mut pending_recvs,
                            &mut pool,
                            conn_id,
                            stats,
                            reason,
                        );
                    }
                }

                CompletionKind::Closed { conn_id } => {
                    close_connection(
                        &mut driver,
                        &mut connections,
                        &mut pending_recvs,
                        &mut pool,
                        conn_id,
                        stats,
                        CloseReason::ClosedEvent,
                    );
                }

                CompletionKind::Error { conn_id, error: _ } => {
                    close_connection(
                        &mut driver,
                        &mut connections,
                        &mut pending_recvs,
                        &mut pool,
                        conn_id,
                        stats,
                        CloseReason::ErrorEvent,
                    );
                }

                CompletionKind::ListenerError { error, .. } => {
                    eprintln!("Listener error: {}", error);
                }

                // AcceptRaw is only used by the acceptor thread, not workers
                CompletionKind::AcceptRaw { raw_fd, .. } => {
                    // This shouldn't happen in workers, close the fd
                    unsafe { libc::close(raw_fd) };
                }

                // io_uring zero-copy recv completion
                // The buffer we submitted is now filled with data
                CompletionKind::RecvComplete { conn_id, bytes } => {
                    stats.inc_recv();
                    let idx = conn_id.as_usize();

                    // Get the pending recv buffer
                    let pending = pending_recvs.get_mut(idx).and_then(|p| p.take());

                    if bytes == 0 {
                        // EOF - return buffer to pool and close
                        if let Some(p) = pending {
                            pool.checkin(p.buf_id);
                        }
                        close_connection(
                            &mut driver,
                            &mut connections,
                            &mut pending_recvs,
                            &mut pool,
                            conn_id,
                            stats,
                            CloseReason::ClientEof,
                        );
                        continue;
                    }

                    let Some(p) = pending else {
                        // No pending buffer - shouldn't happen
                        continue;
                    };

                    let mut close_reason: Option<CloseReason> = None;

                    // Push the received buffer to connection's chain
                    if let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut()) {
                        stats.add_bytes_received(bytes as u64);
                        conn.push_recv_buffer(p.buf_id, bytes);
                        conn.process(&*cache, &pool, &mut assembly);

                        // Return consumed buffers to pool
                        for id in conn.drain_consumed_buffers() {
                            pool.checkin(id);
                        }

                        if conn.should_close() {
                            close_reason = Some(CloseReason::ProtocolClose);
                        } else {
                            // Drain write buffer
                            while conn.has_pending_write() {
                                let data = conn.pending_write_data();
                                match driver.send(conn_id, data) {
                                    Ok(n) => {
                                        stats.add_bytes_sent(n as u64);
                                        conn.advance_write(n);
                                    }
                                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                                    Err(_) => {
                                        close_reason = Some(CloseReason::SendError);
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        // Connection gone, return buffer
                        pool.checkin(p.buf_id);
                    }

                    if let Some(reason) = close_reason {
                        close_connection(
                            &mut driver,
                            &mut connections,
                            &mut pending_recvs,
                            &mut pool,
                            conn_id,
                            stats,
                            reason,
                        );
                        continue;
                    }

                    // Re-submit recv for next data
                    if let Some(conn) = connections.get(idx).and_then(|c| c.as_ref())
                        && conn.should_read()
                        && let Some(buf_id) = pool.checkout()
                    {
                        let buf = pool.get_mut(buf_id);
                        if driver.submit_recv(conn_id, buf).is_ok() {
                            pending_recvs[idx] = Some(PendingRecv { buf_id });
                        } else {
                            pool.checkin(buf_id);
                        }
                    }
                }

                // UDP events - not used by this TCP server
                CompletionKind::UdpReadable { .. }
                | CompletionKind::RecvMsgComplete { .. }
                | CompletionKind::UdpWritable { .. }
                | CompletionKind::SendMsgComplete { .. }
                | CompletionKind::UdpError { .. } => {}
            }
        }
    }
}

#[inline]
fn close_connection(
    driver: &mut Box<dyn IoDriver>,
    connections: &mut [Option<Connection>],
    pending_recvs: &mut [Option<PendingRecv>],
    pool: &mut BufferPool,
    conn_id: ConnId,
    stats: &WorkerStats,
    reason: CloseReason,
) {
    let idx = conn_id.as_usize();

    // Return pending recv buffer if any
    if let Some(pending) = pending_recvs.get_mut(idx).and_then(|p| p.take()) {
        pool.checkin(pending.buf_id);
    }

    // Return connection's buffers to pool
    if let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut()) {
        for id in conn.clear_recv_buffers() {
            pool.checkin(id);
        }
    }

    if let Some(slot) = connections.get_mut(idx)
        && slot.take().is_some()
    {
        let _ = driver.close(conn_id);
        CONNECTIONS_ACTIVE.decrement();
        stats.inc_close(reason);
    }
}
