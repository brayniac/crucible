//! Worker thread implementation.

use crate::backend::{BackendConnection, BackendPool, InFlightRequest};
use crate::cache::SharedCache;
use crate::client::{ClientConnection, ClientState};
use crate::config::Config;
use crate::metrics::{
    BACKEND_REQUESTS, BACKEND_RESPONSES, CACHE_HITS, CACHE_MISSES, CLIENT_COMMANDS,
    CLIENT_CONNECTIONS, PARSE_ERRORS,
};
use ahash::AHashMap;
use io_driver::{CompletionKind, ConnId, Driver, IoDriver};
use protocol_resp::{Command, Value};
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};

/// Request ID counter.
static NEXT_REQUEST_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Run the proxy with the given configuration.
pub fn run(config: &Config, shutdown: Arc<AtomicBool>) -> Result<(), Box<dyn std::error::Error>> {
    let num_workers = config.threads();
    let cpu_affinity = config.cpu_affinity();
    let listen_addr = config.proxy.listen;

    info!(
        workers = num_workers,
        listen = %listen_addr,
        backend = ?config.backend.nodes,
        "Starting proxy"
    );

    // Create shared cache (thread-safe, lock-free)
    let cache = Arc::new(SharedCache::new(&config.cache));
    if cache.is_enabled() {
        info!(
            heap_size = config.cache.heap_size,
            segment_size = config.cache.segment_size,
            ttl_ms = config.cache.ttl_ms,
            eviction = ?config.cache.eviction,
            "Shared cache enabled"
        );
    }

    // Create channels for fd distribution
    let (senders, receivers): (Vec<_>, Vec<_>) = (0..num_workers)
        .map(|_| crossbeam_channel::bounded::<(RawFd, SocketAddr)>(1024))
        .unzip();

    // Spawn workers
    let mut handles = Vec::with_capacity(num_workers + 1);
    for (worker_id, receiver) in receivers.into_iter().enumerate() {
        let worker_config = config.clone();
        let worker_shutdown = shutdown.clone();
        let worker_cache = cache.clone();
        let cpu_id = cpu_affinity
            .as_ref()
            .map(|cpus| cpus[worker_id % cpus.len()]);

        let handle = std::thread::Builder::new()
            .name(format!("worker-{}", worker_id))
            .spawn(move || {
                if let Some(cpu) = cpu_id {
                    set_cpu_affinity(cpu);
                }

                if let Err(e) = run_worker(
                    worker_id,
                    worker_config,
                    receiver,
                    worker_shutdown,
                    worker_cache,
                ) {
                    error!(worker_id, error = %e, "Worker error");
                }
            })
            .expect("failed to spawn worker thread");

        handles.push(handle);
    }

    // Spawn acceptor
    {
        let acceptor_shutdown = shutdown.clone();
        let handle = std::thread::Builder::new()
            .name("acceptor".to_string())
            .spawn(move || {
                if let Err(e) = run_acceptor(listen_addr, senders, acceptor_shutdown) {
                    error!(error = %e, "Acceptor error");
                }
            })
            .expect("failed to spawn acceptor thread");

        handles.push(handle);
    }

    // Wait for shutdown
    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }

    info!("Shutdown signal received, waiting for workers...");

    // Wait for all threads
    let drain_timeout = Duration::from_secs(config.shutdown.drain_timeout_secs);
    let drain_start = std::time::Instant::now();

    for handle in handles {
        let remaining = drain_timeout.saturating_sub(drain_start.elapsed());
        if remaining.is_zero() {
            warn!("Drain timeout reached");
            break;
        }
        let _ = handle.join();
    }

    info!("Proxy shutdown complete");
    Ok(())
}

/// Run the acceptor thread.
fn run_acceptor(
    listen_addr: SocketAddr,
    senders: Vec<crossbeam_channel::Sender<(RawFd, SocketAddr)>>,
    shutdown: Arc<AtomicBool>,
) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;

    let listener = std::net::TcpListener::bind(listen_addr)?;
    listener.set_nonblocking(true)?;

    info!(address = %listen_addr, "Acceptor listening");

    let mut next_worker = 0;

    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, addr)) => {
                let fd = stream.as_raw_fd();
                // Prevent the stream from being closed when it goes out of scope
                std::mem::forget(stream);

                // Round-robin to workers
                if senders[next_worker].try_send((fd, addr)).is_err() {
                    // Worker queue full, close the connection
                    unsafe { libc::close(fd) };
                    warn!(address = %addr, "Dropped connection - worker queue full");
                } else {
                    debug!(address = %addr, worker = next_worker, "Accepted connection");
                }

                next_worker = (next_worker + 1) % senders.len();
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(e) => {
                error!(error = %e, "Accept error");
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    Ok(())
}

/// Run a worker thread.
fn run_worker(
    worker_id: usize,
    config: Config,
    receiver: crossbeam_channel::Receiver<(RawFd, SocketAddr)>,
    shutdown: Arc<AtomicBool>,
    cache: Arc<SharedCache>,
) -> io::Result<()> {
    // Initialize I/O driver
    let io_engine = config.io_engine();
    info!(worker_id, ?io_engine, "Starting worker with I/O engine");

    let mut driver = Driver::builder()
        .engine(io_engine)
        .buffer_size(config.uring.buffer_size)
        .buffer_count(config.uring.buffer_count)
        .sq_depth(config.uring.sq_depth)
        .build()?;

    // Initialize backend pool
    let backend_addr = config
        .backend
        .nodes
        .first()
        .copied()
        .expect("at least one backend node required");
    let mut backend_pool = BackendPool::new(backend_addr, config.backend.pool_size);

    // Establish backend connections BEFORE accepting any clients
    // This ensures we have backends ready when clients connect
    info!(worker_id, address = %backend_addr, pool_size = config.backend.pool_size, "Connecting to backend");
    let initiated = initiate_backend_connections(&mut driver, &mut backend_pool);
    if initiated == 0 {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "Failed to initiate any backend connections",
        ));
    }
    wait_for_backend_connections(&mut driver, &mut backend_pool)?;
    info!(
        worker_id,
        connected = backend_pool.connection_count(),
        "Backend connections established"
    );

    // Client connections indexed by ConnId slot
    let mut clients: Vec<Option<ClientConnection>> = Vec::with_capacity(4096);

    // Request tracking (request_id -> client_id)
    let mut pending_requests: AHashMap<u64, ConnId> = AHashMap::new();

    info!(worker_id, "Worker started");

    // Main event loop
    while !shutdown.load(Ordering::Relaxed) {
        // Accept new connections from the acceptor
        while let Ok((fd, addr)) = receiver.try_recv() {
            // Set TCP_NODELAY to disable Nagle's algorithm - critical for low latency
            // Without this, small responses get batched causing ~2ms delays
            unsafe {
                let enabled: libc::c_int = 1;
                libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_NODELAY,
                    &enabled as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }

            match driver.register_fd(fd) {
                Ok(conn_id) => {
                    let idx = conn_id.slot();
                    if idx >= clients.len() {
                        clients.resize_with(idx + 1, || None);
                    }
                    clients[idx] = Some(ClientConnection::new(conn_id, addr));
                    CLIENT_CONNECTIONS.increment();
                    trace!(worker_id, conn_id = ?conn_id, address = %addr, "Registered client");
                }
                Err(e) => {
                    error!(error = %e, "Failed to register fd");
                    unsafe { libc::close(fd) };
                }
            }
        }

        // Initiate any needed backend connections (non-blocking)
        // This doesn't fail the worker - it will retry on next iteration
        let _ = initiate_backend_connections(&mut driver, &mut backend_pool);

        // Poll for completions - use very short timeout when actively processing
        let timeout = Duration::from_micros(100);
        let _completion_count = driver.poll(Some(timeout))?;

        // Process completions
        for completion in driver.drain_completions() {
            match completion.kind {
                CompletionKind::Recv { conn_id } => {
                    let idx = conn_id.slot();

                    // Determine if this is a client or backend connection
                    // IMPORTANT: Must verify conn_id matches, not just the slot index
                    let is_client = clients
                        .get(idx)
                        .is_some_and(|c| c.as_ref().is_some_and(|c| c.conn_id == conn_id));

                    if is_client {
                        handle_client_recv(
                            &mut driver,
                            &mut clients,
                            &mut backend_pool,
                            &mut pending_requests,
                            conn_id,
                            &cache,
                        );
                    } else if backend_pool.get_connection(conn_id).is_some() {
                        handle_backend_recv(
                            &mut driver,
                            &mut clients,
                            &mut backend_pool,
                            &mut pending_requests,
                            conn_id,
                            &cache,
                        );
                    }
                }

                CompletionKind::SendReady { conn_id } => {
                    let idx = conn_id.slot();

                    // Drain pending sends - verify conn_id matches
                    if let Some(Some(client)) = clients.get_mut(idx) {
                        if client.conn_id == conn_id {
                            drain_client_sends(&mut driver, client, conn_id);
                        }
                    } else if let Some(backend) = backend_pool.get_connection_mut(conn_id) {
                        // If backend was connecting, mark it as connected now
                        if matches!(backend.state, crate::backend::BackendState::Connecting) {
                            backend.mark_connected();
                            debug!(conn_id = ?conn_id, address = %backend.addr, "Backend connected (runtime)");
                        }
                        drain_backend_sends(&mut driver, backend, conn_id);
                    }
                }

                CompletionKind::Closed { conn_id } => {
                    let idx = conn_id.slot();
                    // Verify conn_id matches before closing
                    let is_client = clients
                        .get(idx)
                        .is_some_and(|c| c.as_ref().is_some_and(|c| c.conn_id == conn_id));

                    if is_client {
                        if let Some(slot) = clients.get_mut(idx) {
                            slot.take();
                            let _ = driver.close(conn_id);
                            CLIENT_CONNECTIONS.decrement();
                        }
                    } else if let Some(backend) = backend_pool.remove_connection(conn_id) {
                        let _ = driver.close(conn_id);
                        let in_flight_count = backend.in_flight.len();
                        warn!(
                            conn_id = ?conn_id,
                            in_flight = in_flight_count,
                            "Backend connection closed"
                        );

                        // Send error responses to clients with in-flight requests
                        for request in backend.in_flight {
                            pending_requests.remove(&request.request_id);
                            if let Some(Some(client)) = clients.get_mut(request.client_id.slot()) {
                                if client.conn_id == request.client_id {
                                    // Send RESP error
                                    let error_response = b"-ERR backend disconnected\r\n";
                                    let _ = driver.send(request.client_id, error_response);
                                }
                            }
                        }
                    }
                }

                CompletionKind::Error { conn_id, error } => {
                    let idx = conn_id.slot();
                    // Verify conn_id matches before handling error
                    let is_client = clients
                        .get(idx)
                        .is_some_and(|c| c.as_ref().is_some_and(|c| c.conn_id == conn_id));

                    if is_client {
                        if let Some(slot) = clients.get_mut(idx) {
                            slot.take();
                            debug!(conn_id = ?conn_id, error = %error, "Client error");
                            let _ = driver.close(conn_id);
                            CLIENT_CONNECTIONS.decrement();
                        }
                    } else if let Some(backend) = backend_pool.remove_connection(conn_id) {
                        let in_flight_count = backend.in_flight.len();
                        error!(
                            conn_id = ?conn_id,
                            error = %error,
                            in_flight = in_flight_count,
                            "Backend error"
                        );
                        let _ = driver.close(conn_id);

                        // Send error responses to clients with in-flight requests
                        for request in backend.in_flight {
                            pending_requests.remove(&request.request_id);
                            if let Some(Some(client)) = clients.get_mut(request.client_id.slot()) {
                                if client.conn_id == request.client_id {
                                    let error_response = b"-ERR backend error\r\n";
                                    let _ = driver.send(request.client_id, error_response);
                                }
                            }
                        }
                    }
                }

                // We don't use Accept directly - acceptor distributes via channel
                CompletionKind::Accept { .. } => {}
                CompletionKind::AcceptRaw { raw_fd, .. } => {
                    // Shouldn't happen in workers, close it
                    unsafe { libc::close(raw_fd) };
                }
                CompletionKind::ListenerError { error, .. } => {
                    error!(error = %error, "Listener error");
                }

                // RecvComplete is for single-shot mode, we use multishot
                CompletionKind::RecvComplete { .. } => {}

                // UDP events not used
                CompletionKind::UdpReadable { .. }
                | CompletionKind::RecvMsgComplete { .. }
                | CompletionKind::UdpWritable { .. }
                | CompletionKind::SendMsgComplete { .. }
                | CompletionKind::UdpError { .. } => {}
            }
        }
    }

    info!(worker_id, "Worker shutting down");
    Ok(())
}

/// Initiate backend connections (non-blocking).
/// Connections start in Connecting state and become Connected on SendReady.
/// Returns the number of connections initiated.
fn initiate_backend_connections(driver: &mut Box<dyn IoDriver>, pool: &mut BackendPool) -> usize {
    use socket2::{Domain, Protocol, Socket, Type};

    let mut initiated = 0;

    while pool.needs_connections() {
        let addr = pool.backend_addr();

        // Create non-blocking socket
        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };

        let socket = match Socket::new(domain, Type::STREAM, Some(Protocol::TCP)) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, address = %addr, "Failed to create socket");
                break; // Try again next iteration
            }
        };

        if let Err(e) = socket.set_nonblocking(true) {
            warn!(error = %e, "Failed to set nonblocking");
            break;
        }
        if let Err(e) = socket.set_nodelay(true) {
            warn!(error = %e, "Failed to set nodelay");
            break;
        }

        // Non-blocking connect - returns EINPROGRESS
        match socket.connect(&addr.into()) {
            Ok(()) => {}
            Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {}
            Err(e) => {
                warn!(error = %e, address = %addr, "Backend connect failed, will retry");
                break; // Don't crash, just try again next iteration
            }
        }

        let stream: TcpStream = socket.into();

        // Register with the driver - connection still in progress
        match driver.register(stream) {
            Ok(conn_id) => {
                // Connection is in Connecting state until SendReady
                let conn = BackendConnection::new(conn_id, addr);
                pool.add_connection(conn);
                trace!(address = %addr, conn_id = ?conn_id, "Backend connection initiated");
                initiated += 1;
            }
            Err(e) => {
                warn!(error = %e, address = %addr, "Failed to register backend connection");
                break;
            }
        }
    }

    initiated
}

/// Wait for all backend connections to be established.
/// Polls the driver until all connections receive SendReady.
fn wait_for_backend_connections(
    driver: &mut Box<dyn IoDriver>,
    pool: &mut BackendPool,
) -> io::Result<()> {
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(5);

    while pool
        .connections()
        .any(|c| matches!(c.state, crate::backend::BackendState::Connecting))
    {
        if start.elapsed() > timeout {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Backend connection timeout",
            ));
        }

        driver.poll(Some(Duration::from_millis(100)))?;

        for completion in driver.drain_completions() {
            match completion.kind {
                CompletionKind::SendReady { conn_id } => {
                    // Connection established
                    if let Some(conn) = pool.get_connection_mut(conn_id) {
                        conn.mark_connected();
                        debug!(conn_id = ?conn_id, address = %conn.addr, "Backend connected");
                    }
                }
                CompletionKind::Error { conn_id, error } => {
                    if pool.get_connection(conn_id).is_some() {
                        error!(conn_id = ?conn_id, error = %error, "Backend connection failed");
                        return Err(io::Error::new(io::ErrorKind::ConnectionRefused, error));
                    }
                }
                CompletionKind::Closed { conn_id } => {
                    if pool.get_connection(conn_id).is_some() {
                        error!(conn_id = ?conn_id, "Backend connection closed during connect");
                        return Err(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "Backend closed during connect",
                        ));
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}

/// Handle data received from a client.
fn handle_client_recv(
    driver: &mut Box<dyn IoDriver>,
    clients: &mut [Option<ClientConnection>],
    backend_pool: &mut BackendPool,
    pending_requests: &mut AHashMap<u64, ConnId>,
    conn_id: ConnId,
    cache: &Arc<SharedCache>,
) {
    let idx = conn_id.slot();
    let mut should_close = false;
    let mut close_reason = None;

    // Access received data via with_recv_buf
    // Note: Empty buffer does NOT mean EOF - it just means no new data.
    // EOF is signaled by with_recv_buf returning UnexpectedEof error.
    let result = driver.with_recv_buf(conn_id, &mut |buf| {
        // If no new data, just return - not an error
        if buf.is_empty() {
            return;
        }

        // We already verified conn_id in the caller, just get the client
        let Some(Some(client)) = clients.get_mut(idx) else {
            return;
        };

        // Copy data to client's recv buffer
        let data = buf.as_slice();
        client.append_recv(data);
        let consumed = data.len();
        buf.consume(consumed);

        // Parse and handle commands
        loop {
            let unparsed = client.unparsed();
            if unparsed.is_empty() {
                break;
            }

            match Command::parse(unparsed) {
                Ok((cmd, consumed)) => {
                    CLIENT_COMMANDS.increment();
                    trace!(conn_id = ?conn_id, command = ?cmd, "Parsed command");

                    // Forward to backend or handle locally
                    let result =
                        forward_to_backend(backend_pool, pending_requests, conn_id, &cmd, cache);

                    // Handle result after cmd borrow is released
                    match result {
                        ForwardResult::Pong => {
                            client.queue_response(b"+PONG\r\n");
                        }
                        ForwardResult::CacheHit(data) => {
                            // Send cached data (single lookup, no second cache access)
                            client.queue_response(&data);
                        }
                        ForwardResult::Forwarded => {}
                        ForwardResult::Error(err) => {
                            client.queue_response(err);
                        }
                    }

                    client.consume_parsed(consumed);
                }
                Err(protocol_resp::ParseError::Incomplete) => {
                    // Need more data
                    break;
                }
                Err(e) => {
                    PARSE_ERRORS.increment();
                    warn!(conn_id = ?conn_id, error = ?e, "Parse error");
                    client.queue_response(b"-ERR protocol error\r\n");
                    should_close = true;
                    break;
                }
            }
        }
    });

    // Handle EOF - client closed the connection
    if let Err(e) = result
        && e.kind() == io::ErrorKind::UnexpectedEof
    {
        should_close = true;
        close_reason = Some("eof");
    }

    if should_close {
        close_client(driver, clients, conn_id, close_reason.unwrap_or("unknown"));
        return;
    }

    // Drain any pending sends
    if let Some(Some(client)) = clients.get_mut(idx) {
        drain_client_sends(driver, client, conn_id);
    }

    // Also drain backend sends in case we queued requests
    for backend in backend_pool.connections_mut() {
        if backend.send_buf.is_empty() {
            continue;
        }
        drain_backend_sends(driver, backend, backend.conn_id);
    }
}

/// Result of forwarding a command.
enum ForwardResult {
    /// Return PONG response.
    Pong,
    /// Cache hit - response data to send (single allocation, but avoids double lookup).
    CacheHit(Vec<u8>),
    /// Command was forwarded to backend (response pending).
    Forwarded,
    /// Error occurred (error response needs to be sent).
    Error(&'static [u8]),
}

/// Forward a command to the backend.
fn forward_to_backend(
    pool: &mut BackendPool,
    pending_requests: &mut AHashMap<u64, ConnId>,
    client_id: ConnId,
    cmd: &Command<'_>,
    cache: &Arc<SharedCache>,
) -> ForwardResult {
    // Handle PING locally
    if matches!(cmd, Command::Ping) {
        return ForwardResult::Pong;
    }

    // Check cache for GET commands
    if let Command::Get { key } = cmd {
        // Single cache lookup - get data directly if present
        if let Some(data) = cache.with_value(key, |v| v.to_vec()) {
            CACHE_HITS.increment();
            return ForwardResult::CacheHit(data);
        }
        CACHE_MISSES.increment();
    }

    // Invalidate cache on writes
    match cmd {
        Command::Set { key, .. } | Command::Del { key } => {
            cache.delete(key);
        }
        _ => {}
    }

    // Encode command for backend
    let mut request_buf = Vec::with_capacity(256);
    encode_command(cmd, &mut request_buf);

    // Get a connection and queue the request
    let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);

    let request = InFlightRequest {
        client_id,
        request_id,
        key: extract_key(cmd),
        cacheable: matches!(cmd, Command::Get { .. }),
    };

    if pool.queue_request(request, &request_buf).is_some() {
        pending_requests.insert(request_id, client_id);
        BACKEND_REQUESTS.increment();
        ForwardResult::Forwarded
    } else {
        ForwardResult::Error(b"-ERR backend unavailable\r\n")
    }
}

/// Encode a RESP command.
fn encode_command(cmd: &Command<'_>, buf: &mut Vec<u8>) {
    match cmd {
        Command::Get { key } => {
            buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$");
            buf.extend_from_slice(key.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
        }
        Command::Set { key, value, .. } => {
            buf.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$");
            buf.extend_from_slice(key.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n$");
            buf.extend_from_slice(value.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(value);
            buf.extend_from_slice(b"\r\n");
        }
        _ => {
            // For other commands, we'd need more encoding logic
            // For now, just send a placeholder
            buf.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        }
    }
}

/// Extract the key from a command (for caching).
fn extract_key(cmd: &Command<'_>) -> Option<bytes::Bytes> {
    match cmd {
        Command::Get { key } => Some(bytes::Bytes::copy_from_slice(key)),
        _ => None,
    }
}

/// Response to send to a client after backend recv.
struct ResponseToSend {
    client_id: ConnId,
    response: Vec<u8>,
}

/// Handle data received from a backend.
fn handle_backend_recv(
    driver: &mut Box<dyn IoDriver>,
    clients: &mut [Option<ClientConnection>],
    pool: &mut BackendPool,
    pending_requests: &mut AHashMap<u64, ConnId>,
    conn_id: ConnId,
    cache: &Arc<SharedCache>,
) {
    let mut should_close = false;
    let mut close_reason = "";
    let mut responses_to_send: Vec<ResponseToSend> = Vec::new();

    // Access received data
    // Note: Empty buffer does NOT mean EOF - it just means no new data.
    // EOF is signaled by with_recv_buf returning UnexpectedEof error.
    let result = driver.with_recv_buf(conn_id, &mut |buf| {
        // If no new data, just return - not an error
        if buf.is_empty() {
            return;
        }

        let Some(backend) = pool.get_connection_mut(conn_id) else {
            return;
        };

        // Copy data to backend's recv buffer
        let data = buf.as_slice();
        backend.append_recv(data);
        let consumed_from_buf = data.len();
        buf.consume(consumed_from_buf);

        // Parse responses
        loop {
            let recv_data = &backend.recv_buf[..];
            if recv_data.is_empty() {
                break;
            }

            match Value::parse(recv_data) {
                Ok((_value, consumed)) => {
                    BACKEND_RESPONSES.increment();

                    // Peek at the request to get metadata BEFORE modifying backend state
                    // This avoids the borrow conflict
                    let request_info = backend.oldest_request().map(|req| {
                        (
                            req.request_id,
                            req.client_id,
                            req.cacheable,
                            req.key.clone(),
                        )
                    });

                    if let Some((request_id, client_id, cacheable, key)) = request_info {
                        let response_bytes = &recv_data[..consumed];

                        // Cache successful GET responses for future requests
                        if cacheable {
                            if let Some(ref k) = key {
                                // Only cache successful bulk string responses
                                // Don't cache: nil ($-1 or _), errors (-), MOVED/ASK redirects
                                if !response_bytes.starts_with(b"$-1\r\n")
                                    && !response_bytes.starts_with(b"_\r\n")
                                    && !response_bytes.starts_with(b"-")
                                {
                                    cache.set(k, response_bytes);
                                }
                            }
                        }

                        // Always copy response for immediate send (responses are typically small)
                        responses_to_send.push(ResponseToSend {
                            client_id,
                            response: response_bytes.to_vec(),
                        });

                        // Now we can mutate backend state
                        backend.complete_request();
                        pending_requests.remove(&request_id);
                    }

                    backend.consume_response(consumed);
                }
                Err(protocol_resp::ParseError::Incomplete) => {
                    break;
                }
                Err(e) => {
                    // Log some context about the unparseable data
                    let preview_len = recv_data.len().min(64);
                    let preview = String::from_utf8_lossy(&recv_data[..preview_len]);
                    error!(
                        conn_id = ?conn_id,
                        error = ?e,
                        data_preview = %preview,
                        data_len = recv_data.len(),
                        "Backend response parse error"
                    );
                    should_close = true;
                    close_reason = "parse error";
                    break;
                }
            }
        }
    });

    if let Err(e) = result {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            should_close = true;
            close_reason = "unexpected eof";
        } else {
            error!(conn_id = ?conn_id, error = %e, "Backend recv error");
            should_close = true;
            close_reason = "recv error";
        }
    }

    if should_close {
        // Get diagnostic info before removing
        let (in_flight, recv_buf_preview) = pool
            .get_connection(conn_id)
            .map(|c| {
                let preview = if c.recv_buf.is_empty() {
                    String::from("<empty>")
                } else {
                    let len = c.recv_buf.len().min(128);
                    format!(
                        "{:?} ({}B)",
                        String::from_utf8_lossy(&c.recv_buf[..len]),
                        c.recv_buf.len()
                    )
                };
                (c.in_flight.len(), preview)
            })
            .unwrap_or((0, String::from("<no connection>")));
        warn!(
            conn_id = ?conn_id,
            reason = close_reason,
            in_flight_requests = in_flight,
            recv_buf = %recv_buf_preview,
            "Backend connection closed"
        );
        pool.remove_connection(conn_id);
        let _ = driver.close(conn_id);
        return;
    }

    // Send responses to clients
    for ResponseToSend {
        client_id,
        response,
    } in responses_to_send
    {
        let idx = client_id.slot();
        if let Some(Some(client)) = clients.get_mut(idx) {
            if client.conn_id == client_id {
                client.queue_response(&response);
                drain_client_sends(driver, client, client_id);
            }
        }
    }
}

/// Drain pending sends on a client connection.
fn drain_client_sends(
    driver: &mut Box<dyn IoDriver>,
    client: &mut ClientConnection,
    conn_id: ConnId,
) {
    while client.has_pending_send() {
        let data = client.send_data();
        match driver.send(conn_id, data) {
            Ok(n) => {
                client.advance_sent(n);
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Will get SendReady when we can send more
                break;
            }
            Err(_) => {
                // Send error, connection will be closed
                break;
            }
        }
    }

    // Update state
    if client.has_pending_send() {
        client.state = ClientState::Writing { bytes_written: 0 };
    } else {
        client.state = ClientState::Reading;
    }
}

/// Drain pending sends on a backend connection.
fn drain_backend_sends(
    driver: &mut Box<dyn IoDriver>,
    backend: &mut BackendConnection,
    conn_id: ConnId,
) {
    while !backend.send_buf.is_empty() {
        let data = backend.send_data();
        match driver.send(conn_id, data) {
            Ok(n) => {
                backend.advance_sent(n);
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                break;
            }
            Err(_) => {
                break;
            }
        }
    }
}

/// Close a client connection.
fn close_client(
    driver: &mut Box<dyn IoDriver>,
    clients: &mut [Option<ClientConnection>],
    conn_id: ConnId,
    reason: &str,
) {
    let idx = conn_id.slot();
    // Verify conn_id matches before closing
    if let Some(slot) = clients.get_mut(idx) {
        if slot.as_ref().is_some_and(|c| c.conn_id == conn_id) {
            slot.take();
            debug!(conn_id = ?conn_id, reason, "Closing client");
            let _ = driver.close(conn_id);
            CLIENT_CONNECTIONS.decrement();
        }
    }
}

/// Set CPU affinity for the current thread.
#[cfg(target_os = "linux")]
fn set_cpu_affinity(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

#[cfg(not(target_os = "linux"))]
fn set_cpu_affinity(_cpu: usize) {
    // No-op on non-Linux
}
