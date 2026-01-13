//! I/O driver trait definition.

use crate::types::{Completion, ConnId, ListenerId, RecvMeta, SendMeta, UdpSocketId};
use std::io::{self, IoSlice};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::io::RawFd;

/// Trait for receive buffer access.
///
/// Implemented by connection receive states to provide a unified
/// interface for accessing received data across all backends.
pub trait RecvBuf {
    /// Get a contiguous slice of all available data.
    fn as_slice(&self) -> &[u8];

    /// Get the number of bytes available.
    fn len(&self) -> usize;

    /// Check if the buffer is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Mark `n` bytes as consumed.
    fn consume(&mut self, n: usize);
}

/// I/O driver trait - abstracts over mio and io_uring backends.
///
/// This trait provides a unified interface for network I/O that works
/// efficiently on both Linux (with io_uring) and other platforms (with mio).
///
/// # Design Philosophy
///
/// - **Unified API**: The same code works regardless of backend
/// - **Automatic optimization**: Uses zero-copy operations when available
/// - **Completion-based**: Poll for events, then process completions
///
/// # Usage Pattern
///
/// ```ignore
/// let mut driver = Driver::new()?;
///
/// // Server: listen for connections
/// let listener_id = driver.listen("0.0.0.0:8080".parse()?, 128)?;
///
/// // Client: register an outbound connection
/// let conn_id = driver.register(tcp_stream)?;
///
/// loop {
///     // Wait for events
///     driver.poll(Some(Duration::from_millis(100)))?;
///
///     // Process completions
///     for completion in driver.drain_completions() {
///         match completion.kind {
///             CompletionKind::Accept { conn_id, addr, .. } => {
///                 println!("New connection from {}", addr);
///             }
///             CompletionKind::Recv { conn_id } => {
///                 let mut buf = [0u8; 4096];
///                 if let Ok(n) = driver.recv(conn_id, &mut buf) {
///                     // Process data
///                 }
///             }
///             _ => {}
///         }
///     }
/// }
/// ```
pub trait IoDriver: Send {
    // === Listener operations ===

    /// Bind and start listening on an address.
    ///
    /// Returns a listener ID that can be used to identify accepted connections.
    /// New connections will be reported via `Accept` completions (auto-registered).
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to bind to
    /// * `backlog` - The maximum number of pending connections
    fn listen(&mut self, addr: SocketAddr, backlog: u32) -> io::Result<ListenerId>;

    /// Bind and start listening on an address in raw mode.
    ///
    /// Unlike `listen()`, connections accepted in raw mode are NOT automatically
    /// registered with the driver. Instead, `AcceptRaw` completions are emitted
    /// with the raw file descriptor. This is designed for single-acceptor patterns
    /// where connections are distributed to workers via channels.
    ///
    /// Workers must call `register_fd()` to register received file descriptors.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to bind to
    /// * `backlog` - The maximum number of pending connections
    fn listen_raw(&mut self, addr: SocketAddr, backlog: u32) -> io::Result<ListenerId>;

    /// Close a listener and stop accepting new connections.
    fn close_listener(&mut self, id: ListenerId) -> io::Result<()>;

    // === Connection operations ===

    /// Register an existing TCP connection with the driver.
    ///
    /// Used for client-initiated connections. The stream should be
    /// in non-blocking mode.
    fn register(&mut self, stream: TcpStream) -> io::Result<ConnId>;

    /// Register a raw file descriptor with the driver.
    ///
    /// Used to register connections received from an acceptor thread via
    /// `AcceptRaw` completions. The fd should be a valid TCP socket.
    /// The driver takes ownership of the fd and will close it on connection close.
    ///
    /// # Safety
    ///
    /// The caller must ensure the fd is a valid, open TCP socket.
    #[cfg(unix)]
    fn register_fd(&mut self, fd: RawFd) -> io::Result<ConnId>;

    /// Close a connection.
    ///
    /// This deregisters the connection from the driver and closes the socket.
    fn close(&mut self, id: ConnId) -> io::Result<()>;

    /// Take ownership of a connection's file descriptor.
    ///
    /// This removes the connection from the driver and returns its raw fd
    /// WITHOUT closing it. Used to transfer connections between drivers
    /// (e.g., from acceptor to worker).
    ///
    /// The caller takes ownership of the fd and is responsible for closing it.
    #[cfg(unix)]
    fn take_fd(&mut self, id: ConnId) -> io::Result<RawFd>;

    /// Send data on a connection.
    ///
    /// Automatically uses zero-copy operations (SendZc) when available.
    /// The driver handles buffer management internally.
    ///
    /// # Returns
    ///
    /// - `Ok(n)` - `n` bytes were queued for sending
    /// - `Err(WouldBlock)` - Cannot send right now, try again after `SendReady`
    /// - `Err(other)` - An error occurred
    fn send(&mut self, id: ConnId, data: &[u8]) -> io::Result<usize>;

    /// Send multiple buffers as a single vectored write.
    ///
    /// This is useful for scatter-gather I/O, particularly for sending
    /// responses that consist of multiple parts (e.g., header + value + trailer)
    /// without intermediate copying.
    ///
    /// # Default Implementation
    ///
    /// The default implementation concatenates all buffers into a single
    /// contiguous buffer and calls `send()`. Backends should override this
    /// for more efficient implementations:
    /// - **io_uring**: Uses `SendMsg` for true vectored zero-copy
    /// - **mio**: Uses `writev` system call
    /// - **tokio**: Uses `write_vectored`
    ///
    /// # Returns
    ///
    /// - `Ok(n)` - Total of `n` bytes were queued for sending
    /// - `Err(WouldBlock)` - Cannot send right now, try again after `SendReady`
    /// - `Err(other)` - An error occurred
    fn send_vectored(&mut self, id: ConnId, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        // Default implementation: concatenate and send
        let total_len: usize = bufs.iter().map(|b| b.len()).sum();
        let mut combined = Vec::with_capacity(total_len);
        for buf in bufs {
            combined.extend_from_slice(buf);
        }
        self.send(id, &combined)
    }

    /// Receive data from a connection.
    ///
    /// Reads available data into the provided buffer.
    ///
    /// # Returns
    ///
    /// - `Ok(n)` - `n` bytes were read into `buf`
    /// - `Err(WouldBlock)` - No data available, wait for `Recv` completion
    /// - `Err(other)` - An error occurred
    fn recv(&mut self, id: ConnId, buf: &mut [u8]) -> io::Result<usize>;

    /// Access received data for a connection via callback.
    ///
    /// This is the preferred way to read data as it enables zero-copy access
    /// across all backends. The callback receives a `RecvBuf` that provides:
    /// - `as_slice()` - Get contiguous view of available data
    /// - `len()` / `is_empty()` - Check data availability
    /// - `consume(n)` - Mark bytes as consumed
    ///
    /// # Zero-Copy Behavior
    ///
    /// When data fits in a single kernel buffer and is fully consumed before
    /// the next recv, access is zero-copy (direct slice into kernel buffer).
    /// When data spans recv boundaries, it's automatically coalesced.
    ///
    /// # Example
    ///
    /// ```ignore
    /// driver.with_recv_buf(conn_id, &mut |buf| {
    ///     while let Some((cmd, len)) = parse(buf.as_slice()) {
    ///         execute(cmd);
    ///         buf.consume(len);
    ///     }
    /// })?;
    /// ```
    ///
    /// # Returns
    ///
    /// - `Ok(())` - Callback executed successfully
    /// - `Err(NotFound)` - Connection not found
    fn with_recv_buf(&mut self, id: ConnId, f: &mut dyn FnMut(&mut dyn RecvBuf)) -> io::Result<()>;

    /// Submit an async recv operation with a caller-provided buffer.
    ///
    /// This is an alternative to the `Recv` + `recv()` pattern that enables
    /// zero-copy receives on io_uring. The kernel writes directly to the
    /// provided buffer, eliminating intermediate copies.
    ///
    /// # Buffer Lifetime
    ///
    /// The buffer must remain valid until the corresponding `RecvComplete`
    /// completion is received. The caller is responsible for ensuring this.
    ///
    /// # Returns
    ///
    /// - `Ok(())` - The recv was submitted successfully
    /// - `Err(WouldBlock)` - A recv is already pending for this connection
    /// - `Err(other)` - An error occurred
    ///
    /// # Completion
    ///
    /// When data is received, a `RecvComplete { conn_id, bytes }` completion
    /// is emitted. If `bytes` is 0, the peer closed the connection.
    ///
    /// # Backend Support
    ///
    /// - **io_uring**: Submits a single-shot recv with the provided buffer
    /// - **mio**: Falls back to returning `Err(Unsupported)` - use `recv()` instead
    fn submit_recv(&mut self, id: ConnId, buf: &mut [u8]) -> io::Result<()>;

    // === Event loop ===

    /// Poll for I/O events with optional timeout.
    ///
    /// This submits pending operations and waits for completions.
    ///
    /// # Returns
    ///
    /// The number of completions ready to be processed.
    fn poll(&mut self, timeout: Option<Duration>) -> io::Result<usize>;

    /// Drain all pending completions.
    ///
    /// Call this after `poll()` to process all ready events.
    /// Returns an iterator over the completions.
    fn drain_completions(&mut self) -> Vec<Completion>;

    // === Introspection ===

    /// Get the number of registered connections.
    fn connection_count(&self) -> usize;

    /// Get the number of active listeners.
    fn listener_count(&self) -> usize;

    /// Get the raw file descriptor for a connection.
    ///
    /// Useful for setting socket options like `SO_TIMESTAMPING`.
    #[cfg(unix)]
    fn raw_fd(&self, id: ConnId) -> Option<RawFd>;

    // === UDP socket operations ===

    /// Bind a UDP socket to an address.
    ///
    /// Returns a socket ID for subsequent operations.
    /// The socket is automatically configured for:
    /// - Non-blocking mode
    /// - `IP_RECVTOS` / `IPV6_RECVTCLASS` (ECN reception)
    /// - `IP_PKTINFO` / `IPV6_RECVPKTINFO` (destination address)
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to bind to
    fn bind_udp(&mut self, addr: SocketAddr) -> io::Result<UdpSocketId>;

    /// Close a UDP socket.
    fn close_udp(&mut self, id: UdpSocketId) -> io::Result<()>;

    /// Submit an async recvmsg operation (io_uring backend).
    ///
    /// When a datagram arrives, a `RecvMsgComplete` completion is emitted.
    /// The datagram data is written to `buf`, metadata in the completion.
    ///
    /// # Buffer Lifetime
    ///
    /// The buffer must remain valid until the completion is received.
    ///
    /// # Backend Support
    ///
    /// - **io_uring**: Submits a recvmsg operation
    /// - **mio**: Returns `Err(Unsupported)` - use `recvmsg()` instead
    fn submit_recvmsg(&mut self, id: UdpSocketId, buf: &mut [u8]) -> io::Result<()>;

    /// Submit an async sendmsg operation (io_uring backend).
    ///
    /// Sends a datagram with the specified metadata.
    /// A `SendMsgComplete` completion is emitted when done.
    ///
    /// # Buffer Lifetime
    ///
    /// The data buffer must remain valid until the completion is received.
    ///
    /// # Backend Support
    ///
    /// - **io_uring**: Submits a sendmsg operation
    /// - **mio**: Returns `Err(Unsupported)` - use `sendmsg()` instead
    fn submit_sendmsg(&mut self, id: UdpSocketId, data: &[u8], meta: &SendMeta) -> io::Result<()>;

    /// Receive a datagram synchronously.
    ///
    /// Reads a single datagram into `buf` and returns metadata.
    /// Call this after receiving a `UdpReadable` completion (mio) or
    /// use `submit_recvmsg()` for async operation (io_uring).
    ///
    /// # Returns
    ///
    /// - `Ok(meta)` - A datagram was received, `meta.len` bytes written to `buf`
    /// - `Err(WouldBlock)` - No datagram available
    /// - `Err(other)` - An error occurred
    fn recvmsg(&mut self, id: UdpSocketId, buf: &mut [u8]) -> io::Result<RecvMeta>;

    /// Send a datagram synchronously.
    ///
    /// Sends data to the address specified in `meta`.
    /// Call this after receiving a `UdpWritable` completion (mio) or
    /// use `submit_sendmsg()` for async operation (io_uring).
    ///
    /// # Returns
    ///
    /// - `Ok(n)` - `n` bytes were sent
    /// - `Err(WouldBlock)` - Cannot send right now
    /// - `Err(other)` - An error occurred
    fn sendmsg(&mut self, id: UdpSocketId, data: &[u8], meta: &SendMeta) -> io::Result<usize>;

    /// Get the number of UDP sockets.
    fn udp_socket_count(&self) -> usize;

    /// Get the raw file descriptor for a UDP socket.
    #[cfg(unix)]
    fn udp_raw_fd(&self, id: UdpSocketId) -> Option<RawFd>;
}
