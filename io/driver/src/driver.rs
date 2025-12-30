//! I/O driver trait definition.

use crate::types::{Completion, ConnId, ListenerId};
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::io::RawFd;

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
    /// New connections will be reported via `Accept` completions.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to bind to
    /// * `backlog` - The maximum number of pending connections
    fn listen(&mut self, addr: SocketAddr, backlog: u32) -> io::Result<ListenerId>;

    /// Close a listener and stop accepting new connections.
    fn close_listener(&mut self, id: ListenerId) -> io::Result<()>;

    // === Connection operations ===

    /// Register an existing TCP connection with the driver.
    ///
    /// Used for client-initiated connections. The stream should be
    /// in non-blocking mode.
    fn register(&mut self, stream: TcpStream) -> io::Result<ConnId>;

    /// Close a connection.
    ///
    /// This deregisters the connection from the driver and closes the socket.
    fn close(&mut self, id: ConnId) -> io::Result<()>;

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
}
