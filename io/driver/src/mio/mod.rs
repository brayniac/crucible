//! Mio-based I/O driver using epoll/kqueue.
//!
//! This module provides a cross-platform I/O driver that works on
//! Linux, macOS, and other Unix systems.

use crate::driver::IoDriver;
use crate::types::{Completion, CompletionKind, ConnId, ListenerId};
use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
use mio::{Events, Interest, Poll, Token};
use slab::Slab;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd};
use std::time::Duration;

/// Token offset for listeners to avoid collision with connections.
const LISTENER_TOKEN_OFFSET: usize = 1 << 30;

/// Connection state for mio driver.
struct MioConnection {
    stream: MioTcpStream,
    readable: bool,
    writable: bool,
}

/// Listener state for mio driver.
struct MioListener {
    listener: MioTcpListener,
}

/// Mio-based I/O driver.
///
/// This driver uses mio (epoll on Linux, kqueue on macOS) for I/O multiplexing.
/// It provides a unified completion-based API that matches the io_uring driver.
pub struct MioDriver {
    poll: Poll,
    events: Events,
    connections: Slab<MioConnection>,
    listeners: Slab<MioListener>,
    pending_completions: Vec<Completion>,
    #[allow(dead_code)]
    buffer_size: usize,
}

impl MioDriver {
    /// Create a new mio driver with default settings.
    pub fn new() -> io::Result<Self> {
        Self::with_config(16384, 8192)
    }

    /// Create a new mio driver with custom configuration.
    pub fn with_config(buffer_size: usize, max_connections: usize) -> io::Result<Self> {
        Ok(Self {
            poll: Poll::new()?,
            events: Events::with_capacity(1024),
            connections: Slab::with_capacity(max_connections.min(4096)),
            listeners: Slab::with_capacity(16),
            pending_completions: Vec::with_capacity(256),
            buffer_size,
        })
    }
}

impl IoDriver for MioDriver {
    fn listen(&mut self, addr: SocketAddr, backlog: u32) -> io::Result<ListenerId> {
        // Create socket with socket2 for more control
        let socket = socket2::Socket::new(
            match addr {
                SocketAddr::V4(_) => socket2::Domain::IPV4,
                SocketAddr::V6(_) => socket2::Domain::IPV6,
            },
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;

        socket.set_reuse_address(true)?;

        #[cfg(unix)]
        {
            // Enable SO_REUSEPORT for load balancing
            let fd = socket.as_raw_fd();
            let optval: libc::c_int = 1;
            unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_REUSEPORT,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;
        socket.listen(backlog as i32)?;

        // Convert to mio TcpListener
        let std_listener: std::net::TcpListener = socket.into();
        let mut mio_listener = MioTcpListener::from_std(std_listener);

        let entry = self.listeners.vacant_entry();
        let id = entry.key();

        // Register with poll
        self.poll.registry().register(
            &mut mio_listener,
            Token(id + LISTENER_TOKEN_OFFSET),
            Interest::READABLE,
        )?;

        entry.insert(MioListener {
            listener: mio_listener,
        });

        Ok(ListenerId::new(id))
    }

    fn close_listener(&mut self, id: ListenerId) -> io::Result<()> {
        if let Some(mut listener) = self.listeners.try_remove(id.as_usize()) {
            self.poll.registry().deregister(&mut listener.listener)?;
        }
        Ok(())
    }

    fn register(&mut self, stream: TcpStream) -> io::Result<ConnId> {
        stream.set_nonblocking(true)?;
        let raw_fd = stream.into_raw_fd();
        let mut mio_stream = unsafe { MioTcpStream::from_raw_fd(raw_fd) };

        let entry = self.connections.vacant_entry();
        let id = entry.key();

        // Register with poll
        self.poll.registry().register(
            &mut mio_stream,
            Token(id),
            Interest::READABLE | Interest::WRITABLE,
        )?;

        entry.insert(MioConnection {
            stream: mio_stream,
            readable: false,
            writable: false, // Wait for connect to complete (SendReady event)
        });

        Ok(ConnId::new(id))
    }

    fn close(&mut self, id: ConnId) -> io::Result<()> {
        if let Some(mut conn) = self.connections.try_remove(id.as_usize()) {
            self.poll.registry().deregister(&mut conn.stream)?;
        }
        Ok(())
    }

    fn send(&mut self, id: ConnId, data: &[u8]) -> io::Result<usize> {
        let conn_len = self.connections.len();
        let conn = self.connections.get_mut(id.as_usize()).ok_or_else(|| {
            eprintln!(
                "BUG(driver.send): conn_id={} not found in slab (len={})",
                id.as_usize(),
                conn_len
            );
            io::Error::new(io::ErrorKind::NotFound, "connection not found")
        })?;

        // Don't try to send if we know the socket isn't writable yet
        // (e.g., non-blocking connect in progress)
        if !conn.writable {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }

        // Log ALL sends for debugging
        if !data.is_empty() && data.len() < 100 {
            eprintln!(
                ">>> SEND conn={} len={} data={:?}",
                id.as_usize(),
                data.len(),
                String::from_utf8_lossy(&data[..data.len().min(64)])
            );
        }

        match conn.stream.write(data) {
            Ok(n) => Ok(n),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                conn.writable = false;
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    fn recv(&mut self, id: ConnId, buf: &mut [u8]) -> io::Result<usize> {
        let conn_len = self.connections.len();
        let conn = self.connections.get_mut(id.as_usize()).ok_or_else(|| {
            eprintln!(
                "BUG(driver.recv): conn_id={} not found in slab (len={})",
                id.as_usize(),
                conn_len
            );
            io::Error::new(io::ErrorKind::NotFound, "connection not found")
        })?;

        match conn.stream.read(buf) {
            Ok(0) => Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "connection closed",
            )),
            Ok(n) => {
                if n < 200 {
                    eprintln!(
                        "<<< RECV conn={} len={} data={:?}",
                        id.as_usize(),
                        n,
                        String::from_utf8_lossy(&buf[..n.min(64)])
                    );
                }
                Ok(n)
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                conn.readable = false;
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    fn poll(&mut self, timeout: Option<Duration>) -> io::Result<usize> {
        self.pending_completions.clear();

        self.poll.poll(&mut self.events, timeout)?;

        // Collect event info first to avoid borrow issues
        let events: Vec<_> = self
            .events
            .iter()
            .map(|e| {
                (
                    e.token().0,
                    e.is_readable(),
                    e.is_writable(),
                    e.is_read_closed() || e.is_write_closed(),
                    e.is_error(),
                )
            })
            .collect();

        for (token, readable, writable, closed, error) in events {
            // Check if this is a listener event
            if token >= LISTENER_TOKEN_OFFSET {
                let listener_id = token - LISTENER_TOKEN_OFFSET;
                if self.listeners.contains(listener_id) {
                    // Accept all pending connections
                    self.accept_pending(listener_id);
                }
                continue;
            }

            // Connection event
            if let Some(conn) = self.connections.get_mut(token) {
                if readable {
                    conn.readable = true;
                    self.pending_completions
                        .push(Completion::new(CompletionKind::Recv {
                            conn_id: ConnId::new(token),
                        }));
                }
                if writable {
                    conn.writable = true;
                    self.pending_completions
                        .push(Completion::new(CompletionKind::SendReady {
                            conn_id: ConnId::new(token),
                        }));
                }
                if closed {
                    self.pending_completions
                        .push(Completion::new(CompletionKind::Closed {
                            conn_id: ConnId::new(token),
                        }));
                }
                if error {
                    self.pending_completions
                        .push(Completion::new(CompletionKind::Error {
                            conn_id: ConnId::new(token),
                            error: io::Error::other("socket error"),
                        }));
                }
            }
        }

        Ok(self.pending_completions.len())
    }

    fn drain_completions(&mut self) -> Vec<Completion> {
        std::mem::take(&mut self.pending_completions)
    }

    fn connection_count(&self) -> usize {
        self.connections.len()
    }

    fn listener_count(&self) -> usize {
        self.listeners.len()
    }

    fn raw_fd(&self, id: ConnId) -> Option<RawFd> {
        self.connections
            .get(id.as_usize())
            .map(|c| c.stream.as_raw_fd())
    }
}

impl MioDriver {
    /// Accept all pending connections on a listener.
    fn accept_pending(&mut self, listener_id: usize) {
        let listener = match self.listeners.get(listener_id) {
            Some(l) => l,
            None => return,
        };

        loop {
            match listener.listener.accept() {
                Ok((mut stream, addr)) => {
                    // Register the new connection
                    let entry = self.connections.vacant_entry();
                    let conn_id = entry.key();

                    if let Err(e) = self.poll.registry().register(
                        &mut stream,
                        Token(conn_id),
                        Interest::READABLE | Interest::WRITABLE,
                    ) {
                        self.pending_completions.push(Completion::new(
                            CompletionKind::ListenerError {
                                listener_id: ListenerId::new(listener_id),
                                error: e,
                            },
                        ));
                        continue;
                    }

                    entry.insert(MioConnection {
                        stream,
                        readable: false,
                        writable: true,
                    });

                    self.pending_completions
                        .push(Completion::new(CompletionKind::Accept {
                            listener_id: ListenerId::new(listener_id),
                            conn_id: ConnId::new(conn_id),
                            addr,
                        }));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // No more pending connections
                    break;
                }
                Err(e) => {
                    self.pending_completions
                        .push(Completion::new(CompletionKind::ListenerError {
                            listener_id: ListenerId::new(listener_id),
                            error: e,
                        }));
                    break;
                }
            }
        }
    }

    /// Check if a connection is currently readable.
    pub fn is_readable(&self, id: ConnId) -> bool {
        self.connections
            .get(id.as_usize())
            .map(|c| c.readable)
            .unwrap_or(false)
    }

    /// Check if a connection is currently writable.
    pub fn is_writable(&self, id: ConnId) -> bool {
        self.connections
            .get(id.as_usize())
            .map(|c| c.writable)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mio_driver_new() {
        let driver = MioDriver::new();
        assert!(driver.is_ok());
        let driver = driver.unwrap();
        assert_eq!(driver.connection_count(), 0);
        assert_eq!(driver.listener_count(), 0);
    }

    #[test]
    fn test_mio_driver_with_config() {
        let driver = MioDriver::with_config(8192, 1024);
        assert!(driver.is_ok());
        let driver = driver.unwrap();
        assert_eq!(driver.connection_count(), 0);
        assert_eq!(driver.listener_count(), 0);
    }

    #[test]
    fn test_is_readable_nonexistent() {
        let driver = MioDriver::new().unwrap();
        let id = ConnId::new(999);
        assert!(!driver.is_readable(id));
    }

    #[test]
    fn test_is_writable_nonexistent() {
        let driver = MioDriver::new().unwrap();
        let id = ConnId::new(999);
        assert!(!driver.is_writable(id));
    }

    #[test]
    fn test_raw_fd_nonexistent() {
        let driver = MioDriver::new().unwrap();
        let id = ConnId::new(999);
        assert!(driver.raw_fd(id).is_none());
    }

    #[test]
    fn test_close_nonexistent_connection() {
        let mut driver = MioDriver::new().unwrap();
        let id = ConnId::new(999);
        let result = driver.close(id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_close_nonexistent_listener() {
        let mut driver = MioDriver::new().unwrap();
        let id = ListenerId::new(999);
        let result = driver.close_listener(id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_send_nonexistent_connection() {
        let mut driver = MioDriver::new().unwrap();
        let id = ConnId::new(999);
        let result = driver.send(id, b"hello");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn test_recv_nonexistent_connection() {
        let mut driver = MioDriver::new().unwrap();
        let id = ConnId::new(999);
        let mut buf = [0u8; 64];
        let result = driver.recv(id, &mut buf);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn test_drain_completions_empty() {
        let mut driver = MioDriver::new().unwrap();
        let completions = driver.drain_completions();
        assert!(completions.is_empty());
    }

    #[test]
    fn test_poll_no_events() {
        let mut driver = MioDriver::new().unwrap();
        let result = driver.poll(Some(Duration::from_millis(1)));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_listen_and_close() {
        let mut driver = MioDriver::new().unwrap();
        // Use a random high port
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener_id = driver.listen(addr, 128);
        assert!(listener_id.is_ok());
        let listener_id = listener_id.unwrap();
        assert_eq!(driver.listener_count(), 1);

        // Close the listener
        let result = driver.close_listener(listener_id);
        assert!(result.is_ok());
        assert_eq!(driver.listener_count(), 0);
    }

    #[test]
    fn test_listen_ipv6() {
        let mut driver = MioDriver::new().unwrap();
        let addr: SocketAddr = "[::1]:0".parse().unwrap();
        let result = driver.listen(addr, 128);
        // IPv6 may not be available on all systems
        if let Ok(listener_id) = result {
            assert_eq!(driver.listener_count(), 1);
            driver.close_listener(listener_id).unwrap();
        }
    }

    #[test]
    fn test_register_connection() {
        use std::net::TcpListener;

        // Create a server to accept connections
        let server = TcpListener::bind("127.0.0.1:0").unwrap();
        let server_addr = server.local_addr().unwrap();

        // Connect a client
        let client = TcpStream::connect(server_addr).unwrap();

        // Register the client stream with the driver
        let mut driver = MioDriver::new().unwrap();
        let conn_id = driver.register(client);
        assert!(conn_id.is_ok());
        let conn_id = conn_id.unwrap();

        assert_eq!(driver.connection_count(), 1);
        assert!(driver.raw_fd(conn_id).is_some());

        // Close the connection
        driver.close(conn_id).unwrap();
        assert_eq!(driver.connection_count(), 0);
    }
}
