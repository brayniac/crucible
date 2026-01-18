//! Mio-based I/O driver using epoll/kqueue.
//!
//! This module provides a cross-platform I/O driver that works on
//! Linux, macOS, and other Unix systems.

use crate::driver::{IoDriver, RecvBuf};
use crate::recv_state::ConnectionRecvState;
use crate::types::{
    Completion, CompletionKind, ConnId, DriverCapabilities, ListenerId, RecvMeta, SendMeta,
    SendMode, UdpSocketId,
};
use crate::udp::{self, CMSG_BUFFER_SIZE};
use crate::zero_copy::BoxedZeroCopy;
use mio::net::{
    TcpListener as MioTcpListener, TcpStream as MioTcpStream, UdpSocket as MioUdpSocket,
};
use mio::{Events, Interest, Poll, Token};
use slab::Slab;
use std::io::{self, IoSlice, Read, Write};
use std::mem;
use std::net::{SocketAddr, TcpStream};
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd};
use std::time::Duration;

/// Token offset for listeners to avoid collision with connections.
const LISTENER_TOKEN_OFFSET: usize = 1 << 30;

/// Token offset for UDP sockets to avoid collision with connections and listeners.
const UDP_TOKEN_OFFSET: usize = 1 << 29;

/// Connection state for mio driver.
struct MioConnection {
    stream: MioTcpStream,
    readable: bool,
    writable: bool,
    /// Receive buffer state for unified API.
    recv_state: ConnectionRecvState,
    /// Generation counter to detect stale ConnIds after slot reuse.
    generation: u32,
}

/// Listener state for mio driver.
struct MioListener {
    listener: MioTcpListener,
    /// When true, accepted connections are not registered with poll.
    /// Instead, AcceptRaw completions are emitted with the raw fd.
    raw_mode: bool,
}

/// UDP socket state for mio driver.
struct MioUdpSocketState {
    socket: MioUdpSocket,
    /// Bound address (used to determine IPv4 vs IPv6).
    bound_addr: SocketAddr,
    readable: bool,
    writable: bool,
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
    udp_sockets: Slab<MioUdpSocketState>,
    pending_completions: Vec<Completion>,
    #[allow(dead_code)]
    buffer_size: usize,
    /// Generation counter for connection IDs to detect stale references.
    next_generation: u32,
    /// Configured send mode.
    send_mode: SendMode,
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
            udp_sockets: Slab::with_capacity(64),
            pending_completions: Vec::with_capacity(256),
            buffer_size,
            next_generation: 0,
            send_mode: SendMode::default(),
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
            raw_mode: false,
        });

        Ok(ListenerId::new(id))
    }

    fn listen_raw(&mut self, addr: SocketAddr, backlog: u32) -> io::Result<ListenerId> {
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
        // Note: NO SO_REUSEPORT for raw mode - single acceptor pattern
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
            raw_mode: true,
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
        let slot = entry.key();
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);

        // Register with poll
        self.poll.registry().register(
            &mut mio_stream,
            Token(slot),
            Interest::READABLE | Interest::WRITABLE,
        )?;

        let conn_id = ConnId::with_generation(slot, generation);

        entry.insert(MioConnection {
            stream: mio_stream,
            // Set writable: true because with edge-triggered mode, the non-blocking
            // connect may have already completed before we registered with poll.
            // Queue a SendReady completion to ensure the client tries to send immediately.
            readable: false,
            writable: true,
            recv_state: ConnectionRecvState::default(),
            generation,
        });

        // Queue a SendReady completion so the client tries to send immediately.
        // This handles the edge-triggered case where connect completed before registration.
        self.pending_completions
            .push(Completion::new(CompletionKind::SendReady { conn_id }));

        Ok(conn_id)
    }

    fn register_fd(&mut self, raw_fd: RawFd) -> io::Result<ConnId> {
        // Set non-blocking
        unsafe {
            let flags = libc::fcntl(raw_fd, libc::F_GETFL);
            libc::fcntl(raw_fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }

        let mut mio_stream = unsafe { MioTcpStream::from_raw_fd(raw_fd) };

        let entry = self.connections.vacant_entry();
        let slot = entry.key();
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);

        // Register with poll
        self.poll.registry().register(
            &mut mio_stream,
            Token(slot),
            Interest::READABLE | Interest::WRITABLE,
        )?;

        let conn_id = ConnId::with_generation(slot, generation);

        entry.insert(MioConnection {
            stream: mio_stream,
            // Set readable: true because with edge-triggered mode, data may already
            // be available on the socket when it's registered (e.g., client sent
            // data before server accepted). We queue a Recv completion to ensure
            // the server attempts to read immediately.
            readable: true,
            writable: true,
            recv_state: ConnectionRecvState::default(),
            generation,
        });

        // Queue a Recv completion so the server processes any data already available.
        // This handles the edge-triggered case where data arrived before registration.
        self.pending_completions
            .push(Completion::new(CompletionKind::Recv { conn_id }));
        Ok(conn_id)
    }

    fn close(&mut self, id: ConnId) -> io::Result<()> {
        let slot = id.slot();
        // Validate generation before removing - stale ConnId means connection was already replaced
        if let Some(conn) = self.connections.get(slot)
            && conn.generation != id.generation()
        {
            return Ok(());
        }
        if let Some(mut conn) = self.connections.try_remove(slot) {
            self.poll.registry().deregister(&mut conn.stream)?;
        }
        Ok(())
    }

    fn take_fd(&mut self, id: ConnId) -> io::Result<RawFd> {
        let slot = id.slot();
        // Validate generation before removing
        match self.connections.get(slot) {
            Some(conn) if conn.generation == id.generation() => {}
            Some(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "connection generation mismatch (stale ConnId)",
                ));
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "connection not found",
                ));
            }
        }
        if let Some(mut conn) = self.connections.try_remove(slot) {
            self.poll.registry().deregister(&mut conn.stream)?;
            // Extract the raw fd without closing it
            Ok(conn.stream.into_raw_fd())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "connection not found",
            ))
        }
    }

    fn send(&mut self, id: ConnId, data: &[u8]) -> io::Result<usize> {
        let conn = self
            .connections
            .get_mut(id.slot())
            .filter(|c| c.generation == id.generation())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "connection not found"))?;

        // Don't try to send if we know the socket isn't writable yet
        // (e.g., non-blocking connect in progress)
        if !conn.writable {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
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

    fn send_vectored(&mut self, id: ConnId, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        let conn = self
            .connections
            .get_mut(id.slot())
            .filter(|c| c.generation == id.generation())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "connection not found"))?;

        // Don't try to send if we know the socket isn't writable yet
        if !conn.writable {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }

        match conn.stream.write_vectored(bufs) {
            Ok(n) => Ok(n),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                conn.writable = false;
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    fn recv(&mut self, id: ConnId, buf: &mut [u8]) -> io::Result<usize> {
        let conn = self
            .connections
            .get_mut(id.slot())
            .filter(|c| c.generation == id.generation())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "connection not found"))?;

        match conn.stream.read(buf) {
            Ok(n) => Ok(n),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                conn.readable = false;
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    fn submit_recv(&mut self, _id: ConnId, _buf: &mut [u8]) -> io::Result<()> {
        // mio doesn't support async recv submission - use recv() after Recv completion
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "submit_recv not supported on mio backend, use recv() instead",
        ))
    }

    fn with_recv_buf(&mut self, id: ConnId, f: &mut dyn FnMut(&mut dyn RecvBuf)) -> io::Result<()> {
        let conn = self
            .connections
            .get_mut(id.slot())
            .filter(|c| c.generation == id.generation())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "connection not found"))?;

        // If the socket is readable, read all available data into the recv buffer
        // We loop because mio uses edge-triggered notifications
        while conn.readable {
            let spare = conn.recv_state.spare_capacity_mut();
            match conn.stream.read(spare) {
                Ok(0) => {
                    // EOF - connection closed by peer
                    conn.readable = false;
                    // Return EOF error so the caller knows the connection is closed
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed by peer",
                    ));
                }
                Ok(n) => {
                    conn.recv_state.commit_owned(n);
                    // Continue reading - there might be more data
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // No more data available right now
                    conn.readable = false;
                    break;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        // Create a wrapper that implements RecvBuf
        struct MioRecvBuf<'a> {
            state: &'a mut ConnectionRecvState,
        }

        impl RecvBuf for MioRecvBuf<'_> {
            fn as_slice(&self) -> &[u8] {
                self.state.as_slice()
            }

            fn len(&self) -> usize {
                self.state.available()
            }

            fn consume(&mut self, n: usize) {
                self.state.consume(n);
            }
        }

        let mut buf = MioRecvBuf {
            state: &mut conn.recv_state,
        };
        f(&mut buf);

        Ok(())
    }

    fn poll(&mut self, timeout: Option<Duration>) -> io::Result<usize> {
        // Note: We do NOT clear pending_completions here because register_fd()
        // may have queued Recv completions for newly registered connections.
        // drain_completions() clears the vec via mem::take().

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

            // Check if this is a UDP socket event
            if token >= UDP_TOKEN_OFFSET {
                let socket_id = token - UDP_TOKEN_OFFSET;
                if let Some(sock) = self.udp_sockets.get_mut(socket_id) {
                    if readable {
                        sock.readable = true;
                        self.pending_completions.push(Completion::new(
                            CompletionKind::UdpReadable {
                                socket_id: UdpSocketId::new(socket_id),
                            },
                        ));
                    }
                    if writable {
                        sock.writable = true;
                        self.pending_completions.push(Completion::new(
                            CompletionKind::UdpWritable {
                                socket_id: UdpSocketId::new(socket_id),
                            },
                        ));
                    }
                    if error {
                        self.pending_completions
                            .push(Completion::new(CompletionKind::UdpError {
                                socket_id: UdpSocketId::new(socket_id),
                                error: io::Error::other("UDP socket error"),
                            }));
                    }
                }
                continue;
            }

            // Connection event
            if let Some(conn) = self.connections.get_mut(token) {
                let conn_id = ConnId::with_generation(token, conn.generation);
                if readable {
                    conn.readable = true;
                    self.pending_completions
                        .push(Completion::new(CompletionKind::Recv { conn_id }));
                }
                if writable {
                    conn.writable = true;
                    self.pending_completions
                        .push(Completion::new(CompletionKind::SendReady { conn_id }));
                }
                if closed {
                    self.pending_completions
                        .push(Completion::new(CompletionKind::Closed { conn_id }));
                }
                if error {
                    self.pending_completions
                        .push(Completion::new(CompletionKind::Error {
                            conn_id,
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
            .get(id.slot())
            .filter(|c| c.generation == id.generation())
            .map(|c| c.stream.as_raw_fd())
    }

    // === UDP socket operations ===

    fn bind_udp(&mut self, addr: SocketAddr) -> io::Result<UdpSocketId> {
        // Create UDP socket
        let socket = socket2::Socket::new(
            match addr {
                SocketAddr::V4(_) => socket2::Domain::IPV4,
                SocketAddr::V6(_) => socket2::Domain::IPV6,
            },
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )?;

        socket.set_reuse_address(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;

        let raw_fd = socket.into_raw_fd();

        // Configure socket for ECN and pktinfo
        udp::configure_socket(raw_fd, &addr)?;

        // Convert to mio UdpSocket
        let mut mio_socket = unsafe { MioUdpSocket::from_raw_fd(raw_fd) };

        let entry = self.udp_sockets.vacant_entry();
        let id = entry.key();

        // Register with poll
        self.poll.registry().register(
            &mut mio_socket,
            Token(id + UDP_TOKEN_OFFSET),
            Interest::READABLE | Interest::WRITABLE,
        )?;

        entry.insert(MioUdpSocketState {
            socket: mio_socket,
            bound_addr: addr,
            readable: false,
            writable: true,
        });

        Ok(UdpSocketId::new(id))
    }

    fn close_udp(&mut self, id: UdpSocketId) -> io::Result<()> {
        if let Some(mut sock) = self.udp_sockets.try_remove(id.as_usize()) {
            self.poll.registry().deregister(&mut sock.socket)?;
        }
        Ok(())
    }

    fn submit_recvmsg(&mut self, _id: UdpSocketId, _buf: &mut [u8]) -> io::Result<()> {
        // mio doesn't support async recvmsg submission - use recvmsg() after UdpReadable
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "submit_recvmsg not supported on mio backend, use recvmsg() instead",
        ))
    }

    fn submit_sendmsg(
        &mut self,
        _id: UdpSocketId,
        _data: &[u8],
        _meta: &SendMeta,
    ) -> io::Result<()> {
        // mio doesn't support async sendmsg submission - use sendmsg() after UdpWritable
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "submit_sendmsg not supported on mio backend, use sendmsg() instead",
        ))
    }

    fn recvmsg(&mut self, id: UdpSocketId, buf: &mut [u8]) -> io::Result<RecvMeta> {
        let sock = self
            .udp_sockets
            .get_mut(id.as_usize())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "UDP socket not found"))?;

        let raw_fd = sock.socket.as_raw_fd();
        let bound_addr = sock.bound_addr;

        // Set up for recvmsg
        let mut iovec = libc::iovec {
            iov_base: buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        };

        let mut addr_storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
        let mut cmsg_buf = [0u8; CMSG_BUFFER_SIZE];

        let mut msghdr: libc::msghdr = unsafe { mem::zeroed() };
        msghdr.msg_name = &mut addr_storage as *mut _ as *mut libc::c_void;
        msghdr.msg_namelen = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        msghdr.msg_iov = &mut iovec;
        msghdr.msg_iovlen = 1;
        msghdr.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
        msghdr.msg_controllen = cmsg_buf.len() as _;
        msghdr.msg_flags = 0;

        let result = unsafe { libc::recvmsg(raw_fd, &mut msghdr, 0) };

        if result < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                sock.readable = false;
            }
            return Err(err);
        }

        let len = result as usize;

        // Parse source address
        let source = udp::sockaddr_to_std(&addr_storage, msghdr.msg_namelen).unwrap_or(bound_addr);

        // Parse control messages
        Ok(udp::parse_control_messages(&msghdr, source, len))
    }

    fn sendmsg(&mut self, id: UdpSocketId, data: &[u8], meta: &SendMeta) -> io::Result<usize> {
        let sock = self
            .udp_sockets
            .get_mut(id.as_usize())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "UDP socket not found"))?;

        let raw_fd = sock.socket.as_raw_fd();

        // Set ECN on the socket
        udp::set_ecn(raw_fd, &meta.dest, meta.ecn)?;

        // Convert destination address
        let (dest_storage, dest_len) = udp::std_to_sockaddr(&meta.dest);

        let mut iovec = libc::iovec {
            iov_base: data.as_ptr() as *mut libc::c_void,
            iov_len: data.len(),
        };

        let mut msghdr: libc::msghdr = unsafe { mem::zeroed() };
        msghdr.msg_name = &dest_storage as *const _ as *mut libc::c_void;
        msghdr.msg_namelen = dest_len;
        msghdr.msg_iov = &mut iovec;
        msghdr.msg_iovlen = 1;
        msghdr.msg_control = std::ptr::null_mut();
        msghdr.msg_controllen = 0;
        msghdr.msg_flags = 0;

        let result = unsafe { libc::sendmsg(raw_fd, &msghdr, 0) };

        if result < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                sock.writable = false;
            }
            return Err(err);
        }

        Ok(result as usize)
    }

    fn udp_socket_count(&self) -> usize {
        self.udp_sockets.len()
    }

    fn udp_raw_fd(&self, id: UdpSocketId) -> Option<RawFd> {
        self.udp_sockets
            .get(id.as_usize())
            .map(|s| s.socket.as_raw_fd())
    }

    fn capabilities(&self) -> DriverCapabilities {
        // Mio supports vectored IO via write_vectored
        DriverCapabilities::VECTORED_IO
    }

    fn send_mode(&self) -> SendMode {
        self.send_mode
    }

    fn set_send_mode(&mut self, mode: SendMode) {
        self.send_mode = mode;
    }

    fn send_owned(&mut self, id: ConnId, buffer: BoxedZeroCopy) -> io::Result<usize> {
        let slot = id.slot();

        // For mio, we can't do true zero-copy - the kernel copies during write
        // We hold the buffer during the write call, then drop it after
        let slices = buffer.io_slices();

        // Try to write
        let conn = self
            .connections
            .get_mut(slot)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "connection not found"))?;

        // Check generation
        if conn.generation != id.generation() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "stale connection ID",
            ));
        }

        // Write all slices - mio/epoll will copy to kernel buffers
        // On success or partial write, buffer is dropped (releasing segment ref)
        // On WouldBlock, buffer is also dropped - caller handles retry
        conn.stream.write_vectored(&slices)
    }
}

impl MioDriver {
    /// Accept all pending connections on a listener.
    fn accept_pending(&mut self, listener_id: usize) {
        let listener = match self.listeners.get(listener_id) {
            Some(l) => l,
            None => return,
        };

        let raw_mode = listener.raw_mode;

        loop {
            match listener.listener.accept() {
                Ok((stream, addr)) => {
                    if raw_mode {
                        // Raw mode: don't register, emit AcceptRaw with raw fd
                        // Set TCP_NODELAY before handing off
                        let raw_fd = stream.into_raw_fd();
                        unsafe {
                            let optval: libc::c_int = 1;
                            libc::setsockopt(
                                raw_fd,
                                libc::IPPROTO_TCP,
                                libc::TCP_NODELAY,
                                &optval as *const _ as *const libc::c_void,
                                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                            );
                        }

                        self.pending_completions
                            .push(Completion::new(CompletionKind::AcceptRaw {
                                listener_id: ListenerId::new(listener_id),
                                raw_fd,
                                addr,
                            }));
                    } else {
                        // Normal mode: register the new connection
                        let mut mio_stream =
                            unsafe { MioTcpStream::from_raw_fd(stream.into_raw_fd()) };

                        let entry = self.connections.vacant_entry();
                        let slot = entry.key();
                        let generation = self.next_generation;
                        self.next_generation = self.next_generation.wrapping_add(1);

                        if let Err(e) = self.poll.registry().register(
                            &mut mio_stream,
                            Token(slot),
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
                            stream: mio_stream,
                            readable: false,
                            writable: true,
                            recv_state: ConnectionRecvState::default(),
                            generation,
                        });

                        self.pending_completions
                            .push(Completion::new(CompletionKind::Accept {
                                listener_id: ListenerId::new(listener_id),
                                conn_id: ConnId::with_generation(slot, generation),
                                addr,
                            }));
                    }
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
            .get(id.slot())
            .filter(|c| c.generation == id.generation())
            .map(|c| c.readable)
            .unwrap_or(false)
    }

    /// Check if a connection is currently writable.
    pub fn is_writable(&self, id: ConnId) -> bool {
        self.connections
            .get(id.slot())
            .filter(|c| c.generation == id.generation())
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
