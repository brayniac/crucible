use std::io;
use std::net::SocketAddr;

use crate::buffer::send_copy::SendCopyPool;
use crate::buffer::send_slab::{InFlightSendSlab, MAX_GUARDS, MAX_IOVECS};
use crate::guard::GuardBox;

/// Opaque connection token handed to the EventHandler.
/// Encodes the connection index and generation for stale detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnToken {
    pub(crate) index: u32,
    pub(crate) generation: u32,
}

impl ConnToken {
    pub(crate) fn new(index: u32, generation: u32) -> Self {
        ConnToken { index, generation }
    }

    /// Returns the connection slot index. Useful for indexing into per-connection arrays.
    pub fn index(&self) -> usize {
        self.index as usize
    }
}

/// The context provided to EventHandler callbacks for issuing operations.
///
/// This is a short-lived borrow into the driver's internal state.
pub struct DriverCtx<'a> {
    pub(crate) ring: &'a mut crate::ring::Ring,
    pub(crate) connections: &'a mut crate::connection::ConnectionTable,
    pub(crate) fixed_buffers: &'a mut crate::buffer::fixed::FixedBufferRegistry,
    pub(crate) send_copy_pool: &'a mut SendCopyPool,
    pub(crate) send_slab: &'a mut InFlightSendSlab,
    /// Raw pointer for borrow splitting. Null when plaintext.
    #[cfg(feature = "tls")]
    pub(crate) tls_table: *mut crate::tls::TlsTable,
    pub(crate) shutdown_requested: &'a mut bool,
    /// Pre-allocated sockaddr storage for outbound connect SQEs.
    pub(crate) connect_addrs: &'a mut Vec<libc::sockaddr_storage>,
    /// Whether to set TCP_NODELAY on outbound connections.
    pub(crate) tcp_nodelay: bool,
    /// Pre-allocated timespec storage for connect timeouts.
    pub(crate) connect_timespecs: &'a mut Vec<io_uring::types::Timespec>,
}

impl<'a> DriverCtx<'a> {
    /// Request shutdown of this worker's event loop.
    /// The worker will stop after the current iteration completes.
    pub fn request_shutdown(&mut self) {
        *self.shutdown_requested = true;
    }

    /// Get the peer address for a connection.
    pub fn peer_addr(&self, conn: ConnToken) -> Option<SocketAddr> {
        let cs = self.connections.get(conn.index)?;
        if cs.generation != conn.generation {
            return None;
        }
        cs.peer_addr
    }

    /// Check if a connection is outbound (initiated via connect/connect_tls).
    pub fn is_outbound(&self, conn: ConnToken) -> bool {
        self.connections
            .get(conn.index)
            .map(|cs| cs.generation == conn.generation && cs.outbound)
            .unwrap_or(false)
    }

    /// Get TLS session information for a connection.
    #[cfg(feature = "tls")]
    pub fn tls_info(&self, conn: ConnToken) -> Option<crate::tls::TlsInfo> {
        let cs = self.connections.get(conn.index)?;
        if cs.generation != conn.generation {
            return None;
        }
        if self.tls_table.is_null() {
            return None;
        }
        let tls_table = unsafe { &*self.tls_table };
        tls_table.get_info(conn.index)
    }

    /// Regular (copying) send — copies data into library-owned pool before SQE submission.
    pub fn send(&mut self, conn: ConnToken, data: &[u8]) -> io::Result<()> {
        let conn_state = self
            .connections
            .get(conn.index)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "invalid connection"))?;
        if conn_state.generation != conn.generation {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "stale connection",
            ));
        }

        #[cfg(feature = "tls")]
        if !self.tls_table.is_null() {
            let tls_table = unsafe { &mut *self.tls_table };
            if tls_table.get_mut(conn.index).is_some() {
                return crate::tls::encrypt_and_send(
                    tls_table,
                    self.ring,
                    self.send_copy_pool,
                    conn.index,
                    data,
                );
            }
        }

        let (slot, ptr, len) = self
            .send_copy_pool
            .copy_in(data)
            .ok_or_else(|| io::Error::other("send copy pool exhausted"))?;
        self.ring.submit_send_copied(conn.index, ptr, len, slot)?;
        Ok(())
    }

    /// Begin building a scatter-gather send with mixed copy + zero-copy guard parts.
    pub fn send_parts(&mut self, conn: ConnToken) -> SendBuilder<'_, 'a> {
        SendBuilder {
            ctx: self,
            conn,
            parts: [PartSlot::Empty; MAX_IOVECS],
            part_count: 0,
            copy_slices: [(std::ptr::null(), 0); MAX_IOVECS],
            copy_count: 0,
            total_copy_len: 0,
            guards: [None, None, None, None],
            guard_count: 0,
            total_len: 0,
            error: None,
        }
    }

    /// Close a connection.
    pub fn close(&mut self, conn: ConnToken) {
        if let Some(conn_state) = self.connections.get_mut(conn.index) {
            if conn_state.generation != conn.generation {
                return;
            }
            conn_state.recv_mode = crate::connection::RecvMode::Closed;

            // Graceful TLS shutdown: send close_notify before closing.
            #[cfg(feature = "tls")]
            if !self.tls_table.is_null() {
                let tls_table = unsafe { &mut *self.tls_table };
                tls_table.send_close_notify(conn.index, self.ring, self.send_copy_pool);
            }

            let _ = self.ring.submit_close(conn.index);
        }
    }

    /// Shutdown the write side of a connection.
    pub fn shutdown_write(&mut self, conn: ConnToken) {
        if let Some(conn_state) = self.connections.get(conn.index) {
            if conn_state.generation != conn.generation {
                return;
            }
            let _ = self.ring.submit_shutdown(conn.index);
        }
    }

    /// Initiate an outbound TCP connection. Returns immediately with a `ConnToken`.
    /// The `on_connect` callback fires when the TCP handshake completes (or fails).
    pub fn connect(&mut self, addr: SocketAddr) -> Result<ConnToken, crate::error::Error> {
        let conn_index = self
            .connections
            .allocate_outbound()
            .ok_or(crate::error::Error::ConnectionLimitReached)?;
        let generation = self.connections.generation(conn_index);

        // Store peer address.
        if let Some(cs) = self.connections.get_mut(conn_index) {
            cs.peer_addr = Some(addr);
        }

        // Create socket.
        let domain = if addr.is_ipv4() {
            libc::AF_INET
        } else {
            libc::AF_INET6
        };
        let raw_fd = unsafe {
            libc::socket(
                domain,
                libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            )
        };
        if raw_fd < 0 {
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(io::Error::last_os_error()));
        }

        // Set TCP_NODELAY if configured.
        if self.tcp_nodelay {
            let optval: libc::c_int = 1;
            unsafe {
                libc::setsockopt(
                    raw_fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_NODELAY,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        // Register in the direct file table, then close the original fd.
        if let Err(e) = self.ring.register_files_update(conn_index, &[raw_fd]) {
            unsafe {
                libc::close(raw_fd);
            }
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(e));
        }
        unsafe {
            libc::close(raw_fd);
        }

        // Fill sockaddr_storage for the connect SQE.
        let addrlen = fill_sockaddr_storage(&mut self.connect_addrs[conn_index as usize], &addr);

        // Submit the async connect.
        if let Err(e) = self.ring.submit_connect(
            conn_index,
            &self.connect_addrs[conn_index as usize] as *const _ as *const libc::sockaddr,
            addrlen,
        ) {
            let _ = self.ring.register_files_update(conn_index, &[-1]);
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(e));
        }

        Ok(ConnToken::new(conn_index, generation))
    }

    /// Initiate an outbound TCP connection with a timeout.
    /// If the connection is not established within `timeout_ms`, `on_connect` fires
    /// with `Err(TimedOut)`.
    pub fn connect_with_timeout(
        &mut self,
        addr: SocketAddr,
        timeout_ms: u64,
    ) -> Result<ConnToken, crate::error::Error> {
        let token = self.connect(addr)?;
        self.arm_connect_timeout(token.index, timeout_ms);
        Ok(token)
    }

    /// Initiate an outbound TLS connection. Returns immediately with a `ConnToken`.
    /// The `on_connect` callback fires when both TCP + TLS handshakes complete (or fail).
    #[cfg(feature = "tls")]
    pub fn connect_tls(
        &mut self,
        addr: SocketAddr,
        server_name: &str,
    ) -> Result<ConnToken, crate::error::Error> {
        if self.tls_table.is_null() {
            return Err(crate::error::Error::RingSetup(
                "TLS not configured".to_string(),
            ));
        }
        let tls_table = unsafe { &mut *self.tls_table };
        if !tls_table.has_client_config() {
            return Err(crate::error::Error::RingSetup(
                "TLS client config not set".to_string(),
            ));
        }

        let conn_index = self
            .connections
            .allocate_outbound()
            .ok_or(crate::error::Error::ConnectionLimitReached)?;
        let generation = self.connections.generation(conn_index);

        // Store peer address.
        if let Some(cs) = self.connections.get_mut(conn_index) {
            cs.peer_addr = Some(addr);
        }

        // Create socket.
        let domain = if addr.is_ipv4() {
            libc::AF_INET
        } else {
            libc::AF_INET6
        };
        let raw_fd = unsafe {
            libc::socket(
                domain,
                libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            )
        };
        if raw_fd < 0 {
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(io::Error::last_os_error()));
        }

        // Set TCP_NODELAY if configured.
        if self.tcp_nodelay {
            let optval: libc::c_int = 1;
            unsafe {
                libc::setsockopt(
                    raw_fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_NODELAY,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        // Register in the direct file table, then close the original fd.
        if let Err(e) = self.ring.register_files_update(conn_index, &[raw_fd]) {
            unsafe {
                libc::close(raw_fd);
            }
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(e));
        }
        unsafe {
            libc::close(raw_fd);
        }

        // Create TLS client state (buffers ClientHello internally).
        let sni = rustls::pki_types::ServerName::try_from(server_name.to_owned()).map_err(|e| {
            let _ = self.ring.register_files_update(conn_index, &[-1]);
            self.connections.release(conn_index);
            crate::error::Error::RingSetup(format!("invalid server name: {e}"))
        })?;
        tls_table.create_client(conn_index, sni);

        // Fill sockaddr_storage for the connect SQE.
        let addrlen = fill_sockaddr_storage(&mut self.connect_addrs[conn_index as usize], &addr);

        // Submit the async connect.
        if let Err(e) = self.ring.submit_connect(
            conn_index,
            &self.connect_addrs[conn_index as usize] as *const _ as *const libc::sockaddr,
            addrlen,
        ) {
            tls_table.remove(conn_index);
            let _ = self.ring.register_files_update(conn_index, &[-1]);
            self.connections.release(conn_index);
            return Err(crate::error::Error::Io(e));
        }

        Ok(ConnToken::new(conn_index, generation))
    }

    /// Initiate an outbound TLS connection with a timeout.
    #[cfg(feature = "tls")]
    pub fn connect_tls_with_timeout(
        &mut self,
        addr: SocketAddr,
        server_name: &str,
        timeout_ms: u64,
    ) -> Result<ConnToken, crate::error::Error> {
        let token = self.connect_tls(addr, server_name)?;
        self.arm_connect_timeout(token.index, timeout_ms);
        Ok(token)
    }

    /// Cancel pending operations on a connection.
    pub fn cancel(&mut self, conn: ConnToken) -> io::Result<()> {
        let cs = self
            .connections
            .get_mut(conn.index)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "invalid connection"))?;
        if cs.generation != conn.generation {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "stale connection",
            ));
        }

        // Determine target op to cancel.
        let target_tag = match cs.recv_mode {
            crate::connection::RecvMode::Connecting => crate::completion::OpTag::Connect,
            crate::connection::RecvMode::Multi => crate::completion::OpTag::RecvMulti,
            crate::connection::RecvMode::Closed => {
                return Ok(()); // nothing to cancel
            }
        };

        // If cancelling a connect with an armed timeout, also cancel the timeout
        // so the Connect ECANCELED CQE is handled as user-initiated (not timeout-initiated).
        if matches!(target_tag, crate::completion::OpTag::Connect) && cs.connect_timeout_armed {
            cs.connect_timeout_armed = false;
            let timeout_ud = crate::completion::UserData::encode(
                crate::completion::OpTag::Timeout,
                conn.index,
                0,
            );
            let _ = self.ring.submit_async_cancel(timeout_ud.raw(), conn.index);
        }

        cs.recv_mode = crate::connection::RecvMode::Closed;

        let target_ud = crate::completion::UserData::encode(target_tag, conn.index, 0);
        self.ring.submit_async_cancel(target_ud.raw(), conn.index)?;
        Ok(())
    }

    /// Arm a connect timeout for the given connection index.
    fn arm_connect_timeout(&mut self, conn_index: u32, timeout_ms: u64) {
        let ts = &mut self.connect_timespecs[conn_index as usize];
        *ts = io_uring::types::Timespec::new()
            .sec(timeout_ms / 1000)
            .nsec((timeout_ms % 1000) as u32 * 1_000_000);

        let ud =
            crate::completion::UserData::encode(crate::completion::OpTag::Timeout, conn_index, 0);
        if self.ring.submit_timeout(ts as *const _, ud).is_ok()
            && let Some(cs) = self.connections.get_mut(conn_index)
        {
            cs.connect_timeout_armed = true;
        }
    }
}

/// Write a `SocketAddr` into a `sockaddr_storage`, return the address length.
fn fill_sockaddr_storage(
    storage: &mut libc::sockaddr_storage,
    addr: &SocketAddr,
) -> libc::socklen_t {
    unsafe {
        std::ptr::write_bytes(
            storage as *mut _ as *mut u8,
            0,
            std::mem::size_of::<libc::sockaddr_storage>(),
        );
    }
    match addr {
        SocketAddr::V4(v4) => {
            let sa = storage as *mut _ as *mut libc::sockaddr_in;
            unsafe {
                (*sa).sin_family = libc::AF_INET as libc::sa_family_t;
                (*sa).sin_port = v4.port().to_be();
                (*sa).sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            }
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
        }
        SocketAddr::V6(v6) => {
            let sa = storage as *mut _ as *mut libc::sockaddr_in6;
            unsafe {
                (*sa).sin6_family = libc::AF_INET6 as libc::sa_family_t;
                (*sa).sin6_port = v6.port().to_be();
                (*sa).sin6_flowinfo = v6.flowinfo();
                (*sa).sin6_addr.s6_addr = v6.ip().octets();
                (*sa).sin6_scope_id = v6.scope_id();
            }
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
        }
    }
}

/// Part type in a scatter-gather send.
#[derive(Clone, Copy)]
enum PartSlot {
    Empty,
    Copy { slice_idx: u8 },
    Guard { guard_idx: u8 },
}

/// Builder for scatter-gather sends with mixed copy + zero-copy guard parts.
pub struct SendBuilder<'b, 'a> {
    ctx: &'b mut DriverCtx<'a>,
    conn: ConnToken,
    parts: [PartSlot; MAX_IOVECS],
    part_count: u8,
    copy_slices: [(*const u8, usize); MAX_IOVECS],
    copy_count: u8,
    total_copy_len: usize,
    guards: [Option<GuardBox>; MAX_GUARDS],
    guard_count: u8,
    total_len: u32,
    error: Option<io::Error>,
}

impl<'b, 'a> SendBuilder<'b, 'a> {
    /// Add a copy part. The data will be copied into the send pool on `submit()`.
    /// The data reference must outlive the builder (guaranteed by the `'b` lifetime).
    pub fn copy(mut self, data: &'b [u8]) -> Self {
        if self.error.is_some() {
            return self;
        }
        if self.part_count as usize >= MAX_IOVECS {
            self.error = Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many send parts (max 8)",
            ));
            return self;
        }
        let idx = self.copy_count;
        self.copy_slices[idx as usize] = (data.as_ptr(), data.len());
        self.copy_count += 1;
        self.parts[self.part_count as usize] = PartSlot::Copy { slice_idx: idx };
        self.part_count += 1;
        self.total_len += data.len() as u32;
        self.total_copy_len += data.len();
        self
    }

    /// Add a zero-copy guard part. The guard keeps the memory alive until the kernel
    /// releases it via the ZC notification.
    pub fn guard(mut self, guard: GuardBox) -> Self {
        if self.error.is_some() {
            return self;
        }
        if self.part_count as usize >= MAX_IOVECS {
            self.error = Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many send parts (max 8)",
            ));
            return self;
        }
        if self.guard_count as usize >= MAX_GUARDS {
            self.error = Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many guards (max 4)",
            ));
            return self;
        }
        let (_, len) = guard.as_ptr_len();
        let gidx = self.guard_count;
        self.guards[gidx as usize] = Some(guard);
        self.guard_count += 1;
        self.parts[self.part_count as usize] = PartSlot::Guard { guard_idx: gidx };
        self.part_count += 1;
        self.total_len += len;
        self
    }

    /// Submit the scatter-gather send.
    pub fn submit(mut self) -> io::Result<()> {
        if let Some(e) = self.error.take() {
            return Err(e);
        }

        if self.part_count == 0 {
            return Ok(());
        }

        // Validate connection + generation.
        let conn_state = self
            .ctx
            .connections
            .get(self.conn.index)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "invalid connection"))?;
        if conn_state.generation != self.conn.generation {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "stale connection",
            ));
        }

        // TLS path: gather all data, encrypt, copy-send. Drop guards immediately.
        #[cfg(feature = "tls")]
        if !self.ctx.tls_table.is_null() {
            let tls_table = unsafe { &mut *self.ctx.tls_table };
            if tls_table.get_mut(self.conn.index).is_some() {
                return self.submit_tls(tls_table);
            }
        }

        // No guards: gather all copy parts into one pool slot, submit as regular Send.
        if self.guard_count == 0 {
            return self.submit_copy_only();
        }

        // With guards: build iovecs mixing copy pool subranges and guard pointers.
        self.submit_with_guards()
    }

    /// TLS fallback: gather all data into a contiguous buffer, encrypt, copy-send.
    #[cfg(feature = "tls")]
    fn submit_tls(mut self, tls_table: &mut crate::tls::TlsTable) -> io::Result<()> {
        let mut plaintext = Vec::with_capacity(self.total_len as usize);
        for i in 0..self.part_count as usize {
            match self.parts[i] {
                PartSlot::Copy { slice_idx } => {
                    let (ptr, len) = self.copy_slices[slice_idx as usize];
                    let data = unsafe { std::slice::from_raw_parts(ptr, len) };
                    plaintext.extend_from_slice(data);
                }
                PartSlot::Guard { guard_idx } => {
                    if let Some(ref g) = self.guards[guard_idx as usize] {
                        let (ptr, len) = g.as_ptr_len();
                        let data = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
                        plaintext.extend_from_slice(data);
                    }
                }
                PartSlot::Empty => {}
            }
        }
        // Drop guards — TLS encrypted copy-send doesn't need ZC
        for g in self.guards.iter_mut() {
            *g = None;
        }
        crate::tls::encrypt_and_send(
            tls_table,
            self.ctx.ring,
            self.ctx.send_copy_pool,
            self.conn.index,
            &plaintext,
        )
    }

    /// Copy-only path: gather all copy parts into one pool slot.
    fn submit_copy_only(self) -> io::Result<()> {
        let (slot, ptr, len) = unsafe {
            self.ctx.send_copy_pool.copy_in_gather(
                &self.copy_slices[..self.copy_count as usize],
                self.total_copy_len,
            )
        }
        .ok_or_else(|| io::Error::other("send copy pool exhausted"))?;
        self.ctx
            .ring
            .submit_send_copied(self.conn.index, ptr, len, slot)?;
        Ok(())
    }

    /// Mixed copy+guard path: allocate pool slot for copy data, build iovecs, submit SendMsgZc.
    #[allow(clippy::needless_range_loop)]
    fn submit_with_guards(self) -> io::Result<()> {
        let slot_size = self.ctx.send_copy_pool.slot_size() as usize;
        if self.total_copy_len > slot_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "total copy data exceeds send pool slot size",
            ));
        }

        // Allocate a pool slot for copy data (if any).
        let pool_slot = if self.total_copy_len > 0 {
            let (slot, pool_ptr, _pool_len) = unsafe {
                self.ctx.send_copy_pool.copy_in_gather(
                    &self.copy_slices[..self.copy_count as usize],
                    self.total_copy_len,
                )
            }
            .ok_or_else(|| io::Error::other("send copy pool exhausted"))?;

            // Now build iovecs. Copy parts point into subranges of the pool slot.
            let mut iovecs = [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; MAX_IOVECS];
            let mut copy_offset: usize = 0;
            for i in 0..self.part_count as usize {
                match self.parts[i] {
                    PartSlot::Copy { slice_idx } => {
                        let (_src_ptr, src_len) = self.copy_slices[slice_idx as usize];
                        iovecs[i] = libc::iovec {
                            iov_base: pool_ptr.wrapping_add(copy_offset) as *mut _,
                            iov_len: src_len,
                        };
                        copy_offset += src_len;
                    }
                    PartSlot::Guard { guard_idx } => {
                        let g = self.guards[guard_idx as usize].as_ref().unwrap();
                        let (gptr, glen) = g.as_ptr_len();
                        // Validate guard region (skip for unregistered heap memory).
                        let region = g.region();
                        if region != crate::buffer::fixed::RegionId::UNREGISTERED {
                            self.ctx
                                .fixed_buffers
                                .validate_region_ptr(region, gptr, glen)
                                .map_err(|e| {
                                    // Release pool slot on error.
                                    self.ctx.send_copy_pool.release(slot);
                                    io::Error::new(io::ErrorKind::InvalidInput, e.to_string())
                                })?;
                        }
                        iovecs[i] = libc::iovec {
                            iov_base: gptr as *mut _,
                            iov_len: glen as usize,
                        };
                    }
                    PartSlot::Empty => {}
                }
            }

            // Allocate slab entry (moves guards in).
            let iov_slice = &iovecs[..self.part_count as usize];
            let total_len = self.total_len;
            let (slab_idx, msg_ptr) = self
                .ctx
                .send_slab
                .allocate(
                    self.conn.index,
                    iov_slice,
                    slot,
                    self.guards,
                    self.guard_count,
                    total_len,
                )
                .ok_or_else(|| {
                    self.ctx.send_copy_pool.release(slot);
                    io::Error::other("send slab exhausted")
                })?;

            self.ctx
                .ring
                .submit_send_msg_zc(self.conn.index, msg_ptr, slab_idx)?;
            // Guards have been moved into the slab; prevent double-drop
            // (self.guards are now all None after the move above — the array was moved by value)
            slot
        } else {
            // No copy data, only guards.
            let mut iovecs = [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; MAX_IOVECS];
            for i in 0..self.part_count as usize {
                if let PartSlot::Guard { guard_idx } = self.parts[i] {
                    let g = self.guards[guard_idx as usize].as_ref().unwrap();
                    let (gptr, glen) = g.as_ptr_len();
                    let region = g.region();
                    if region != crate::buffer::fixed::RegionId::UNREGISTERED {
                        self.ctx
                            .fixed_buffers
                            .validate_region_ptr(region, gptr, glen)
                            .map_err(|e| {
                                io::Error::new(io::ErrorKind::InvalidInput, e.to_string())
                            })?;
                    }
                    iovecs[i] = libc::iovec {
                        iov_base: gptr as *mut _,
                        iov_len: glen as usize,
                    };
                }
            }

            let iov_slice = &iovecs[..self.part_count as usize];
            let total_len = self.total_len;
            let (slab_idx, msg_ptr) = self
                .ctx
                .send_slab
                .allocate(
                    self.conn.index,
                    iov_slice,
                    u16::MAX,
                    self.guards,
                    self.guard_count,
                    total_len,
                )
                .ok_or_else(|| io::Error::other("send slab exhausted"))?;

            self.ctx
                .ring
                .submit_send_msg_zc(self.conn.index, msg_ptr, slab_idx)?;
            return Ok(());
        };

        let _ = pool_slot; // used in slab allocation above
        Ok(())
    }
}

/// Trait that users implement to handle I/O events.
pub trait EventHandler: Send + 'static {
    /// New connection accepted. Multishot recv is already armed.
    fn on_accept(&mut self, ctx: &mut DriverCtx, conn: ConnToken);

    /// Outbound connection completed. Called when TCP connect succeeds/fails,
    /// or after TLS handshake completes for `connect_tls()`.
    fn on_connect(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken, _result: io::Result<()>) {}

    /// Contiguous data received. Returns the number of bytes consumed.
    /// Unconsumed bytes are kept for the next callback.
    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize;

    /// Send operation completed (the operation CQE, not the ZC notification).
    /// Reports the total original length on success.
    fn on_send_complete(&mut self, ctx: &mut DriverCtx, conn: ConnToken, result: io::Result<u32>);

    /// Connection closed (by peer, error, or explicit close).
    fn on_close(&mut self, ctx: &mut DriverCtx, conn: ConnToken);

    /// Called once per event loop iteration after all CQEs processed.
    fn on_tick(&mut self, _ctx: &mut DriverCtx) {}

    /// Create a new handler instance for each worker thread.
    fn create_for_worker(worker_id: usize) -> Self
    where
        Self: Sized;
}
