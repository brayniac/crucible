use std::io;
use std::os::fd::RawFd;

use io_uring::types::{Fd, Fixed};
use io_uring::{IoUring, opcode};

use crate::buffer::fixed::FixedBufferRegistry;
use crate::buffer::provided::ProvidedBufRing;
use crate::completion::{OpTag, UserData};
use crate::config::Config;

/// Wrapper around IoUring providing high-level SQE submission helpers.
pub struct Ring {
    pub(crate) ring: IoUring,
    /// Recv buffer group ID for multishot recv.
    bgid: u16,
}

impl Ring {
    /// Create and configure the io_uring instance.
    pub fn setup(config: &Config) -> io::Result<Self> {
        let cq_entries = config
            .sq_entries
            .checked_mul(4)
            .unwrap_or(config.sq_entries);

        let mut builder = IoUring::builder();
        builder.setup_cqsize(cq_entries);
        builder.setup_coop_taskrun();
        builder.setup_single_issuer();

        if config.sqpoll {
            builder.setup_sqpoll(config.sqpoll_idle_ms);
            if let Some(cpu) = config.sqpoll_cpu {
                builder.setup_sqpoll_cpu(cpu);
            }
            // DEFER_TASKRUN is incompatible with SQPOLL (kernel returns EINVAL).
        } else {
            builder.setup_defer_taskrun();
        }

        let ring = builder.build(config.sq_entries)?;

        Ok(Ring {
            ring,
            bgid: config.recv_buffer.bgid,
        })
    }

    /// Register fixed buffers (user memory regions).
    pub fn register_buffers(&self, registry: &FixedBufferRegistry) -> io::Result<()> {
        let iovecs = registry.iovecs();
        if iovecs.is_empty() {
            return Ok(());
        }
        // Safety: iovecs point to valid memory that outlives the registration.
        unsafe {
            self.ring.submitter().register_buffers(iovecs)?;
        }
        Ok(())
    }

    /// Register a sparse file table for direct descriptors.
    pub fn register_files_sparse(&self, count: u32) -> io::Result<()> {
        self.ring.submitter().register_files_sparse(count)?;
        Ok(())
    }

    /// Update registered file table at given offset.
    pub fn register_files_update(&self, offset: u32, fds: &[RawFd]) -> io::Result<()> {
        self.ring.submitter().register_files_update(offset, fds)?;
        Ok(())
    }

    /// Register the provided buffer ring with the kernel.
    pub fn register_buf_ring(&self, provided: &ProvidedBufRing) -> io::Result<()> {
        // Safety: ring_addr points to valid mmap'd memory that outlives the registration.
        unsafe {
            self.ring.submitter().register_buf_ring_with_flags(
                provided.ring_addr(),
                provided.ring_entries() as u16,
                provided.bgid(),
                0,
            )?;
        }
        Ok(())
    }

    /// Submit a multishot recv with provided buffer ring for a connection.
    pub fn submit_multishot_recv(&mut self, conn_index: u32) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::RecvMulti, conn_index, 0);
        let entry = opcode::RecvMulti::new(Fixed(conn_index), self.bgid)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a copied send. The data must be in a SendCopyPool slot.
    /// The pool slot index is stored in the payload for release on CQE.
    pub fn submit_send_copied(
        &mut self,
        conn_index: u32,
        ptr: *const u8,
        len: u32,
        pool_slot: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::Send, conn_index, pool_slot as u32);
        let entry = opcode::Send::new(Fixed(conn_index), ptr, len)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a SendMsgZc operation.
    /// The slab index is stored in the payload for lookup on CQE.
    pub fn submit_send_msg_zc(
        &mut self,
        conn_index: u32,
        msg: *const libc::msghdr,
        slab_idx: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::SendMsgZc, conn_index, slab_idx as u32);
        let entry = opcode::SendMsgZc::new(Fixed(conn_index), msg)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a TLS-internal send (handshake, alert). Uses OpTag::TlsSend
    /// so the CQE handler releases the pool slot without calling on_send_complete.
    #[cfg(feature = "tls")]
    pub fn submit_tls_send(
        &mut self,
        conn_index: u32,
        ptr: *const u8,
        len: u32,
        pool_slot: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::TlsSend, conn_index, pool_slot as u32);
        let entry = opcode::Send::new(Fixed(conn_index), ptr, len)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a TLS-internal send with IOSQE_IO_LINK. Used for close_notify
    /// so the subsequent Close SQE is chained and only runs after the send completes.
    #[cfg(feature = "tls")]
    pub fn submit_tls_send_linked(
        &mut self,
        conn_index: u32,
        ptr: *const u8,
        len: u32,
        pool_slot: u16,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::TlsSend, conn_index, pool_slot as u32);
        let entry = opcode::Send::new(Fixed(conn_index), ptr, len)
            .build()
            .user_data(user_data.raw())
            .flags(io_uring::squeue::Flags::IO_LINK);
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit an eventfd read (8 bytes).
    pub fn submit_eventfd_read(&mut self, eventfd: RawFd, buf: *mut u8) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::EventFdRead, 0, 0);
        let entry = opcode::Read::new(Fd(eventfd), buf, 8)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a close for a direct file descriptor.
    pub fn submit_close(&mut self, conn_index: u32) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::Close, conn_index, 0);
        let entry = opcode::Close::new(Fixed(conn_index))
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit an async connect for a direct file descriptor.
    pub fn submit_connect(
        &mut self,
        conn_index: u32,
        addr: *const libc::sockaddr,
        addrlen: libc::socklen_t,
    ) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::Connect, conn_index, 0);
        let entry = opcode::Connect::new(Fixed(conn_index), addr, addrlen)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a timeout SQE. The timespec must remain valid until the CQE arrives.
    pub fn submit_timeout(
        &mut self,
        timespec: *const io_uring::types::Timespec,
        user_data: UserData,
    ) -> io::Result<()> {
        let entry = opcode::Timeout::new(timespec)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit an async cancel targeting a specific user_data value.
    pub fn submit_async_cancel(
        &mut self,
        target_user_data: u64,
        conn_index: u32,
    ) -> io::Result<()> {
        let ud = UserData::encode(OpTag::Cancel, conn_index, 0);
        let entry = opcode::AsyncCancel::new(target_user_data)
            .build()
            .user_data(ud.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit a shutdown(SHUT_WR) for a connection.
    pub fn submit_shutdown(&mut self, conn_index: u32) -> io::Result<()> {
        let user_data = UserData::encode(OpTag::Shutdown, conn_index, 0);
        let entry = opcode::Shutdown::new(Fixed(conn_index), libc::SHUT_WR)
            .build()
            .user_data(user_data.raw());
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit all pending SQEs and wait for at least `min_complete` CQEs.
    pub fn submit_and_wait(&self, min_complete: u32) -> io::Result<()> {
        self.ring
            .submitter()
            .submit_and_wait(min_complete as usize)?;
        Ok(())
    }

    /// Submit a timeout SQE that fires after the given duration.
    /// Produces a CQE with the given user_data when it fires (-ETIME)
    /// or is cancelled (-ECANCELED).
    pub fn submit_tick_timeout(
        &mut self,
        ts: *const io_uring::types::Timespec,
        user_data: u64,
    ) -> io::Result<()> {
        let entry = opcode::Timeout::new(ts).build().user_data(user_data);
        unsafe {
            self.push_sqe(entry)?;
        }
        Ok(())
    }

    /// Submit pending SQEs without waiting. Used for mid-iteration flush.
    pub fn flush(&self) -> io::Result<()> {
        self.ring.submit()?;
        Ok(())
    }

    /// Push an SQE to the submission queue.
    ///
    /// # Safety
    /// The SQE must reference valid memory for the lifetime of the operation.
    pub(crate) unsafe fn push_sqe(
        &mut self,
        entry: io_uring::squeue::Entry,
    ) -> io::Result<()> {
        // Try to push; if SQ is full, submit first to make room.
        unsafe {
            if self.ring.submission().push(&entry).is_err() {
                self.ring.submit()?;
                self.ring
                    .submission()
                    .push(&entry)
                    .map_err(|_| io::Error::other("SQ still full after submit"))?;
            }
        }
        Ok(())
    }

    /// Push a chain of linked SQEs atomically.
    ///
    /// Sets `IOSQE_IO_LINK` on all entries except the last, so the kernel
    /// executes them sequentially. All entries are pushed via `push_multiple`
    /// to guarantee contiguous placement in the SQ.
    ///
    /// # Safety
    /// All SQEs must reference valid memory for the lifetime of their operations.
    pub(crate) unsafe fn push_sqe_chain(
        &mut self,
        entries: &mut [io_uring::squeue::Entry],
    ) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        if entries.len() == 1 {
            return unsafe { self.push_sqe(entries[0].clone()) };
        }

        // Set IO_LINK on all entries except the last.
        let last = entries.len() - 1;
        for entry in entries[..last].iter_mut() {
            *entry = entry.clone().flags(io_uring::squeue::Flags::IO_LINK);
        }

        // Ensure enough room in the SQ for the entire chain.
        {
            let sq = self.ring.submission();
            if sq.capacity() - sq.len() < entries.len() {
                drop(sq);
                self.ring.submit()?;
                let sq = self.ring.submission();
                if sq.capacity() - sq.len() < entries.len() {
                    return Err(io::Error::other("SQ too small for chain"));
                }
            }
        }

        // Atomic push of the entire chain.
        unsafe {
            self.ring
                .submission()
                .push_multiple(entries)
                .map_err(|_| io::Error::other("SQ full after flush for chain"))?;
        }
        Ok(())
    }
}
