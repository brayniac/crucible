//! Ring-provided buffer pool using IORING_REGISTER_PBUF_RING.
//!
//! This avoids the SQE overhead of ProvideBuffers by using a shared ring
//! that the kernel can directly access to get buffers for recv operations.

use io_uring::types;
use std::io;
use std::sync::atomic::{AtomicU16, Ordering};

/// Ring-provided buffer pool.
///
/// Uses the io_uring buf_ring mechanism for efficient buffer management.
/// The kernel can directly pick buffers from the ring without requiring
/// additional SQE submissions.
pub struct BufRing {
    /// Page-aligned memory for ring entries + buffer data.
    /// Layout: [BufRingEntry; ring_entries] followed by buffer data.
    memory: *mut u8,
    memory_layout: std::alloc::Layout,
    /// Size of each buffer.
    buffer_size: usize,
    /// Number of entries (must be power of 2).
    ring_entries: u16,
    /// Pointer to the tail (within the ring entry area).
    tail: *const AtomicU16,
    /// Current tail value (cached for local updates).
    local_tail: u16,
    /// Mask for wrapping indices.
    mask: u16,
}

// Safety: BufRing manages its own memory and synchronization via atomics
unsafe impl Send for BufRing {}

impl BufRing {
    /// Create a new buffer ring.
    ///
    /// `ring_entries` must be a power of 2.
    pub fn new(ring_entries: u16, buffer_size: usize) -> io::Result<Self> {
        assert!(ring_entries.is_power_of_two());

        let ring_size = ring_entries as usize * std::mem::size_of::<types::BufRingEntry>();
        let buffer_data_size = ring_entries as usize * buffer_size;
        let total_size = ring_size + buffer_data_size;

        // Allocate page-aligned memory
        let page_size = 4096;
        let layout = std::alloc::Layout::from_size_align(total_size, page_size)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid layout"))?;

        let memory = unsafe { std::alloc::alloc_zeroed(layout) };
        if memory.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "allocation failed",
            ));
        }

        // The tail pointer is at the resv field of the first entry
        let tail = unsafe {
            types::BufRingEntry::tail(memory as *const types::BufRingEntry) as *const AtomicU16
        };

        let mut ring = Self {
            memory,
            memory_layout: layout,
            buffer_size,
            ring_entries,
            tail,
            local_tail: 0,
            mask: ring_entries - 1,
        };

        // Initialize all buffer entries
        for i in 0..ring_entries {
            ring.add_buffer(i);
        }
        // Commit all buffers
        ring.commit();

        Ok(ring)
    }

    /// Get ring base address for registration.
    #[inline]
    pub fn ring_addr(&self) -> u64 {
        self.memory as u64
    }

    /// Get pointer to buffer data area (after ring entries).
    #[inline]
    fn buffer_base(&self) -> *mut u8 {
        let ring_size = self.ring_entries as usize * std::mem::size_of::<types::BufRingEntry>();
        unsafe { self.memory.add(ring_size) }
    }

    /// Get a slice to a buffer by ID.
    #[inline]
    pub fn get(&self, buf_id: u16) -> &[u8] {
        let start = buf_id as usize * self.buffer_size;
        unsafe { std::slice::from_raw_parts(self.buffer_base().add(start), self.buffer_size) }
    }

    /// Add a buffer back to the ring (does not commit).
    #[inline]
    fn add_buffer(&mut self, buf_id: u16) {
        let idx = (self.local_tail & self.mask) as usize;
        let entry = unsafe { &mut *(self.memory as *mut types::BufRingEntry).add(idx) };

        let buf_addr = unsafe { self.buffer_base().add(buf_id as usize * self.buffer_size) };
        entry.set_addr(buf_addr as u64);
        entry.set_len(self.buffer_size as u32);
        entry.set_bid(buf_id);

        self.local_tail = self.local_tail.wrapping_add(1);
    }

    /// Commit pending buffer additions by updating the tail.
    ///
    /// Call this after one or more `return_buffer_deferred` calls to make
    /// the buffers visible to the kernel. Batching multiple returns before
    /// a single commit reduces atomic operation overhead.
    #[inline]
    pub fn commit(&self) {
        // Use release ordering so kernel sees the buffer data
        unsafe {
            (*self.tail).store(self.local_tail, Ordering::Release);
        }
    }

    /// Return a buffer to the ring without committing.
    ///
    /// Use this when returning multiple buffers, then call `commit()` once
    /// at the end to reduce atomic operation overhead.
    #[inline]
    pub fn return_buffer_deferred(&mut self, buf_id: u16) {
        self.add_buffer(buf_id);
    }

    /// Return a buffer to the ring (adds and commits immediately).
    ///
    /// For single buffer returns. When returning multiple buffers, prefer
    /// `return_buffer_deferred` followed by a single `commit()`.
    #[inline]
    pub fn return_buffer(&mut self, buf_id: u16) {
        self.add_buffer(buf_id);
        self.commit();
    }
}

impl Drop for BufRing {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.memory, self.memory_layout);
        }
    }
}
