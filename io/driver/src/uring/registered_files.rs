//! Registered file descriptor table for io_uring.
//!
//! Maps connection tokens to registered fd indices for faster kernel operations.
//! Uses a sparse file table with a free list for slot allocation.

/// Registered file descriptor table.
///
/// Uses a free list to efficiently track available slots in the
/// sparse registered file table.
pub struct RegisteredFiles {
    /// Free slot indices.
    free_slots: Vec<u32>,
}

impl RegisteredFiles {
    /// Create a new registered files table with the given capacity.
    pub fn new(capacity: u32) -> Self {
        Self {
            free_slots: (0..capacity).rev().collect(),
        }
    }

    /// Allocate a slot for a new fd.
    ///
    /// Returns the slot index, or None if no slots are available.
    #[inline]
    pub fn alloc(&mut self) -> Option<u32> {
        self.free_slots.pop()
    }

    /// Free a slot, returning it to the pool.
    #[inline]
    pub fn free(&mut self, slot: u32) {
        self.free_slots.push(slot);
    }

    /// Get the number of available slots.
    #[inline]
    #[allow(dead_code)]
    pub fn available(&self) -> usize {
        self.free_slots.len()
    }
}
