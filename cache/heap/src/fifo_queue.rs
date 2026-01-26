//! Ring buffer FIFO queue for S3-FIFO eviction policy.
//!
//! This module provides a lock-free FIFO queue implemented as a ring buffer,
//! used for tracking items in the small and main queues of S3-FIFO.

use std::sync::atomic::{AtomicU32, Ordering};

/// Queue entry stored in the ring buffer.
///
/// Layout: 16 bytes
/// - `bucket_index`: u64 - Index into the hashtable bucket
/// - `item_info`: u64 - Packed item info from hashtable [TAG:12][FREQ:8][LOCATION:44]
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct QueueEntry {
    /// Index into the hashtable bucket where this item resides.
    pub bucket_index: u64,
    /// Packed item info from the hashtable (tag, frequency, location).
    pub item_info: u64,
}

impl QueueEntry {
    /// Create a new queue entry.
    #[inline]
    pub fn new(bucket_index: u64, item_info: u64) -> Self {
        Self {
            bucket_index,
            item_info,
        }
    }

    /// Get the location from the item info (lower 44 bits).
    #[allow(dead_code)]
    #[inline]
    pub fn location(&self) -> u64 {
        self.item_info & 0xFFF_FFFF_FFFF
    }

    /// Get the frequency from the item info (bits 44-51).
    #[allow(dead_code)]
    #[inline]
    pub fn frequency(&self) -> u8 {
        ((self.item_info >> 44) & 0xFF) as u8
    }

    /// Get the tag from the item info (upper 12 bits).
    #[allow(dead_code)]
    #[inline]
    pub fn tag(&self) -> u16 {
        ((self.item_info >> 52) & 0xFFF) as u16
    }
}

/// Ring buffer FIFO queue.
///
/// This is a single-producer-single-consumer queue optimized for eviction.
/// In practice, only the eviction thread modifies head, and only the insertion
/// thread modifies tail.
pub struct FifoQueue {
    /// Fixed-size entry storage.
    entries: Box<[QueueEntry]>,
    /// Head index - points to the next entry to evict (consume).
    head: AtomicU32,
    /// Tail index - points to the next slot to insert (produce).
    tail: AtomicU32,
    /// Queue capacity (power of 2 for efficient masking).
    capacity: u32,
    /// Mask for wrapping indices (capacity - 1).
    mask: u32,
}

impl FifoQueue {
    /// Create a new FIFO queue with the given capacity.
    ///
    /// Capacity will be rounded up to the next power of 2.
    pub fn new(capacity: u32) -> Self {
        // Round up to power of 2
        let capacity = capacity.next_power_of_two();
        let mask = capacity - 1;

        let entries = vec![QueueEntry::default(); capacity as usize].into_boxed_slice();

        Self {
            entries,
            head: AtomicU32::new(0),
            tail: AtomicU32::new(0),
            capacity,
            mask,
        }
    }

    /// Push an entry to the tail of the queue.
    ///
    /// Returns `true` if successful, `false` if the queue is full.
    #[inline]
    pub fn push(&self, entry: QueueEntry) -> bool {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);

        // Check if full (tail would wrap to head)
        let next_tail = (tail + 1) & self.mask;
        if next_tail == head {
            return false;
        }

        // Store entry at tail position
        // SAFETY: We have exclusive write access to the tail position
        unsafe {
            let slot = self.entries.as_ptr().add((tail & self.mask) as usize) as *mut QueueEntry;
            std::ptr::write_volatile(slot, entry);
        }

        // Advance tail
        self.tail.store(next_tail, Ordering::Release);
        true
    }

    /// Pop an entry from the head of the queue.
    ///
    /// Returns `None` if the queue is empty.
    #[inline]
    pub fn pop(&self) -> Option<QueueEntry> {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);

        // Check if empty
        if head == tail {
            return None;
        }

        // Read entry at head position
        // SAFETY: We have exclusive read access to the head position
        let entry = unsafe {
            let slot = self.entries.as_ptr().add((head & self.mask) as usize);
            std::ptr::read_volatile(slot)
        };

        // Advance head
        let next_head = (head + 1) & self.mask;
        self.head.store(next_head, Ordering::Release);

        Some(entry)
    }

    /// Peek at the head entry without removing it.
    #[allow(dead_code)]
    #[inline]
    pub fn peek(&self) -> Option<QueueEntry> {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);

        if head == tail {
            return None;
        }

        unsafe {
            let slot = self.entries.as_ptr().add((head & self.mask) as usize);
            Some(std::ptr::read_volatile(slot))
        }
    }

    /// Get the current number of entries in the queue.
    #[inline]
    pub fn len(&self) -> u32 {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);

        if tail >= head {
            tail - head
        } else {
            self.capacity - (head - tail)
        }
    }

    /// Check if the queue is empty.
    #[allow(dead_code)]
    #[inline]
    pub fn is_empty(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head == tail
    }

    /// Check if the queue is full.
    #[allow(dead_code)]
    #[inline]
    pub fn is_full(&self) -> bool {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);
        let next_tail = (tail + 1) & self.mask;
        next_tail == head
    }

    /// Get the queue capacity.
    #[allow(dead_code)]
    #[inline]
    pub fn capacity(&self) -> u32 {
        self.capacity - 1 // One slot is reserved to distinguish full from empty
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_creation() {
        let queue = FifoQueue::new(16);
        assert_eq!(queue.capacity(), 15); // One slot reserved
        assert!(queue.is_empty());
        assert!(!queue.is_full());
    }

    #[test]
    fn test_push_pop() {
        let queue = FifoQueue::new(8);

        let entry = QueueEntry::new(42, 0x1234_5678_9ABC_DEF0);
        assert!(queue.push(entry));

        let popped = queue.pop().unwrap();
        assert_eq!(popped.bucket_index, 42);
        assert_eq!(popped.item_info, 0x1234_5678_9ABC_DEF0);
    }

    #[test]
    fn test_queue_full() {
        let queue = FifoQueue::new(4); // Capacity is 4, but usable is 3

        assert!(queue.push(QueueEntry::new(1, 1)));
        assert!(queue.push(QueueEntry::new(2, 2)));
        assert!(queue.push(QueueEntry::new(3, 3)));
        assert!(!queue.push(QueueEntry::new(4, 4))); // Should fail - full

        assert!(queue.is_full());
    }

    #[test]
    fn test_queue_wrap_around() {
        let queue = FifoQueue::new(4);

        // Fill and empty multiple times
        for round in 0u64..3 {
            for i in 0u64..3 {
                assert!(queue.push(QueueEntry::new(round * 10 + i, i)));
            }
            for i in 0u64..3 {
                let entry = queue.pop().unwrap();
                assert_eq!(entry.bucket_index, round * 10 + i);
            }
            assert!(queue.is_empty());
        }
    }

    #[test]
    fn test_entry_accessors() {
        // item_info layout: [TAG:12][FREQ:8][LOCATION:44]
        // Tag = 0xABC (bits 52-63)
        // Freq = 0x5F (bits 44-51)
        // Location = 0x123_4567_89AB (bits 0-43)
        let item_info = (0xABC_u64 << 52) | (0x5F_u64 << 44) | 0x123_4567_89AB_u64;
        let entry = QueueEntry::new(99, item_info);

        assert_eq!(entry.tag(), 0xABC);
        assert_eq!(entry.frequency(), 0x5F);
        assert_eq!(entry.location(), 0x123_4567_89AB);
    }

    #[test]
    fn test_peek() {
        let queue = FifoQueue::new(8);

        assert!(queue.peek().is_none());

        queue.push(QueueEntry::new(1, 100));
        queue.push(QueueEntry::new(2, 200));

        let peeked = queue.peek().unwrap();
        assert_eq!(peeked.bucket_index, 1);

        // Peek doesn't remove
        let peeked2 = queue.peek().unwrap();
        assert_eq!(peeked2.bucket_index, 1);

        // Pop removes
        let popped = queue.pop().unwrap();
        assert_eq!(popped.bucket_index, 1);

        // Now peek returns second item
        let peeked3 = queue.peek().unwrap();
        assert_eq!(peeked3.bucket_index, 2);
    }

    #[test]
    fn test_len() {
        let queue = FifoQueue::new(8);

        assert_eq!(queue.len(), 0);

        queue.push(QueueEntry::new(1, 1));
        assert_eq!(queue.len(), 1);

        queue.push(QueueEntry::new(2, 2));
        queue.push(QueueEntry::new(3, 3));
        assert_eq!(queue.len(), 3);

        queue.pop();
        assert_eq!(queue.len(), 2);
    }
}
