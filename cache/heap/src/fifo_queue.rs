//! Ring buffer FIFO queue for S3-FIFO eviction policy.
//!
//! This module provides a multi-producer multi-consumer (MPMC) FIFO queue
//! implemented as a bounded ring buffer using the Vyukov MPMC algorithm.
//! Per-slot sequence counters ensure entries are fully written before being
//! read, and CAS loops on head/tail prevent data races under concurrent
//! push/pop from multiple worker threads.

use std::cell::UnsafeCell;
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

/// Per-slot state for the Vyukov MPMC queue.
///
/// The sequence counter coordinates producers and consumers:
/// - When `sequence == tail`: the slot is ready for a producer to write
/// - When `sequence == head + 1`: the slot has been written and is ready to read
/// - After reading, sequence is advanced to `head + capacity` for the next cycle
struct Slot {
    sequence: AtomicU32,
    entry: UnsafeCell<QueueEntry>,
}

// SAFETY: The sequence counter ensures entries are only accessed by one thread
// at a time — producers write after claiming via CAS, consumers read after
// verifying the sequence, and the Release/Acquire ordering ensures visibility.
unsafe impl Send for Slot {}
unsafe impl Sync for Slot {}

/// MPMC ring buffer FIFO queue (Vyukov bounded queue).
///
/// Used by S3-FIFO eviction policy to track items in the small and main queues.
/// Multiple worker threads may push (during insert) and pop (during eviction)
/// concurrently.
pub struct FifoQueue {
    /// Per-slot storage with sequence counters.
    slots: Box<[Slot]>,
    /// Head index (monotonically increasing, masked for array access).
    head: AtomicU32,
    /// Tail index (monotonically increasing, masked for array access).
    tail: AtomicU32,
    /// Queue capacity (power of 2).
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

        let slots: Vec<Slot> = (0..capacity)
            .map(|i| Slot {
                sequence: AtomicU32::new(i),
                entry: UnsafeCell::new(QueueEntry::default()),
            })
            .collect();

        Self {
            slots: slots.into_boxed_slice(),
            head: AtomicU32::new(0),
            tail: AtomicU32::new(0),
            capacity,
            mask,
        }
    }

    /// Push an entry to the tail of the queue.
    ///
    /// Returns `true` if successful, `false` if the queue is full.
    ///
    /// # Thread Safety
    ///
    /// Uses CAS on the tail index and per-slot sequence counters to ensure
    /// only one thread writes to each slot. The sequence store with Release
    /// ordering ensures the entry is visible to consumers.
    #[inline]
    pub fn push(&self, entry: QueueEntry) -> bool {
        let mut tail = self.tail.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[(tail & self.mask) as usize];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(tail) as i32;

            if diff == 0 {
                // Slot is ready for writing — try to claim it
                match self.tail.compare_exchange_weak(
                    tail,
                    tail.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: We won the CAS, so we have exclusive write
                        // access to this slot until we advance its sequence.
                        unsafe {
                            slot.entry.get().write(entry);
                        }
                        // Signal that the entry is ready for consumers
                        slot.sequence.store(tail.wrapping_add(1), Ordering::Release);
                        return true;
                    }
                    Err(new_tail) => tail = new_tail,
                }
            } else if diff < 0 {
                // Queue is full
                return false;
            } else {
                // Another producer is writing to this slot, reload tail
                tail = self.tail.load(Ordering::Relaxed);
            }
        }
    }

    /// Pop an entry from the head of the queue.
    ///
    /// Returns `None` if the queue is empty.
    ///
    /// # Thread Safety
    ///
    /// Uses CAS on the head index and per-slot sequence counters to ensure
    /// only one thread reads from each slot. The sequence load with Acquire
    /// ordering ensures the entry written by the producer is visible.
    #[inline]
    pub fn pop(&self) -> Option<QueueEntry> {
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[(head & self.mask) as usize];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(head.wrapping_add(1)) as i32;

            if diff == 0 {
                // Entry is ready — try to claim it
                match self.head.compare_exchange_weak(
                    head,
                    head.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: We won the CAS, so we have exclusive read
                        // access to this slot until we advance its sequence.
                        let entry = unsafe { slot.entry.get().read() };
                        // Mark slot as available for the next producer cycle
                        slot.sequence
                            .store(head.wrapping_add(self.capacity), Ordering::Release);
                        return Some(entry);
                    }
                    Err(new_head) => head = new_head,
                }
            } else if diff < 0 {
                // Queue is empty
                return None;
            } else {
                // Another consumer is reading from this slot, reload head
                head = self.head.load(Ordering::Relaxed);
            }
        }
    }

    /// Peek at the head entry without removing it.
    ///
    /// Note: under concurrent access, the peeked entry may be popped by
    /// another thread before the caller acts on it.
    #[allow(dead_code)]
    #[inline]
    pub fn peek(&self) -> Option<QueueEntry> {
        let head = self.head.load(Ordering::Relaxed);
        let slot = &self.slots[(head & self.mask) as usize];
        let seq = slot.sequence.load(Ordering::Acquire);

        if seq.wrapping_sub(head.wrapping_add(1)) as i32 == 0 {
            // SAFETY: The entry has been written (sequence check passed).
            // We don't advance head, so another thread may also read this.
            Some(unsafe { slot.entry.get().read() })
        } else {
            None
        }
    }

    /// Get the approximate number of entries in the queue.
    ///
    /// Under concurrent access, this is a snapshot and may be stale.
    #[inline]
    pub fn len(&self) -> u32 {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);
        tail.wrapping_sub(head)
    }

    /// Check if the queue is approximately empty.
    #[allow(dead_code)]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if the queue is approximately full.
    #[allow(dead_code)]
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity
    }

    /// Get the queue capacity.
    #[allow(dead_code)]
    #[inline]
    pub fn capacity(&self) -> u32 {
        self.capacity
    }
}

// SAFETY: FifoQueue is Send + Sync because all shared access is coordinated
// through atomic operations and per-slot sequence counters.
unsafe impl Send for FifoQueue {}
unsafe impl Sync for FifoQueue {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_creation() {
        let queue = FifoQueue::new(16);
        assert_eq!(queue.capacity(), 16);
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
        let queue = FifoQueue::new(4);

        assert!(queue.push(QueueEntry::new(1, 1)));
        assert!(queue.push(QueueEntry::new(2, 2)));
        assert!(queue.push(QueueEntry::new(3, 3)));
        assert!(queue.push(QueueEntry::new(4, 4)));
        assert!(!queue.push(QueueEntry::new(5, 5))); // Should fail - full

        assert!(queue.is_full());
    }

    #[test]
    fn test_queue_wrap_around() {
        let queue = FifoQueue::new(4);

        // Fill and empty multiple times
        for round in 0u64..3 {
            for i in 0u64..4 {
                assert!(queue.push(QueueEntry::new(round * 10 + i, i)));
            }
            for i in 0u64..4 {
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

    #[test]
    fn test_concurrent_push_pop() {
        use std::sync::Arc;
        use std::thread;

        let queue = Arc::new(FifoQueue::new(1024));
        let items_per_thread = 10_000;
        let num_threads = 4;

        // Spawn producer threads
        let mut handles = Vec::new();
        for t in 0..num_threads {
            let q = queue.clone();
            handles.push(thread::spawn(move || {
                let mut pushed = 0u64;
                for i in 0..items_per_thread {
                    let entry = QueueEntry::new(t * items_per_thread + i, i);
                    while !q.push(entry) {
                        std::thread::yield_now();
                    }
                    pushed += 1;
                }
                pushed
            }));
        }

        // Spawn consumer threads
        let total_items = (num_threads * items_per_thread) as u64;
        let consumed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        for _ in 0..num_threads {
            let q = queue.clone();
            let consumed = consumed.clone();
            handles.push(thread::spawn(move || {
                let mut count = 0u64;
                loop {
                    if let Some(_entry) = q.pop() {
                        count += 1;
                        consumed.fetch_add(1, Ordering::Relaxed);
                    } else if consumed.load(Ordering::Relaxed) >= total_items {
                        break;
                    } else {
                        std::thread::yield_now();
                    }
                }
                count
            }));
        }

        for handle in handles {
            let _ = handle.join().unwrap();
        }

        assert_eq!(consumed.load(Ordering::Relaxed), total_items);
        assert!(queue.is_empty());
    }
}
