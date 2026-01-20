//! S3-FIFO eviction policy for HeapCache.
//!
//! S3-FIFO uses two FIFO queues:
//! - **Small queue**: Admission filter (~10% capacity). New items enter here.
//! - **Main queue**: Long-term storage (~90% capacity). Hot items promoted here.
//!
//! On eviction from the small queue:
//! - If frequency > threshold: promote to main queue, decay frequency
//! - If frequency <= threshold: create ghost, free the slot
//!
//! On eviction from the main queue:
//! - If frequency > 0: decay and reinsert at tail (CLOCK-like behavior)
//! - If frequency == 0: create ghost, free the slot

use crate::fifo_queue::{FifoQueue, QueueEntry};
use cache_core::{Hashtable, Location};

/// Mask to compare entries while ignoring the frequency field.
/// item_info layout: [TAG:12][FREQ:8][LOCATION:44]
/// We want to match TAG and LOCATION, masking out FREQ (bits 44-51).
const COMPARE_MASK: u64 = !((0xFF_u64) << 44);

/// S3-FIFO eviction policy.
pub struct S3FifoPolicy {
    /// Small FIFO queue (admission filter).
    small: FifoQueue,
    /// Main FIFO queue (long-term storage).
    main: FifoQueue,
    /// Frequency threshold for promotion from small to main.
    demotion_threshold: u8,
}

impl S3FifoPolicy {
    /// Create a new S3-FIFO policy.
    ///
    /// # Arguments
    /// - `total_capacity`: Total number of items the cache can hold
    /// - `small_percent`: Percentage of capacity for small queue (1-50, typically 10)
    /// - `demotion_threshold`: Frequency threshold for promotion (typically 1)
    pub fn new(total_capacity: u32, small_percent: u8, demotion_threshold: u8) -> Self {
        let small_percent = small_percent.clamp(1, 50) as u32;
        let small_capacity = (total_capacity * small_percent / 100).max(1);
        let main_capacity = total_capacity.saturating_sub(small_capacity).max(1);

        Self {
            small: FifoQueue::new(small_capacity),
            main: FifoQueue::new(main_capacity),
            demotion_threshold,
        }
    }

    /// Record an item insertion in the small queue.
    ///
    /// This should be called after a successful hashtable insert.
    ///
    /// # Arguments
    /// - `bucket_index`: The hashtable bucket where the item was inserted
    /// - `item_info`: The packed item info (tag, freq, location)
    pub fn record_insert(&self, bucket_index: u64, item_info: u64) {
        let entry = QueueEntry::new(bucket_index, item_info);

        // Try to push to small queue
        if !self.small.push(entry) {
            // Small queue full - this shouldn't happen if we evict properly
            // but we handle it gracefully by dropping the oldest entry
            let _ = self.small.pop();
            let _ = self.small.push(entry);
        }
    }

    /// Evict an item from the cache using S3-FIFO policy.
    ///
    /// Returns the location of the evicted item to be freed, or None if no
    /// eviction was possible.
    ///
    /// # Arguments
    /// - `hashtable`: The hashtable for looking up/modifying items
    /// - `create_ghosts`: Whether to create ghost entries for evicted items
    pub fn evict<H: Hashtable>(&self, hashtable: &H, create_ghosts: bool) -> Option<Location> {
        // First try to evict from small queue
        if let Some(result) = self.evict_from_small(hashtable, create_ghosts) {
            return Some(result);
        }

        // Fall back to main queue
        self.evict_from_main(hashtable, create_ghosts)
    }

    /// Evict from the small queue.
    ///
    /// Items with freq > threshold are promoted to main queue.
    /// Items with freq <= threshold are evicted (ghosted).
    fn evict_from_small<H: Hashtable>(
        &self,
        hashtable: &H,
        create_ghosts: bool,
    ) -> Option<Location> {
        // Process up to N entries looking for something to evict
        const MAX_ITERATIONS: usize = 100;

        for _ in 0..MAX_ITERATIONS {
            let entry = self.small.pop()?;

            // Verify the entry is still valid in the hashtable
            let current_info = hashtable.get_info_at_bucket(entry.bucket_index, |info| {
                // Match tag and location (ignore frequency which may have changed)
                (info & COMPARE_MASK) == (entry.item_info & COMPARE_MASK)
            })?;

            let current_freq = ((current_info >> 44) & 0xFF) as u8;
            let location = current_info & 0xFFF_FFFF_FFFF;

            if current_freq > self.demotion_threshold {
                // Promote to main queue
                let decayed_freq = current_freq.saturating_sub(1);
                let new_info = (current_info & !((0xFF_u64) << 44)) | ((decayed_freq as u64) << 44);

                // Decay frequency in hashtable
                hashtable.set_frequency_at_bucket(entry.bucket_index, location, decayed_freq);

                // Add to main queue
                let promoted_entry = QueueEntry::new(entry.bucket_index, new_info);
                if !self.main.push(promoted_entry) {
                    // Main queue full, evict from main first
                    if let Some(evicted) = self.evict_from_main(hashtable, create_ghosts) {
                        // Now try to push again
                        let _ = self.main.push(promoted_entry);
                        return Some(evicted);
                    }
                }
                // Continue processing small queue
                continue;
            }

            // Evict this item
            if create_ghosts {
                hashtable.convert_to_ghost_at_bucket(entry.bucket_index, location);
            } else {
                hashtable.remove_at_bucket(entry.bucket_index, location);
            }

            return Some(Location::new(location));
        }

        None
    }

    /// Evict from the main queue.
    ///
    /// Items with freq > 0 are decayed and reinserted (CLOCK-like).
    /// Items with freq == 0 are evicted.
    fn evict_from_main<H: Hashtable>(
        &self,
        hashtable: &H,
        create_ghosts: bool,
    ) -> Option<Location> {
        const MAX_ITERATIONS: usize = 100;

        for _ in 0..MAX_ITERATIONS {
            let entry = self.main.pop()?;

            // Verify the entry is still valid
            let current_info = hashtable.get_info_at_bucket(entry.bucket_index, |info| {
                (info & COMPARE_MASK) == (entry.item_info & COMPARE_MASK)
            })?;

            let current_freq = ((current_info >> 44) & 0xFF) as u8;
            let location = current_info & 0xFFF_FFFF_FFFF;

            if current_freq > 0 {
                // Decay and reinsert at tail (second chance)
                let decayed_freq = current_freq.saturating_sub(1);
                hashtable.set_frequency_at_bucket(entry.bucket_index, location, decayed_freq);

                let new_info = (current_info & !((0xFF_u64) << 44)) | ((decayed_freq as u64) << 44);
                let reinsert_entry = QueueEntry::new(entry.bucket_index, new_info);

                if !self.main.push(reinsert_entry) {
                    // Queue full, evict this item
                    if create_ghosts {
                        hashtable.convert_to_ghost_at_bucket(entry.bucket_index, location);
                    } else {
                        hashtable.remove_at_bucket(entry.bucket_index, location);
                    }
                    return Some(Location::new(location));
                }
                continue;
            }

            // Evict this item (freq == 0)
            if create_ghosts {
                hashtable.convert_to_ghost_at_bucket(entry.bucket_index, location);
            } else {
                hashtable.remove_at_bucket(entry.bucket_index, location);
            }

            return Some(Location::new(location));
        }

        None
    }

    /// Get the number of items in the small queue.
    pub fn small_queue_len(&self) -> u32 {
        self.small.len()
    }

    /// Get the number of items in the main queue.
    pub fn main_queue_len(&self) -> u32 {
        self.main.len()
    }

    /// Get the total number of items tracked.
    pub fn total_tracked(&self) -> u32 {
        self.small.len() + self.main.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_creation() {
        let policy = S3FifoPolicy::new(1000, 10, 1);

        assert_eq!(policy.small_queue_len(), 0);
        assert_eq!(policy.main_queue_len(), 0);
        assert_eq!(policy.demotion_threshold, 1);
    }

    #[test]
    fn test_record_insert() {
        let policy = S3FifoPolicy::new(100, 10, 1);

        // Record some insertions
        policy.record_insert(0, 0x1234_5678_9ABC_DEF0);
        policy.record_insert(1, 0xFEDC_BA98_7654_3210);

        assert_eq!(policy.small_queue_len(), 2);
        assert_eq!(policy.main_queue_len(), 0);
    }

    #[test]
    fn test_compare_mask() {
        // item_info layout: [TAG:12][FREQ:8][LOCATION:44]
        let info1 = (0xABC_u64 << 52) | (0x10_u64 << 44) | 0x123_4567_89AB_u64;
        let info2 = (0xABC_u64 << 52) | (0x20_u64 << 44) | 0x123_4567_89AB_u64;

        // Same tag and location, different frequency
        assert_ne!(info1, info2);
        assert_eq!(info1 & COMPARE_MASK, info2 & COMPARE_MASK);

        // Different location
        let info3 = (0xABC_u64 << 52) | (0x10_u64 << 44) | 0x123_4567_0000_u64;
        assert_ne!(info1 & COMPARE_MASK, info3 & COMPARE_MASK);
    }
}
