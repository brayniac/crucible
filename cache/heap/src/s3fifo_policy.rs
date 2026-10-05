//! S3-FIFO eviction policy for HeapCache.
//!
//! The policy follows S3-FIFO (Yang et al., "FIFO queues are all you need for
//! cache eviction", SOSP 2023) and uses two FIFO queues:
//! - Small queue: admission filter, `small_percent` of the total capacity.
//!   New items enter here.
//! - Main queue: the rest of the capacity. Items read in the small queue
//!   move here.
//!
//! The small queue's oldest entry is processed when an eviction needs a
//! victim, and also when an insert finds the small queue at its target size:
//! - If frequency > threshold: move to the main queue, decay frequency
//! - Otherwise: unlink the item (or convert it to a ghost) and return its
//!   location for the caller to free
//!
//! So an item read no more than `demotion_threshold` times is evicted once
//! the small queue's target size of newer inserts follow it, whether or not
//! memory is short. The small queue is allocated at twice its target size, so
//! concurrent inserts can push past the target without failing.
//!
//! On eviction from the main queue:
//! - If frequency > 0: decay and reinsert at tail (CLOCK-like behavior)
//! - If frequency == 0: unlink the item (or convert it to a ghost) and return
//!   its location for the caller to free
//!
//! Decayed frequencies are capped at 3, the maximum of the paper's 2-bit
//! counter. The hashtable keeps counting reads past 3, and an overwrite
//! carries the count over to the new item.

use crate::fifo_queue::{FifoQueue, QueueEntry};
use cache_core::{Hashtable, Location};

/// Mask to compare entries while ignoring the frequency field.
/// item_info layout: [TAG:12][FREQ:8][LOCATION:44]
/// We want to match TAG and LOCATION, masking out FREQ (bits 44-51).
const COMPARE_MASK: u64 = !((0xFF_u64) << 44);

/// The location bits of an item_info.
const LOCATION_MASK: u64 = 0xFFF_FFFF_FFFF;

/// Decayed frequencies are capped at this value, as in the reference
/// S3-FIFO; the hashtable counts up to 127. An item at the cap that is not
/// read again is evicted on its `MAX_FREQUENCY + 1`th visit to the head of
/// the main queue.
const MAX_FREQUENCY: u8 = 3;

/// Attempts `record_insert` makes to queue a new entry.
const MAX_PUSH_ATTEMPTS: usize = 8;

/// Largest main-queue capacity; `FifoQueue` holds at most 2^30 entries.
const MAX_MAIN_CAPACITY: u64 = 1 << 30;

/// Largest small-queue target, half of `MAX_MAIN_CAPACITY` because the small
/// queue is allocated at twice its target.
const MAX_SMALL_TARGET: u64 = 1 << 29;

/// S3-FIFO eviction policy.
pub struct S3FifoPolicy {
    /// Small FIFO queue (admission filter), allocated at twice `small_target`.
    small: FifoQueue,
    /// Inserts trim the small queue to below this length before pushing.
    small_target: u32,
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
    ///
    /// The main queue is capped at 2^30 entries and the small-queue target
    /// at 2^29. Items past the cap in the main queue are evicted.
    pub fn new(total_capacity: u64, small_percent: u8, demotion_threshold: u8) -> Self {
        let small_percent = small_percent.clamp(1, 50) as u64;
        let small_target = (total_capacity * small_percent / 100).clamp(1, MAX_SMALL_TARGET);
        let main_capacity = total_capacity
            .saturating_sub(small_target)
            .clamp(1, MAX_MAIN_CAPACITY);

        Self {
            small: FifoQueue::new(2 * small_target as u32),
            small_target: small_target as u32,
            main: FifoQueue::new(main_capacity as u32),
            demotion_threshold,
        }
    }

    /// Record an item insertion in the small queue.
    ///
    /// This should be called after a successful hashtable insert. If the
    /// small queue holds `small_target` or more entries, its oldest entries
    /// are processed as for an eviction, up to `MAX_PUSH_ATTEMPTS` of them,
    /// until it holds fewer. If the queue is then full, the same processing
    /// continues for up to `MAX_PUSH_ATTEMPTS` more entries; if the new entry
    /// still does not fit, the new item is unlinked, because an item in no
    /// queue would never be evicted. Returns the locations of every item
    /// unlinked, for the caller to free.
    ///
    /// # Arguments
    /// - `hashtable`: The hashtable for looking up/modifying items
    /// - `bucket_index`: The hashtable bucket where the item was inserted
    /// - `item_info`: The packed item info (tag, freq, location)
    /// - `create_ghosts`: Whether to create ghost entries for evicted items
    pub fn record_insert<H: Hashtable>(
        &self,
        hashtable: &H,
        bucket_index: u64,
        item_info: u64,
        create_ghosts: bool,
    ) -> Vec<Location> {
        let entry = QueueEntry::new(bucket_index, item_info);
        let mut freed = Vec::new();
        for _ in 0..MAX_PUSH_ATTEMPTS {
            if self.small.len() < self.small_target
                || !self.take_small_head(hashtable, create_ghosts, &mut freed)
            {
                break;
            }
        }
        for _ in 0..MAX_PUSH_ATTEMPTS {
            if self.small.push(entry) {
                return freed;
            }
            self.take_small_head(hashtable, create_ghosts, &mut freed);
        }
        // Other inserters filled each slot these steps freed.
        let location = item_info & LOCATION_MASK;
        if Self::unlink(hashtable, bucket_index, location, create_ghosts) {
            freed.push(Location::new(location));
        }
        freed
    }

    /// Evict items from the cache using S3-FIFO policy.
    ///
    /// Returns the locations of the items unlinked, for the caller to free;
    /// empty if no eviction was possible. Usually one; a promotion that
    /// cannot enter the main queue unlinks the promoted item too.
    ///
    /// # Arguments
    /// - `hashtable`: The hashtable for looking up/modifying items
    /// - `create_ghosts`: Whether to create ghost entries for evicted items
    pub fn evict<H: Hashtable>(&self, hashtable: &H, create_ghosts: bool) -> Vec<Location> {
        let mut freed = Vec::new();
        // Small queue first: process entries in order until one item is
        // unlinked, bounded by the queue's length at the start, plus one.
        for _ in 0..=self.small.len() {
            if !self.take_small_head(hashtable, create_ghosts, &mut freed) || !freed.is_empty() {
                break;
            }
        }
        if freed.is_empty() {
            self.evict_from_main(hashtable, create_ghosts, &mut freed);
        }
        freed
    }

    /// Pop the small queue's oldest entry and apply S3-FIFO's small-queue
    /// decision to it, adding the locations of items unlinked to `freed`.
    /// Returns `false` if the queue was empty.
    fn take_small_head<H: Hashtable>(
        &self,
        hashtable: &H,
        create_ghosts: bool,
        freed: &mut Vec<Location>,
    ) -> bool {
        let Some(entry) = self.small.pop() else {
            return false;
        };

        // The entry is stale if its item was overwritten, deleted or evicted
        // since it was queued; it is dropped.
        let Some(current_info) = hashtable.get_info_at_bucket(entry.bucket_index, |info| {
            // Match tag and location (ignore frequency which may have changed)
            (info & COMPARE_MASK) == (entry.item_info & COMPARE_MASK)
        }) else {
            return true;
        };

        let current_freq = ((current_info >> 44) & 0xFF) as u8;
        let location = current_info & LOCATION_MASK;

        if current_freq > self.demotion_threshold {
            let decayed_freq = current_freq.min(MAX_FREQUENCY).saturating_sub(1);
            let new_info = (current_info & !((0xFF_u64) << 44)) | ((decayed_freq as u64) << 44);
            hashtable.set_frequency_at_bucket(entry.bucket_index, location, decayed_freq);

            let promoted_entry = QueueEntry::new(entry.bucket_index, new_info);
            if self.main.push(promoted_entry) {
                return true;
            }
            // Main queue full: make room by evicting from it.
            self.evict_from_main(hashtable, create_ghosts, freed);
            if self.main.push(promoted_entry) {
                return true;
            }
            // Other threads' pushes to the main queue took the free slot. An
            // item in no queue would never be evicted, so this one is
            // evicted now.
        }

        // Evict this item, if it is still ours to evict
        if Self::unlink(hashtable, entry.bucket_index, location, create_ghosts) {
            freed.push(Location::new(location));
        }
        true
    }

    /// Unlink an evicted item's entry. Returns `true` iff this call unlinked
    /// it; only then may the caller free the item. `false` means a concurrent
    /// overwrite, delete or eviction took the entry, and that thread frees
    /// the item.
    fn unlink<H: Hashtable>(
        hashtable: &H,
        bucket_index: u64,
        location: u64,
        create_ghosts: bool,
    ) -> bool {
        if create_ghosts {
            hashtable.convert_to_ghost_at_bucket(bucket_index, location)
        } else {
            hashtable.remove_at_bucket(bucket_index, location)
        }
    }

    /// Evict from the main queue: items with freq > 0 are decayed and
    /// reinserted (CLOCK-like); the first with freq == 0 is unlinked and its
    /// location added to `freed`.
    fn evict_from_main<H: Hashtable>(
        &self,
        hashtable: &H,
        create_ghosts: bool,
        freed: &mut Vec<Location>,
    ) {
        // An entry not read again is evicted on its `MAX_FREQUENCY + 1`th
        // visit; this bound gives each entry present at the start that many
        // visits. Reads by other threads between visits raise frequencies
        // again, so the loop can end without unlinking anything.
        let passes = (MAX_FREQUENCY as usize + 1) * (self.main.len() as usize + 1);

        for _ in 0..passes {
            let Some(entry) = self.main.pop() else {
                return;
            };

            // A stale entry, whose item was overwritten, deleted or evicted
            // since it was queued, is dropped.
            let Some(current_info) = hashtable.get_info_at_bucket(entry.bucket_index, |info| {
                (info & COMPARE_MASK) == (entry.item_info & COMPARE_MASK)
            }) else {
                continue;
            };

            let current_freq = ((current_info >> 44) & 0xFF) as u8;
            let location = current_info & LOCATION_MASK;

            if current_freq > 0 {
                // Decay and reinsert at tail (second chance)
                let decayed_freq = current_freq.min(MAX_FREQUENCY).saturating_sub(1);
                hashtable.set_frequency_at_bucket(entry.bucket_index, location, decayed_freq);

                let new_info = (current_info & !((0xFF_u64) << 44)) | ((decayed_freq as u64) << 44);
                if self
                    .main
                    .push(QueueEntry::new(entry.bucket_index, new_info))
                {
                    continue;
                }
                // Queue full: evict this item instead.
            }

            // Evict this item, if it is still ours to evict
            if Self::unlink(hashtable, entry.bucket_index, location, create_ghosts) {
                freed.push(Location::new(location));
                return;
            }
        }
    }

    /// Drop all queued entries, returning the policy to its initial state.
    ///
    /// Every popped entry stops being tracked, so call it only when the items
    /// the entries name are about to be freed: `HeapCache::flush` calls it
    /// before draining the hashtable, and `HeapCache::reset` with no operation
    /// in flight. Left in place after the items are freed, every entry would
    /// be stale, and eviction would drop them one at a time.
    pub fn reset(&self) {
        self.small.clear();
        self.main.clear();
    }

    /// Get the number of items in the small queue.
    #[cfg(test)]
    pub fn small_queue_len(&self) -> u32 {
        self.small.len()
    }

    /// Get the number of items in the main queue.
    #[cfg(test)]
    pub fn main_queue_len(&self) -> u32 {
        self.main.len()
    }

    /// Get the total number of items tracked.
    #[cfg(all(test, not(feature = "loom")))]
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
    #[cfg(not(feature = "loom"))]
    fn test_record_insert() {
        let policy = S3FifoPolicy::new(100, 10, 1);
        let hashtable = cache_core::MultiChoiceHashtable::new(4);

        // Record some insertions; the small queue has room, so none evicts.
        assert!(
            policy
                .record_insert(&hashtable, 0, 0x1234_5678_9ABC_DEF0, false)
                .is_empty()
        );
        assert!(
            policy
                .record_insert(&hashtable, 1, 0xFEDC_BA98_7654_3210, false)
                .is_empty()
        );

        assert_eq!(policy.small_queue_len(), 2);
        assert_eq!(policy.main_queue_len(), 0);
    }

    #[test]
    #[cfg(not(feature = "loom"))]
    fn record_insert_trims_small_queue_to_target() {
        let policy = S3FifoPolicy::new(100, 10, 1);
        let hashtable = cache_core::MultiChoiceHashtable::new(4);

        for i in 0..50 {
            policy.record_insert(&hashtable, i, i, false);
        }
        assert_eq!(policy.small_queue_len(), 10);
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
