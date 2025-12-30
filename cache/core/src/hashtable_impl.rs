//! Lock-free cuckoo hashtable implementation.
//!
//! This hashtable supports:
//! - Configurable N-choice hashing (1-8 choices) for tunable load factors
//! - ASFC (Adaptive Software Frequency Counter) for frequency tracking
//! - Ghost entries for preserving frequency after eviction
//! - Multi-pool storage with pool_id tracking

use crate::error::{CacheError, CacheResult};
use crate::hashtable::{Hashtable, SegmentProvider};
use crate::location::ItemLocation;
use crate::segment::SegmentKeyVerify;
use crate::sync::{AtomicU64, Ordering};
use ahash::RandomState;

/// Maximum number of bucket choices supported.
pub const MAX_CHOICES: u8 = 8;

/// Lock-free hashtable for segment-based caches.
///
/// Each entry stores:
/// - 12-bit tag (hash suffix for fast filtering)
/// - 8-bit frequency counter (ASFC algorithm)
/// - 2-bit pool_id (supports up to 4 pools)
/// - 22-bit segment_id (up to 4M segments per pool)
/// - 20-bit offset/8 (up to ~8MB per segment)
pub struct CuckooHashtable {
    hash_builder: Box<RandomState>,
    buckets: Box<[Hashbucket]>,
    num_buckets: usize,
    mask: u64,
    /// Number of bucket choices (1-8). Higher values increase max load factor
    /// but add probe overhead. Recommended: 2-3 for most workloads.
    num_choices: u8,
}

impl CuckooHashtable {
    /// Create a new hashtable with two-choice hashing (default).
    ///
    /// # Parameters
    /// - `power`: Number of buckets = 2^power (minimum 4)
    pub fn new(power: u8) -> Self {
        Self::with_choices(power, 2)
    }

    /// Create a new hashtable with configurable N-choice hashing.
    ///
    /// # Parameters
    /// - `power`: Number of buckets = 2^power (minimum 4)
    /// - `num_choices`: Number of bucket choices (1-8)
    ///   - 1: Single-choice, fastest probes, ~70% max load
    ///   - 2: Two-choice, good balance, ~80% max load
    ///   - 3: Three-choice, recommended, ~85% max load with <1% failures
    ///   - 4+: Diminishing returns, higher probe cost
    pub fn with_choices(power: u8, num_choices: u8) -> Self {
        assert!(power >= 4, "power must be at least 4");
        assert!(
            (1..=MAX_CHOICES).contains(&num_choices),
            "num_choices must be 1-{}",
            MAX_CHOICES
        );

        // Use fixed seeds in tests for deterministic behavior
        #[cfg(test)]
        let hash_builder = RandomState::with_seeds(
            0xbb8c484891ec6c86,
            0x0522a25ae9c769f9,
            0xeed2797b9571bc75,
            0x4feb29c1fbbd59d0,
        );
        #[cfg(not(test))]
        let hash_builder = RandomState::new();

        let num_buckets = 1_usize << power;
        let mask = (num_buckets as u64) - 1;

        // Allocate buckets (zeroed)
        let buckets = (0..num_buckets)
            .map(|_| Hashbucket::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            hash_builder: Box::new(hash_builder),
            buckets,
            num_buckets,
            mask,
            num_choices,
        }
    }

    /// Get the number of bucket choices.
    #[inline]
    pub fn num_choices(&self) -> u8 {
        self.num_choices
    }

    /// Get the number of buckets.
    #[inline]
    pub fn num_buckets(&self) -> usize {
        self.num_buckets
    }

    /// Get a reference to a bucket by index.
    #[inline]
    fn bucket(&self, index: usize) -> &Hashbucket {
        debug_assert!(index < self.num_buckets);
        &self.buckets[index]
    }

    /// Compute hash for a key.
    #[inline]
    fn hash_key(&self, key: &[u8]) -> u64 {
        self.hash_builder.hash_one(key)
    }

    /// Compute bucket indices for N-choice hashing.
    ///
    /// Returns an array of up to 8 bucket indices. Use `num_choices()` to know
    /// how many are valid. Each index is derived from a different mixing of
    /// the hash bits to ensure good distribution.
    #[inline]
    fn bucket_indices(&self, hash: u64) -> [usize; MAX_CHOICES as usize] {
        let mask = self.mask;
        [
            // Primary: low bits
            (hash & mask) as usize,
            // Secondary: high bits XOR'd with low
            ((hash ^ (hash >> 32)) & mask) as usize,
            // Tertiary: middle bits mixed
            (((hash >> 16) ^ (hash << 16)) & mask) as usize,
            // Quaternary: different mixing
            (((hash >> 48) ^ (hash >> 8) ^ hash) & mask) as usize,
            // 5th: rotate and mix
            ((hash.rotate_left(17) ^ hash) & mask) as usize,
            // 6th: another rotation
            ((hash.rotate_left(31) ^ (hash >> 16)) & mask) as usize,
            // 7th: multiply-shift style
            ((hash.wrapping_mul(0x9E3779B97F4A7C15) >> 32) & mask) as usize,
            // 8th: different multiplier
            ((hash.wrapping_mul(0x517CC1B727220A95) >> 32) & mask) as usize,
        ]
    }

    /// Extract tag from hash.
    #[inline]
    fn tag_from_hash(hash: u64) -> u16 {
        ((hash >> 32) & 0xFFF) as u16
    }

    /// Count occupied (non-empty, non-ghost) slots in a bucket.
    #[inline]
    fn count_occupied(&self, bucket_index: usize) -> usize {
        let bucket = self.bucket(bucket_index);
        let mut count = 0;
        for slot in &bucket.items {
            let packed = slot.load(Ordering::Relaxed);
            if packed != 0 && !Hashbucket::is_ghost(packed) {
                count += 1;
            }
        }
        count
    }

    /// Search a bucket for an item, updating frequency on hit.
    fn search_bucket_for_get<S: SegmentKeyVerify>(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        segments: &impl SegmentProvider<S>,
    ) -> Option<(ItemLocation, u8)> {
        let bucket = self.bucket(bucket_index);

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if packed == 0 || Hashbucket::is_ghost(packed) {
                continue;
            }

            if Hashbucket::item_tag(packed) == tag {
                let pool_id = Hashbucket::item_pool_id(packed);
                let segment_id = Hashbucket::item_segment_id(packed);
                let offset = Hashbucket::item_offset(packed);

                // Verify key matches
                if let Some(segment) = segments.get_segment(pool_id, segment_id)
                    && segment.verify_key_at_offset(offset, key, false)
                {
                    // Update frequency (best effort)
                    let freq = Hashbucket::item_freq(packed);
                    if freq < 127
                        && let Some(new_packed) = Hashbucket::try_update_freq(packed, freq)
                    {
                        let _ = slot.compare_exchange(
                            packed,
                            new_packed,
                            Ordering::Release,
                            Ordering::Relaxed,
                        );
                    }

                    let location = ItemLocation::new(pool_id, segment_id, offset);
                    return Some((location, Hashbucket::item_freq(packed)));
                }
            }
        }

        None
    }

    /// Search a bucket for existence (no frequency update).
    fn search_bucket_exists<S: SegmentKeyVerify>(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        segments: &impl SegmentProvider<S>,
    ) -> bool {
        let bucket = self.bucket(bucket_index);

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if packed == 0 || Hashbucket::is_ghost(packed) {
                continue;
            }

            if Hashbucket::item_tag(packed) == tag {
                let pool_id = Hashbucket::item_pool_id(packed);
                let segment_id = Hashbucket::item_segment_id(packed);
                let offset = Hashbucket::item_offset(packed);

                if let Some(segment) = segments.get_segment(pool_id, segment_id)
                    && segment.verify_key_at_offset(offset, key, false)
                {
                    return true;
                }
            }
        }

        false
    }

    /// Search for a ghost entry's frequency.
    fn search_bucket_for_ghost(&self, bucket_index: usize, tag: u16) -> Option<u8> {
        let bucket = self.bucket(bucket_index);

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if Hashbucket::is_ghost(packed) && Hashbucket::item_tag(packed) == tag {
                return Some(Hashbucket::item_freq(packed));
            }
        }

        None
    }

    /// Search for frequency of a specific item.
    fn search_bucket_for_freq<S: SegmentKeyVerify>(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        segments: &impl SegmentProvider<S>,
    ) -> Option<u8> {
        let bucket = self.bucket(bucket_index);

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if packed == 0 || Hashbucket::is_ghost(packed) {
                continue;
            }

            if Hashbucket::item_tag(packed) == tag {
                let pool_id = Hashbucket::item_pool_id(packed);
                let segment_id = Hashbucket::item_segment_id(packed);
                let offset = Hashbucket::item_offset(packed);

                if let Some(segment) = segments.get_segment(pool_id, segment_id)
                    && segment.verify_key_at_offset(offset, key, false)
                {
                    return Some(Hashbucket::item_freq(packed));
                }
            }
        }

        None
    }

    /// Search for frequency by exact location.
    fn search_bucket_for_item_freq(
        &self,
        bucket_index: usize,
        tag: u16,
        location: ItemLocation,
    ) -> Option<u8> {
        let bucket = self.bucket(bucket_index);
        let pool_id = location.pool_id();
        let segment_id = location.segment_id();
        let offset = location.offset();

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if packed == 0 || Hashbucket::is_ghost(packed) {
                continue;
            }

            if Hashbucket::item_tag(packed) == tag
                && Hashbucket::item_pool_id(packed) == pool_id
                && Hashbucket::item_segment_id(packed) == segment_id
                && Hashbucket::item_offset(packed) == offset
            {
                return Some(Hashbucket::item_freq(packed));
            }
        }

        None
    }

    /// Try to link in a bucket, handling existing entries and ghosts.
    fn try_link_in_bucket<S: SegmentKeyVerify>(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        new_packed: u64,
        segments: &impl SegmentProvider<S>,
    ) -> Option<CacheResult<Option<ItemLocation>>> {
        let bucket = self.bucket(bucket_index);

        // First pass: look for existing entry or matching ghost
        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if Hashbucket::item_tag(packed) == tag {
                if Hashbucket::is_ghost(packed) {
                    // Replace ghost, preserving frequency
                    let freq = Hashbucket::item_freq(packed);
                    let new_with_freq = Hashbucket::update_freq_in_packed(new_packed, freq);

                    match slot.compare_exchange(
                        packed,
                        new_with_freq,
                        Ordering::Release,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return Some(Ok(None)),
                        Err(_) => continue,
                    }
                }

                let pool_id = Hashbucket::item_pool_id(packed);
                let segment_id = Hashbucket::item_segment_id(packed);
                let offset = Hashbucket::item_offset(packed);

                if let Some(segment) = segments.get_segment(pool_id, segment_id)
                    && segment.verify_key_at_offset(offset, key, true)
                {
                    // Replace existing entry, preserving frequency
                    let freq = Hashbucket::item_freq(packed);
                    let new_with_freq = Hashbucket::update_freq_in_packed(new_packed, freq);

                    match slot.compare_exchange(
                        packed,
                        new_with_freq,
                        Ordering::Release,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            let old_loc = ItemLocation::new(pool_id, segment_id, offset);
                            return Some(Ok(Some(old_loc)));
                        }
                        Err(_) => continue,
                    }
                }
            }
        }

        // Second pass: look for empty slot
        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if packed == 0 {
                match slot.compare_exchange(0, new_packed, Ordering::Release, Ordering::Acquire) {
                    Ok(_) => return Some(Ok(None)),
                    Err(_) => continue,
                }
            }
        }

        // Third pass: look for any ghost to evict
        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if Hashbucket::is_ghost(packed) {
                match slot.compare_exchange(
                    packed,
                    new_packed,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Some(Ok(None)),
                    Err(_) => continue,
                }
            }
        }

        None // Bucket full, try another
    }

    /// Try to unlink an item from a bucket.
    fn try_unlink_in_bucket(&self, bucket_index: usize, tag: u16, location: ItemLocation) -> bool {
        let bucket = self.bucket(bucket_index);
        let pool_id = location.pool_id();
        let segment_id = location.segment_id();
        let offset = location.offset();

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if packed == 0 || Hashbucket::is_ghost(packed) {
                continue;
            }

            if Hashbucket::item_tag(packed) == tag
                && Hashbucket::item_pool_id(packed) == pool_id
                && Hashbucket::item_segment_id(packed) == segment_id
                && Hashbucket::item_offset(packed) == offset
            {
                match slot.compare_exchange(packed, 0, Ordering::Release, Ordering::Acquire) {
                    Ok(_) => return true,
                    Err(_) => continue,
                }
            }
        }

        false
    }

    /// Try to convert an item to ghost in a bucket.
    fn try_to_ghost_in_bucket(
        &self,
        bucket_index: usize,
        tag: u16,
        location: ItemLocation,
    ) -> bool {
        let bucket = self.bucket(bucket_index);
        let pool_id = location.pool_id();
        let segment_id = location.segment_id();
        let offset = location.offset();

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if packed == 0 || Hashbucket::is_ghost(packed) {
                continue;
            }

            if Hashbucket::item_tag(packed) == tag
                && Hashbucket::item_pool_id(packed) == pool_id
                && Hashbucket::item_segment_id(packed) == segment_id
                && Hashbucket::item_offset(packed) == offset
            {
                let ghost = Hashbucket::to_ghost(packed);
                match slot.compare_exchange(packed, ghost, Ordering::Release, Ordering::Acquire) {
                    Ok(_) => return true,
                    Err(_) => continue,
                }
            }
        }

        false
    }

    /// Try to CAS update location in a bucket.
    fn try_cas_in_bucket(
        &self,
        bucket_index: usize,
        tag: u16,
        old_location: ItemLocation,
        new_location: ItemLocation,
        preserve_freq: bool,
    ) -> bool {
        let bucket = self.bucket(bucket_index);
        let old_pool_id = old_location.pool_id();
        let old_segment_id = old_location.segment_id();
        let old_offset = old_location.offset();

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if packed == 0 || Hashbucket::is_ghost(packed) {
                continue;
            }

            if Hashbucket::item_tag(packed) == tag
                && Hashbucket::item_pool_id(packed) == old_pool_id
                && Hashbucket::item_segment_id(packed) == old_segment_id
                && Hashbucket::item_offset(packed) == old_offset
            {
                let freq = if preserve_freq {
                    Hashbucket::item_freq(packed)
                } else {
                    1
                };
                let new_packed = Hashbucket::pack_item(
                    tag,
                    freq,
                    new_location.pool_id(),
                    new_location.segment_id(),
                    new_location.offset(),
                );

                if slot
                    .compare_exchange(packed, new_packed, Ordering::Release, Ordering::Acquire)
                    .is_ok()
                {
                    return true;
                }
            }
        }

        false
    }

    /// Check if key exists (for ADD semantics).
    fn check_key_exists<S: SegmentKeyVerify>(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        segments: &impl SegmentProvider<S>,
    ) -> bool {
        let bucket = self.bucket(bucket_index);

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);
            if packed == 0 || Hashbucket::is_ghost(packed) {
                continue;
            }

            if Hashbucket::item_tag(packed) == tag {
                let pool_id = Hashbucket::item_pool_id(packed);
                let seg_id = Hashbucket::item_segment_id(packed);
                let off = Hashbucket::item_offset(packed);

                if let Some(segment) = segments.get_segment(pool_id, seg_id)
                    && segment.verify_key_at_offset(off, key, false)
                {
                    return true;
                }
            }
        }

        false
    }

    /// Try to replace a matching ghost for ADD (inherit frequency).
    fn try_replace_ghost_for_add(
        &self,
        bucket_index: usize,
        tag: u16,
        location: ItemLocation,
    ) -> Option<CacheResult<()>> {
        let bucket = self.bucket(bucket_index);

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if Hashbucket::is_ghost(packed) && Hashbucket::item_tag(packed) == tag {
                let freq = Hashbucket::item_freq(packed);
                let new_packed = Hashbucket::pack_item(
                    tag,
                    freq,
                    location.pool_id(),
                    location.segment_id(),
                    location.offset(),
                );

                match slot.compare_exchange(
                    packed,
                    new_packed,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Some(Ok(())),
                    Err(_) => continue,
                }
            }
        }

        None
    }

    /// Try to insert into empty slot for ADD.
    fn try_insert_empty_for_add<S: SegmentKeyVerify>(
        &self,
        bucket_index: usize,
        tag: u16,
        new_packed: u64,
        key: &[u8],
        segments: &impl SegmentProvider<S>,
    ) -> Option<CacheResult<()>> {
        let bucket = self.bucket(bucket_index);

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if packed == 0 {
                match slot.compare_exchange(0, new_packed, Ordering::Release, Ordering::Acquire) {
                    Ok(_) => {
                        // Double-check for race condition
                        if self.check_for_duplicate_after_insert(
                            bucket_index,
                            slot,
                            tag,
                            key,
                            segments,
                        ) {
                            let _ = slot.compare_exchange(
                                new_packed,
                                0,
                                Ordering::Release,
                                Ordering::Relaxed,
                            );
                            return Some(Err(CacheError::KeyExists));
                        }
                        return Some(Ok(()));
                    }
                    Err(new_current) => {
                        // Check if someone else inserted our key
                        if new_current != 0
                            && !Hashbucket::is_ghost(new_current)
                            && Hashbucket::item_tag(new_current) == tag
                        {
                            let pool_id = Hashbucket::item_pool_id(new_current);
                            let seg_id = Hashbucket::item_segment_id(new_current);
                            let off = Hashbucket::item_offset(new_current);
                            if let Some(segment) = segments.get_segment(pool_id, seg_id)
                                && segment.verify_key_at_offset(off, key, false)
                            {
                                return Some(Err(CacheError::KeyExists));
                            }
                        }
                        continue;
                    }
                }
            }
        }

        None
    }

    /// Check for duplicate after inserting (race detection).
    fn check_for_duplicate_after_insert<S: SegmentKeyVerify>(
        &self,
        inserted_bucket: usize,
        inserted_slot: &AtomicU64,
        tag: u16,
        key: &[u8],
        segments: &impl SegmentProvider<S>,
    ) -> bool {
        let hash = self.hash_key(key);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            let bucket = self.bucket(bucket_index);

            for slot in &bucket.items {
                if bucket_index == inserted_bucket && std::ptr::eq(slot, inserted_slot) {
                    continue;
                }

                let packed = slot.load(Ordering::Acquire);
                if packed == 0 || Hashbucket::is_ghost(packed) {
                    continue;
                }

                if Hashbucket::item_tag(packed) == tag {
                    let pool_id = Hashbucket::item_pool_id(packed);
                    let seg_id = Hashbucket::item_segment_id(packed);
                    let off = Hashbucket::item_offset(packed);

                    if let Some(segment) = segments.get_segment(pool_id, seg_id)
                        && segment.verify_key_at_offset(off, key, false)
                    {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Try to evict any ghost to make space.
    fn try_evict_any_ghost(&self, bucket_index: usize, new_packed: u64) -> Option<CacheResult<()>> {
        let bucket = self.bucket(bucket_index);

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);

            if Hashbucket::is_ghost(packed) {
                match slot.compare_exchange(
                    packed,
                    new_packed,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Some(Ok(())),
                    Err(_) => continue,
                }
            }
        }

        None
    }

    /// Try to replace existing entry for REPLACE semantics.
    fn try_replace_existing_for_replace<S: SegmentKeyVerify>(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        new_location: ItemLocation,
        segments: &impl SegmentProvider<S>,
    ) -> Option<CacheResult<ItemLocation>> {
        let bucket = self.bucket(bucket_index);

        for slot in &bucket.items {
            let packed = slot.load(Ordering::Acquire);
            if packed == 0 || Hashbucket::is_ghost(packed) {
                continue;
            }

            if Hashbucket::item_tag(packed) == tag {
                let old_pool_id = Hashbucket::item_pool_id(packed);
                let old_seg_id = Hashbucket::item_segment_id(packed);
                let old_offset = Hashbucket::item_offset(packed);

                if let Some(segment) = segments.get_segment(old_pool_id, old_seg_id)
                    && segment.verify_key_at_offset(old_offset, key, false)
                {
                    let freq = Hashbucket::item_freq(packed);
                    let new_packed = Hashbucket::pack_item(
                        tag,
                        freq,
                        new_location.pool_id(),
                        new_location.segment_id(),
                        new_location.offset(),
                    );

                    match slot.compare_exchange(
                        packed,
                        new_packed,
                        Ordering::Release,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            let old_loc = ItemLocation::new(old_pool_id, old_seg_id, old_offset);
                            return Some(Ok(old_loc));
                        }
                        Err(_) => continue,
                    }
                }
            }
        }

        None
    }
}

impl Hashtable for CuckooHashtable {
    fn lookup<S: SegmentKeyVerify>(
        &self,
        key: &[u8],
        segments: &impl SegmentProvider<S>,
    ) -> Option<(ItemLocation, u8)> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if let Some(result) = self.search_bucket_for_get(bucket_index, tag, key, segments) {
                return Some(result);
            }
        }

        None
    }

    fn contains<S: SegmentKeyVerify>(
        &self,
        key: &[u8],
        segments: &impl SegmentProvider<S>,
    ) -> bool {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if self.search_bucket_exists(bucket_index, tag, key, segments) {
                return true;
            }
        }

        false
    }

    fn insert<S: SegmentKeyVerify>(
        &self,
        key: &[u8],
        location: ItemLocation,
        segments: &impl SegmentProvider<S>,
    ) -> CacheResult<Option<ItemLocation>> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);
        let choices = &buckets[..self.num_choices as usize];

        let new_packed = Hashbucket::pack_item(
            tag,
            1,
            location.pool_id(),
            location.segment_id(),
            location.offset(),
        );

        // First pass: try to find existing key or ghost in any bucket
        for &bucket_index in choices {
            if let Some(result) =
                self.try_link_in_bucket(bucket_index, tag, key, new_packed, segments)
            {
                return result;
            }
        }

        // Second pass: find least-full bucket and try to insert there
        if self.num_choices > 1 {
            // Find bucket with minimum occupancy
            let target = choices
                .iter()
                .copied()
                .min_by_key(|&b| self.count_occupied(b))
                .unwrap();

            if let Some(result) = self.try_link_in_bucket(target, tag, key, new_packed, segments) {
                return result;
            }

            // Try remaining buckets in order of occupancy
            let mut sorted: Vec<_> = choices.to_vec();
            sorted.sort_by_key(|&b| self.count_occupied(b));
            for bucket_index in sorted {
                if let Some(result) =
                    self.try_link_in_bucket(bucket_index, tag, key, new_packed, segments)
                {
                    return result;
                }
            }
        }

        Err(CacheError::HashTableFull)
    }

    fn insert_if_absent<S: SegmentKeyVerify>(
        &self,
        key: &[u8],
        location: ItemLocation,
        segments: &impl SegmentProvider<S>,
    ) -> CacheResult<()> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);
        let choices = &buckets[..self.num_choices as usize];

        // Phase 1: Check if key already exists in any bucket
        for &bucket_index in choices {
            if self.check_key_exists(bucket_index, tag, key, segments) {
                return Err(CacheError::KeyExists);
            }
        }

        // Phase 2: Try to replace matching ghost in any bucket
        for &bucket_index in choices {
            if let Some(result) = self.try_replace_ghost_for_add(bucket_index, tag, location) {
                return result;
            }
        }

        // Phase 3: Insert into empty slot, preferring least-full bucket
        let new_packed = Hashbucket::pack_item(
            tag,
            1,
            location.pool_id(),
            location.segment_id(),
            location.offset(),
        );

        // Sort buckets by occupancy (least-full first)
        let mut sorted: Vec<_> = choices.to_vec();
        sorted.sort_by_key(|&b| self.count_occupied(b));

        for bucket_index in &sorted {
            if let Some(result) =
                self.try_insert_empty_for_add(*bucket_index, tag, new_packed, key, segments)
            {
                return result;
            }
        }

        // Phase 4: Try evicting any ghost
        for bucket_index in &sorted {
            if let Some(result) = self.try_evict_any_ghost(*bucket_index, new_packed) {
                return result;
            }
        }

        Err(CacheError::HashTableFull)
    }

    fn update_if_present<S: SegmentKeyVerify>(
        &self,
        key: &[u8],
        location: ItemLocation,
        segments: &impl SegmentProvider<S>,
    ) -> CacheResult<ItemLocation> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if let Some(result) =
                self.try_replace_existing_for_replace(bucket_index, tag, key, location, segments)
            {
                return result;
            }
        }

        Err(CacheError::KeyNotFound)
    }

    fn remove(&self, key: &[u8], expected: ItemLocation) -> bool {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if self.try_unlink_in_bucket(bucket_index, tag, expected) {
                return true;
            }
        }

        false
    }

    fn convert_to_ghost(&self, key: &[u8], expected: ItemLocation) -> bool {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if self.try_to_ghost_in_bucket(bucket_index, tag, expected) {
                return true;
            }
        }

        false
    }

    fn cas_location(
        &self,
        key: &[u8],
        old_location: ItemLocation,
        new_location: ItemLocation,
        preserve_freq: bool,
    ) -> bool {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if self.try_cas_in_bucket(bucket_index, tag, old_location, new_location, preserve_freq)
            {
                return true;
            }
        }

        false
    }

    fn get_frequency<S: SegmentKeyVerify>(
        &self,
        key: &[u8],
        segments: &impl SegmentProvider<S>,
    ) -> Option<u8> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if let Some(freq) = self.search_bucket_for_freq(bucket_index, tag, key, segments) {
                return Some(freq);
            }
        }

        None
    }

    fn get_item_frequency(&self, key: &[u8], location: ItemLocation) -> Option<u8> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if let Some(freq) = self.search_bucket_for_item_freq(bucket_index, tag, location) {
                return Some(freq);
            }
        }

        None
    }

    fn get_ghost_frequency(&self, key: &[u8]) -> Option<u8> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if let Some(freq) = self.search_bucket_for_ghost(bucket_index, tag) {
                return Some(freq);
            }
        }

        None
    }
}

// ============================================================================
// Hashbucket - 64-byte cache-line aligned bucket
// ============================================================================

/// A single hashtable bucket (64 bytes, cache-line aligned).
///
/// Contains 8 item slots (no separate info slot - CAS handled differently).
#[repr(C, align(64))]
pub struct Hashbucket {
    /// 8 item slots, each packed as:
    /// `[12 bits tag][8 bits freq][2 bits pool_id][22 bits segment_id][20 bits offset/8]`
    pub(crate) items: [AtomicU64; 8],
}

const _: () = assert!(std::mem::size_of::<Hashbucket>() == 64);
const _: () = assert!(std::mem::align_of::<Hashbucket>() == 64);

impl Hashbucket {
    /// Sentinel segment ID for ghost entries.
    pub const GHOST_SEGMENT_ID: u32 = 0x3FFFFF;

    /// Create a new empty bucket.
    pub fn new() -> Self {
        Self {
            items: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Pack item data into a u64.
    ///
    /// Layout: `[12 bits tag][8 bits freq][2 bits pool_id][22 bits segment_id][20 bits offset/8]`
    #[inline]
    pub fn pack_item(tag: u16, freq: u8, pool_id: u8, segment_id: u32, offset: u32) -> u64 {
        debug_assert!(offset & 0x7 == 0, "offset must be 8-byte aligned");
        debug_assert!(pool_id <= 3, "pool_id exceeds 2-bit limit");
        debug_assert!(segment_id <= 0x3FFFFF, "segment_id exceeds 22-bit limit");

        let tag_64 = (tag as u64 & 0xFFF) << 52;
        let freq_64 = (freq as u64 & 0xFF) << 44;
        let pool_64 = (pool_id as u64 & 0x3) << 42;
        let seg_64 = (segment_id as u64 & 0x3FFFFF) << 20;
        let off_64 = (offset >> 3) as u64 & 0xFFFFF;

        tag_64 | freq_64 | pool_64 | seg_64 | off_64
    }

    /// Extract tag (12 bits).
    #[inline]
    pub fn item_tag(packed: u64) -> u16 {
        (packed >> 52) as u16
    }

    /// Extract frequency (8 bits).
    #[inline]
    pub fn item_freq(packed: u64) -> u8 {
        ((packed >> 44) & 0xFF) as u8
    }

    /// Extract pool ID (2 bits).
    #[inline]
    pub fn item_pool_id(packed: u64) -> u8 {
        ((packed >> 42) & 0x3) as u8
    }

    /// Extract segment ID (22 bits).
    #[inline]
    pub fn item_segment_id(packed: u64) -> u32 {
        ((packed >> 20) & 0x3FFFFF) as u32
    }

    /// Extract offset (20 bits * 8).
    #[inline]
    pub fn item_offset(packed: u64) -> u32 {
        ((packed & 0xFFFFF) << 3) as u32
    }

    /// Check if entry is a ghost.
    #[inline]
    pub fn is_ghost(packed: u64) -> bool {
        packed != 0 && Self::item_segment_id(packed) == Self::GHOST_SEGMENT_ID
    }

    /// Pack a ghost entry (tag + frequency only).
    #[inline]
    pub fn pack_ghost(tag: u16, freq: u8) -> u64 {
        let tag_64 = (tag as u64 & 0xFFF) << 52;
        let freq_64 = (freq as u64 & 0xFF) << 44;
        let seg_64 = (Self::GHOST_SEGMENT_ID as u64) << 20;
        tag_64 | freq_64 | seg_64
    }

    /// Convert a live entry to ghost.
    #[inline]
    pub fn to_ghost(packed: u64) -> u64 {
        let tag = Self::item_tag(packed);
        let freq = Self::item_freq(packed);
        Self::pack_ghost(tag, freq)
    }

    /// Update frequency in a packed value.
    #[inline]
    pub fn update_freq_in_packed(packed: u64, freq: u8) -> u64 {
        let freq_mask = 0xFF_u64 << 44;
        (packed & !freq_mask) | ((freq as u64) << 44)
    }

    /// Try to update frequency using ASFC algorithm.
    ///
    /// Returns `Some(new_packed)` if frequency should increment.
    #[inline]
    pub fn try_update_freq(packed: u64, freq: u8) -> Option<u64> {
        if freq >= 127 {
            return None;
        }

        // ASFC: probabilistic increment
        let should_increment = if freq <= 16 {
            true
        } else {
            #[cfg(not(feature = "loom"))]
            let rand = {
                use rand::Rng;
                rand::rng().random::<u64>()
            };
            #[cfg(feature = "loom")]
            let rand = 0u64;

            rand.is_multiple_of(freq as u64)
        };

        if should_increment {
            Some(Self::update_freq_in_packed(packed, freq + 1))
        } else {
            None
        }
    }
}

impl Default for Hashbucket {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    // Mock segment for testing
    struct MockSegment {
        keys: Vec<(u32, Vec<u8>, bool)>, // (offset, key, is_deleted)
    }

    impl SegmentKeyVerify for MockSegment {
        fn verify_key_at_offset(&self, offset: u32, key: &[u8], allow_deleted: bool) -> bool {
            self.keys
                .iter()
                .any(|(off, k, deleted)| *off == offset && k == key && (allow_deleted || !deleted))
        }
    }

    #[test]
    fn test_pack_item_basic() {
        let tag = 0xABC;
        let freq = 42;
        let pool_id = 2;
        let segment_id = 0x123456 & 0x3FFFFF;
        let offset = 0x100;

        let packed = Hashbucket::pack_item(tag, freq, pool_id, segment_id, offset);

        assert_eq!(Hashbucket::item_tag(packed), tag);
        assert_eq!(Hashbucket::item_freq(packed), freq);
        assert_eq!(Hashbucket::item_pool_id(packed), pool_id);
        assert_eq!(Hashbucket::item_segment_id(packed), segment_id);
        assert_eq!(Hashbucket::item_offset(packed), offset);
    }

    #[test]
    fn test_pack_item_max_values() {
        let tag = 0xFFF;
        let freq = 0xFF;
        let pool_id = 3;
        let segment_id = 0x3FFFFE;
        let offset = 0xFFFFF << 3;

        let packed = Hashbucket::pack_item(tag, freq, pool_id, segment_id, offset);

        assert_eq!(Hashbucket::item_tag(packed), tag);
        assert_eq!(Hashbucket::item_freq(packed), freq);
        assert_eq!(Hashbucket::item_pool_id(packed), pool_id);
        assert_eq!(Hashbucket::item_segment_id(packed), segment_id);
        assert_eq!(Hashbucket::item_offset(packed), offset);
    }

    #[test]
    fn test_ghost_entries() {
        let tag = 0x123;
        let freq = 50;

        let ghost = Hashbucket::pack_ghost(tag, freq);

        assert!(Hashbucket::is_ghost(ghost));
        assert_eq!(Hashbucket::item_tag(ghost), tag);
        assert_eq!(Hashbucket::item_freq(ghost), freq);
        assert_eq!(
            Hashbucket::item_segment_id(ghost),
            Hashbucket::GHOST_SEGMENT_ID
        );
    }

    #[test]
    fn test_to_ghost() {
        let packed = Hashbucket::pack_item(0x456, 75, 1, 1000, 2048);
        let ghost = Hashbucket::to_ghost(packed);

        assert!(Hashbucket::is_ghost(ghost));
        assert_eq!(Hashbucket::item_tag(ghost), 0x456);
        assert_eq!(Hashbucket::item_freq(ghost), 75);
    }

    #[test]
    fn test_hashtable_creation() {
        // Default is 2-choice
        let ht = CuckooHashtable::new(10);
        assert_eq!(ht.num_buckets(), 1024);
        assert_eq!(ht.num_choices(), 2);

        // Test various choice counts
        let ht1 = CuckooHashtable::with_choices(10, 1);
        assert_eq!(ht1.num_choices(), 1);

        let ht3 = CuckooHashtable::with_choices(10, 3);
        assert_eq!(ht3.num_choices(), 3);

        let ht8 = CuckooHashtable::with_choices(10, 8);
        assert_eq!(ht8.num_choices(), 8);
    }

    #[test]
    fn test_insert_and_lookup() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location = ItemLocation::new(0, 0, 0);
        let result = ht.insert(b"test", location, &provider);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        let lookup = ht.lookup(b"test", &provider);
        assert!(lookup.is_some());
        let (loc, freq) = lookup.unwrap();
        assert_eq!(loc.pool_id(), 0);
        assert_eq!(loc.segment_id(), 0);
        assert_eq!(loc.offset(), 0);
        assert!(freq >= 1);
    }

    #[test]
    fn test_insert_if_absent() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false), (8, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location1 = ItemLocation::new(0, 0, 0);
        let result = ht.insert_if_absent(b"test", location1, &provider);
        assert!(result.is_ok());

        let location2 = ItemLocation::new(0, 0, 8);
        let result = ht.insert_if_absent(b"test", location2, &provider);
        assert!(matches!(result, Err(CacheError::KeyExists)));
    }

    #[test]
    fn test_update_if_present() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false), (8, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        // Try to update non-existent key
        let location = ItemLocation::new(0, 0, 8);
        let result = ht.update_if_present(b"test", location, &provider);
        assert!(matches!(result, Err(CacheError::KeyNotFound)));

        // Insert first
        let location1 = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location1, &provider).unwrap();

        // Now update should work
        let result = ht.update_if_present(b"test", location, &provider);
        assert!(result.is_ok());
        let old = result.unwrap();
        assert_eq!(old.offset(), 0);
    }

    #[test]
    fn test_remove() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location, &provider).unwrap();

        assert!(ht.contains(b"test", &provider));
        assert!(ht.remove(b"test", location));
        assert!(!ht.contains(b"test", &provider));
    }

    #[test]
    fn test_convert_to_ghost() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location, &provider).unwrap();

        // Lookup to increase frequency
        for _ in 0..5 {
            ht.lookup(b"test", &provider);
        }

        // Convert to ghost
        assert!(ht.convert_to_ghost(b"test", location));

        // Should not be found via normal lookup
        assert!(!ht.contains(b"test", &provider));

        // But ghost frequency should be preserved
        let ghost_freq = ht.get_ghost_frequency(b"test");
        assert!(ghost_freq.is_some());
        assert!(ghost_freq.unwrap() > 1);
    }

    #[test]
    fn test_cas_location() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false), (8, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location1 = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location1, &provider).unwrap();

        let location2 = ItemLocation::new(0, 0, 8);

        // CAS with correct old location should succeed
        assert!(ht.cas_location(b"test", location1, location2, true));

        // Verify new location
        let lookup = ht.lookup(b"test", &provider);
        assert!(lookup.is_some());
        assert_eq!(lookup.unwrap().0.offset(), 8);
    }

    #[test]
    fn test_get_frequency() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        // Key doesn't exist
        assert!(ht.get_frequency(b"test", &provider).is_none());

        // Insert and check frequency
        let location = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location, &provider).unwrap();

        let freq = ht.get_frequency(b"test", &provider);
        assert!(freq.is_some());
        assert!(freq.unwrap() >= 1);
    }

    #[test]
    fn test_lookup_nonexistent() {
        let ht = CuckooHashtable::new(10);
        let segments: Vec<MockSegment> = vec![];
        let provider = crate::hashtable::SinglePool::new(&segments);

        assert!(ht.lookup(b"nonexistent", &provider).is_none());
    }

    #[test]
    fn test_contains_nonexistent() {
        let ht = CuckooHashtable::new(10);
        let segments: Vec<MockSegment> = vec![];
        let provider = crate::hashtable::SinglePool::new(&segments);

        assert!(!ht.contains(b"nonexistent", &provider));
    }

    #[test]
    fn test_remove_nonexistent() {
        let ht = CuckooHashtable::new(10);

        let location = ItemLocation::new(0, 0, 0);
        assert!(!ht.remove(b"nonexistent", location));
    }

    #[test]
    fn test_cas_location_wrong_old() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false), (8, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location1 = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location1, &provider).unwrap();

        // Try CAS with wrong old location
        let wrong_old = ItemLocation::new(0, 0, 16);
        let new = ItemLocation::new(0, 0, 8);
        assert!(!ht.cas_location(b"test", wrong_old, new, true));
    }

    #[test]
    fn test_cas_location_nonexistent() {
        let ht = CuckooHashtable::new(10);

        let old = ItemLocation::new(0, 0, 0);
        let new = ItemLocation::new(0, 0, 8);
        assert!(!ht.cas_location(b"nonexistent", old, new, true));
    }

    #[test]
    fn test_single_choice_hashtable() {
        let ht = CuckooHashtable::with_choices(10, 1);
        assert_eq!(ht.num_choices(), 1);

        let segments = vec![MockSegment {
            keys: vec![(0, b"key1".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location = ItemLocation::new(0, 0, 0);
        let result = ht.insert(b"key1", location, &provider);
        assert!(result.is_ok());

        assert!(ht.contains(b"key1", &provider));
    }

    #[test]
    fn test_three_choice_hashtable() {
        let ht = CuckooHashtable::with_choices(10, 3);
        assert_eq!(ht.num_choices(), 3);

        let segments = vec![MockSegment {
            keys: vec![(0, b"key1".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location = ItemLocation::new(0, 0, 0);
        let result = ht.insert(b"key1", location, &provider);
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_multiple_keys() {
        let ht = CuckooHashtable::new(10);

        // Create segments with multiple keys
        let mut keys = Vec::new();
        for i in 0..100 {
            let key = format!("key_{}", i);
            let offset = (i * 8) as u32;
            keys.push((offset, key.into_bytes(), false));
        }
        let segments = vec![MockSegment { keys }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        // Insert all keys
        for i in 0..100 {
            let key = format!("key_{}", i);
            let offset = (i * 8) as u32;
            let location = ItemLocation::new(0, 0, offset);
            let result = ht.insert(key.as_bytes(), location, &provider);
            assert!(result.is_ok(), "Failed to insert key_{}", i);
        }

        // Verify all keys exist
        for i in 0..100 {
            let key = format!("key_{}", i);
            assert!(
                ht.contains(key.as_bytes(), &provider),
                "key_{} not found",
                i
            );
        }
    }

    #[test]
    fn test_insert_overwrite() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false), (8, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        // First insert
        let location1 = ItemLocation::new(0, 0, 0);
        let result = ht.insert(b"test", location1, &provider);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // No old value

        // Second insert overwrites
        let location2 = ItemLocation::new(0, 0, 8);
        let result = ht.insert(b"test", location2, &provider);
        assert!(result.is_ok());
        let old = result.unwrap();
        assert!(old.is_some());
        assert_eq!(old.unwrap().offset(), 0);
    }

    #[test]
    fn test_hashbucket_default() {
        let bucket = Hashbucket::default();
        for slot in &bucket.items {
            assert_eq!(slot.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn test_update_freq_in_packed() {
        let packed = Hashbucket::pack_item(0x123, 10, 0, 100, 0);
        let updated = Hashbucket::update_freq_in_packed(packed, 50);

        assert_eq!(Hashbucket::item_freq(updated), 50);
        assert_eq!(Hashbucket::item_tag(updated), 0x123);
        assert_eq!(Hashbucket::item_pool_id(updated), 0);
        assert_eq!(Hashbucket::item_segment_id(updated), 100);
    }

    #[test]
    fn test_try_update_freq_max() {
        let packed = Hashbucket::pack_item(0x123, 127, 0, 100, 0);
        // At max frequency, should return None
        assert!(Hashbucket::try_update_freq(packed, 127).is_none());
    }

    #[test]
    fn test_try_update_freq_low() {
        let packed = Hashbucket::pack_item(0x123, 5, 0, 100, 0);
        // Low frequency always increments
        let result = Hashbucket::try_update_freq(packed, 5);
        assert!(result.is_some());
        let new_packed = result.unwrap();
        assert_eq!(Hashbucket::item_freq(new_packed), 6);
    }

    #[test]
    fn test_is_ghost_empty() {
        // Empty slot is not a ghost
        assert!(!Hashbucket::is_ghost(0));
    }

    #[test]
    fn test_is_ghost_live_entry() {
        let packed = Hashbucket::pack_item(0x123, 10, 0, 100, 0);
        assert!(!Hashbucket::is_ghost(packed));
    }

    #[test]
    fn test_convert_to_ghost_and_resurrect() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false), (8, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        // Insert with offset 0
        let location1 = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location1, &provider).unwrap();

        // Access to build up frequency
        for _ in 0..10 {
            ht.lookup(b"test", &provider);
        }

        // Convert to ghost
        assert!(ht.convert_to_ghost(b"test", location1));

        // Get ghost frequency
        let ghost_freq = ht.get_ghost_frequency(b"test");
        assert!(ghost_freq.is_some());

        // Re-insert should resurrect the ghost with preserved frequency
        let location2 = ItemLocation::new(0, 0, 8);
        let result = ht.insert(b"test", location2, &provider);
        assert!(result.is_ok());

        // Now the item should be found again
        assert!(ht.contains(b"test", &provider));
    }

    #[test]
    fn test_remove_with_ghost_config() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location, &provider).unwrap();

        // Remove the item
        assert!(ht.remove(b"test", location));

        // Should no longer be found
        assert!(!ht.contains(b"test", &provider));
    }

    #[test]
    fn test_get_ghost_frequency_nonexistent() {
        let ht = CuckooHashtable::new(10);
        assert!(ht.get_ghost_frequency(b"nonexistent").is_none());
    }

    #[test]
    fn test_convert_to_ghost_nonexistent() {
        let ht = CuckooHashtable::new(10);
        let location = ItemLocation::new(0, 0, 0);
        assert!(!ht.convert_to_ghost(b"nonexistent", location));
    }

    #[test]
    fn test_get_item_frequency() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        // Key doesn't exist
        let location = ItemLocation::new(0, 0, 0);
        assert!(ht.get_item_frequency(b"test", location).is_none());

        // Insert and check frequency by exact location
        ht.insert(b"test", location, &provider).unwrap();

        let freq = ht.get_item_frequency(b"test", location);
        assert!(freq.is_some());
        assert!(freq.unwrap() >= 1);
    }

    #[test]
    fn test_get_item_frequency_wrong_location() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location, &provider).unwrap();

        // Query with wrong location should return None
        let wrong_location = ItemLocation::new(0, 0, 8);
        assert!(ht.get_item_frequency(b"test", wrong_location).is_none());

        // Query with wrong pool_id
        let wrong_pool = ItemLocation::new(1, 0, 0);
        assert!(ht.get_item_frequency(b"test", wrong_pool).is_none());

        // Query with wrong segment_id
        let wrong_seg = ItemLocation::new(0, 1, 0);
        assert!(ht.get_item_frequency(b"test", wrong_seg).is_none());
    }

    #[test]
    fn test_insert_over_ghost_preserves_frequency() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false), (8, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        // Insert key
        let location1 = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location1, &provider).unwrap();

        // Increase frequency by multiple lookups
        for _ in 0..10 {
            ht.lookup(b"test", &provider);
        }

        // Get the frequency before converting to ghost
        let freq_before = ht.get_frequency(b"test", &provider).unwrap();
        assert!(freq_before > 1);

        // Convert to ghost
        assert!(ht.convert_to_ghost(b"test", location1));

        // Verify ghost frequency is preserved
        let ghost_freq = ht.get_ghost_frequency(b"test");
        assert!(ghost_freq.is_some());
        assert_eq!(ghost_freq.unwrap(), freq_before);

        // Insert over the ghost - frequency should be preserved
        let location2 = ItemLocation::new(0, 0, 8);
        let result = ht.insert(b"test", location2, &provider);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // Ghost resurrection returns None

        // Verify the frequency was preserved
        let freq_after = ht.get_frequency(b"test", &provider).unwrap();
        assert_eq!(freq_after, freq_before);
    }

    #[test]
    fn test_insert_if_absent_over_ghost() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false), (8, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        // Insert key
        let location1 = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location1, &provider).unwrap();

        // Increase frequency
        for _ in 0..5 {
            ht.lookup(b"test", &provider);
        }

        let freq_before = ht.get_frequency(b"test", &provider).unwrap();

        // Convert to ghost
        assert!(ht.convert_to_ghost(b"test", location1));

        // Now the key doesn't exist as live entry
        assert!(!ht.contains(b"test", &provider));

        // insert_if_absent should succeed over a ghost
        let location2 = ItemLocation::new(0, 0, 8);
        let result = ht.insert_if_absent(b"test", location2, &provider);
        assert!(result.is_ok());

        // Verify the key exists now
        assert!(ht.contains(b"test", &provider));

        // Frequency should be preserved from ghost
        let freq_after = ht.get_frequency(b"test", &provider).unwrap();
        assert_eq!(freq_after, freq_before);
    }

    #[test]
    fn test_convert_to_ghost_wrong_location() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location, &provider).unwrap();

        // Converting with wrong location should return false
        let wrong_location = ItemLocation::new(0, 0, 8);
        assert!(!ht.convert_to_ghost(b"test", wrong_location));

        // The entry should still be live
        assert!(ht.contains(b"test", &provider));
    }

    #[test]
    fn test_get_ghost_frequency_live_entry() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location, &provider).unwrap();

        // get_ghost_frequency should return None for live entries
        assert!(ht.get_ghost_frequency(b"test").is_none());
    }

    #[test]
    fn test_remove_wrong_location() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location, &provider).unwrap();

        // Try to remove with wrong location - should fail
        let wrong_location = ItemLocation::new(0, 0, 8);
        assert!(!ht.remove(b"test", wrong_location));

        // Entry should still exist
        assert!(ht.contains(b"test", &provider));
    }

    #[test]
    fn test_cas_location_reset_frequency() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), false), (8, b"test".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location1 = ItemLocation::new(0, 0, 0);
        ht.insert(b"test", location1, &provider).unwrap();

        // Increase frequency
        for _ in 0..10 {
            ht.lookup(b"test", &provider);
        }

        let freq_before = ht.get_frequency(b"test", &provider).unwrap();
        assert!(freq_before > 1);

        // CAS with preserve_freq = false should reset frequency
        let location2 = ItemLocation::new(0, 0, 8);
        assert!(ht.cas_location(b"test", location1, location2, false));

        let freq_after = ht.get_frequency(b"test", &provider).unwrap();
        assert_eq!(freq_after, 1);
    }

    #[test]
    fn test_lookup_deleted_entry() {
        let ht = CuckooHashtable::new(10);
        let segments = vec![MockSegment {
            keys: vec![(0, b"test".to_vec(), true)], // marked as deleted
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location = ItemLocation::new(0, 0, 0);

        // Insert should still work (uses allow_deleted=true)
        ht.insert(b"test", location, &provider).unwrap();

        // But lookup should not find it (uses allow_deleted=false)
        assert!(ht.lookup(b"test", &provider).is_none());
        assert!(!ht.contains(b"test", &provider));
    }

    #[test]
    fn test_hashtable_with_four_choices() {
        let ht = CuckooHashtable::with_choices(10, 4);
        assert_eq!(ht.num_choices(), 4);

        let segments = vec![MockSegment {
            keys: vec![(0, b"key".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location = ItemLocation::new(0, 0, 0);
        let result = ht.insert(b"key", location, &provider);
        assert!(result.is_ok());
        assert!(ht.contains(b"key", &provider));
    }

    #[test]
    fn test_hashtable_with_eight_choices() {
        let ht = CuckooHashtable::with_choices(10, 8);
        assert_eq!(ht.num_choices(), 8);

        let segments = vec![MockSegment {
            keys: vec![(0, b"key".to_vec(), false)],
        }];
        let provider = crate::hashtable::SinglePool::new(&segments);

        let location = ItemLocation::new(0, 0, 0);
        let result = ht.insert(b"key", location, &provider);
        assert!(result.is_ok());
    }
}
