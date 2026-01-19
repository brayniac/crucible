//! Lock-free cuckoo hashtable implementation.
//!
//! This hashtable supports:
//! - Configurable N-choice hashing (1-8 choices) for tunable load factors
//! - ASFC (Adaptive Software Frequency Counter) for frequency tracking
//! - Ghost entries for preserving frequency after eviction
//! - Storage-agnostic location handling
//! - SIMD-accelerated bucket scanning on supported platforms

use crate::error::{CacheError, CacheResult};
use crate::hashtable::{Hashtable, KeyVerifier};
use crate::location::Location;
use crate::sync::{AtomicU64, Ordering};
use ahash::RandomState;

/// Maximum number of bucket choices supported.
pub const MAX_CHOICES: u8 = 8;

/// Lock-free hashtable for caches.
///
/// Each entry stores:
/// - 12-bit tag (hash suffix for fast filtering)
/// - 8-bit frequency counter (ASFC algorithm)
/// - 44-bit location (opaque, meaning defined by storage backend)
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

    /// Prefetch a bucket into cache.
    ///
    /// Call this before accessing a bucket to hide memory latency.
    #[inline]
    fn prefetch_bucket(&self, index: usize) {
        debug_assert!(index < self.num_buckets);
        let bucket_ptr = &self.buckets[index] as *const Hashbucket as *const i8;

        #[cfg(all(target_arch = "x86_64", target_feature = "sse"))]
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(bucket_ptr);
        }

        #[cfg(target_arch = "aarch64")]
        unsafe {
            // PRFM PLDL1KEEP - prefetch for load, L1 cache, keep in cache
            std::arch::asm!(
                "prfm pldl1keep, [{ptr}]",
                ptr = in(reg) bucket_ptr,
                options(nostack, preserves_flags)
            );
        }

        #[cfg(not(any(
            all(target_arch = "x86_64", target_feature = "sse"),
            target_arch = "aarch64"
        )))]
        let _ = bucket_ptr;
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

    /// Find slots with matching tags using SIMD (AVX2).
    ///
    /// Returns a bitmask where bit N is set if items[N] has a matching tag
    /// and is not empty/ghost. Scans all 8 slots using 2 × 256-bit loads.
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[inline]
    fn find_tag_matches_simd(bucket: &Hashbucket, tag_shifted: u64) -> u8 {
        use std::arch::x86_64::*;

        // SAFETY: bucket is 64-byte aligned with 8 × 8-byte items
        // We load all 8 slots with 2 × 256-bit loads
        unsafe {
            let items_ptr = bucket.items.as_ptr() as *const u8;

            // Load items[0..4] (32 bytes)
            let slots_0_3 = _mm256_load_si256(items_ptr as *const __m256i);
            // Load items[4..8] (32 bytes)
            let slots_4_7 = _mm256_load_si256(items_ptr.add(32) as *const __m256i);

            // Tag mask and broadcast tag
            let tag_mask_val = 0xFFF0_0000_0000_0000_u64 as i64;
            let tag_shifted_i64 = tag_shifted as i64;

            let tag_mask = _mm256_set1_epi64x(tag_mask_val);
            let tag_vec = _mm256_set1_epi64x(tag_shifted_i64);

            // Ghost mask (44 bits all set = ghost location)
            let ghost_mask_val = 0x0000_0FFF_FFFF_FFFF_u64 as i64;
            let ghost_vec = _mm256_set1_epi64x(ghost_mask_val);
            let zero = _mm256_setzero_si256();
            let all_ones = _mm256_set1_epi64x(-1);

            // Process slots 0-3
            let tags_0_3 = _mm256_and_si256(slots_0_3, tag_mask);
            let tag_match_0_3 = _mm256_cmpeq_epi64(tags_0_3, tag_vec);
            let nonzero_0_3 = _mm256_xor_si256(_mm256_cmpeq_epi64(slots_0_3, zero), all_ones);
            let locs_0_3 = _mm256_and_si256(slots_0_3, _mm256_set1_epi64x(ghost_mask_val));
            let nonghost_0_3 = _mm256_xor_si256(_mm256_cmpeq_epi64(locs_0_3, ghost_vec), all_ones);
            let valid_0_3 =
                _mm256_and_si256(tag_match_0_3, _mm256_and_si256(nonzero_0_3, nonghost_0_3));

            // Process slots 4-7
            let tags_4_7 = _mm256_and_si256(slots_4_7, tag_mask);
            let tag_match_4_7 = _mm256_cmpeq_epi64(tags_4_7, tag_vec);
            let nonzero_4_7 = _mm256_xor_si256(_mm256_cmpeq_epi64(slots_4_7, zero), all_ones);
            let locs_4_7 = _mm256_and_si256(slots_4_7, _mm256_set1_epi64x(ghost_mask_val));
            let nonghost_4_7 = _mm256_xor_si256(_mm256_cmpeq_epi64(locs_4_7, ghost_vec), all_ones);
            let valid_4_7 =
                _mm256_and_si256(tag_match_4_7, _mm256_and_si256(nonzero_4_7, nonghost_4_7));

            // Convert to bitmasks
            let mask_0_3 = _mm256_movemask_pd(_mm256_castsi256_pd(valid_0_3)) as u8;
            let mask_4_7 = _mm256_movemask_pd(_mm256_castsi256_pd(valid_4_7)) as u8;

            // Combine: bits 0-3 from first load, bits 4-7 from second load
            mask_0_3 | (mask_4_7 << 4)
        }
    }

    /// Find slots with matching tags using NEON (ARM64).
    ///
    /// Returns a bitmask where bit N is set if items[N] has a matching tag
    /// and is not empty/ghost. Scans all 8 slots using 4 × 128-bit loads.
    #[cfg(target_arch = "aarch64")]
    #[inline]
    fn find_tag_matches_simd(bucket: &Hashbucket, tag_shifted: u64) -> u8 {
        use std::arch::aarch64::*;

        const TAG_MASK: u64 = 0xFFF0_0000_0000_0000;
        const GHOST_LOCATION: u64 = 0x0000_0FFF_FFFF_FFFF;

        // SAFETY: We're loading pairs of u64 values using NEON (4 loads × 2 slots = 8 slots)
        unsafe {
            let items_ptr = bucket.items.as_ptr() as *const u64;

            let tag_mask_vec = vdupq_n_u64(TAG_MASK);
            let tag_vec = vdupq_n_u64(tag_shifted);
            let ghost_vec = vdupq_n_u64(GHOST_LOCATION);
            let zero_vec = vdupq_n_u64(0);

            // Process slots 0-1
            let slots_0_1 = vld1q_u64(items_ptr);
            let tags_0_1 = vandq_u64(slots_0_1, tag_mask_vec);
            let tag_match_0_1 = vceqq_u64(tags_0_1, tag_vec);
            let nonzero_0_1 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(slots_0_1, zero_vec)));
            let locs_0_1 = vandq_u64(slots_0_1, vdupq_n_u64(GHOST_LOCATION));
            let nonghost_0_1 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(locs_0_1, ghost_vec)));
            let valid_0_1 = vandq_u32(
                vreinterpretq_u32_u64(tag_match_0_1),
                vandq_u32(nonzero_0_1, nonghost_0_1),
            );

            // Process slots 2-3
            let slots_2_3 = vld1q_u64(items_ptr.add(2));
            let tags_2_3 = vandq_u64(slots_2_3, tag_mask_vec);
            let tag_match_2_3 = vceqq_u64(tags_2_3, tag_vec);
            let nonzero_2_3 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(slots_2_3, zero_vec)));
            let locs_2_3 = vandq_u64(slots_2_3, vdupq_n_u64(GHOST_LOCATION));
            let nonghost_2_3 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(locs_2_3, ghost_vec)));
            let valid_2_3 = vandq_u32(
                vreinterpretq_u32_u64(tag_match_2_3),
                vandq_u32(nonzero_2_3, nonghost_2_3),
            );

            // Process slots 4-5
            let slots_4_5 = vld1q_u64(items_ptr.add(4));
            let tags_4_5 = vandq_u64(slots_4_5, tag_mask_vec);
            let tag_match_4_5 = vceqq_u64(tags_4_5, tag_vec);
            let nonzero_4_5 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(slots_4_5, zero_vec)));
            let locs_4_5 = vandq_u64(slots_4_5, vdupq_n_u64(GHOST_LOCATION));
            let nonghost_4_5 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(locs_4_5, ghost_vec)));
            let valid_4_5 = vandq_u32(
                vreinterpretq_u32_u64(tag_match_4_5),
                vandq_u32(nonzero_4_5, nonghost_4_5),
            );

            // Process slots 6-7
            let slots_6_7 = vld1q_u64(items_ptr.add(6));
            let tags_6_7 = vandq_u64(slots_6_7, tag_mask_vec);
            let tag_match_6_7 = vceqq_u64(tags_6_7, tag_vec);
            let nonzero_6_7 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(slots_6_7, zero_vec)));
            let locs_6_7 = vandq_u64(slots_6_7, vdupq_n_u64(GHOST_LOCATION));
            let nonghost_6_7 = vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(locs_6_7, ghost_vec)));
            let valid_6_7 = vandq_u32(
                vreinterpretq_u32_u64(tag_match_6_7),
                vandq_u32(nonzero_6_7, nonghost_6_7),
            );

            // Extract results from each pair
            let v0_1 = vreinterpretq_u64_u32(valid_0_1);
            let v2_3 = vreinterpretq_u64_u32(valid_2_3);
            let v4_5 = vreinterpretq_u64_u32(valid_4_5);
            let v6_7 = vreinterpretq_u64_u32(valid_6_7);

            let bit0 = if vgetq_lane_u64(v0_1, 0) != 0 { 1u8 } else { 0 };
            let bit1 = if vgetq_lane_u64(v0_1, 1) != 0 { 2u8 } else { 0 };
            let bit2 = if vgetq_lane_u64(v2_3, 0) != 0 { 4u8 } else { 0 };
            let bit3 = if vgetq_lane_u64(v2_3, 1) != 0 { 8u8 } else { 0 };
            let bit4 = if vgetq_lane_u64(v4_5, 0) != 0 {
                16u8
            } else {
                0
            };
            let bit5 = if vgetq_lane_u64(v4_5, 1) != 0 {
                32u8
            } else {
                0
            };
            let bit6 = if vgetq_lane_u64(v6_7, 0) != 0 {
                64u8
            } else {
                0
            };
            let bit7 = if vgetq_lane_u64(v6_7, 1) != 0 {
                128u8
            } else {
                0
            };

            bit0 | bit1 | bit2 | bit3 | bit4 | bit5 | bit6 | bit7
        }
    }

    /// Scalar fallback for finding tag matches.
    ///
    /// Returns a bitmask where bit N is set if items[N] has a matching tag
    /// and is not empty/ghost. Scans all 8 slots.
    #[cfg(not(any(
        all(target_arch = "x86_64", target_feature = "avx2"),
        target_arch = "aarch64"
    )))]
    #[inline]
    fn find_tag_matches_simd(bucket: &Hashbucket, tag_shifted: u64) -> u8 {
        const TAG_MASK: u64 = 0xFFF0_0000_0000_0000;
        const GHOST_LOCATION: u64 = 0x0000_0FFF_FFFF_FFFF;

        let mut result = 0u8;

        for slot_index in 0..8 {
            let packed = bucket.items[slot_index].load(Ordering::Relaxed);

            if packed != 0
                && (packed & GHOST_LOCATION) != GHOST_LOCATION
                && (packed & TAG_MASK) == tag_shifted
            {
                result |= 1 << slot_index;
            }
        }

        result
    }

    /// Search a bucket for an item, updating frequency on hit.
    #[inline]
    fn search_bucket_for_get(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        verifier: &impl KeyVerifier,
    ) -> Option<(Location, u8)> {
        let bucket = self.bucket(bucket_index);

        const TAG_MASK: u64 = 0xFFF0_0000_0000_0000;
        const GHOST_LOCATION: u64 = 0x0000_0FFF_FFFF_FFFF;
        let tag_shifted = (tag as u64) << 52;

        // Use SIMD to find slots with matching tags (filters out empty/ghost)
        let mut mask = Self::find_tag_matches_simd(bucket, tag_shifted);

        // Iterate only over slots with matching tags
        while mask != 0 {
            let slot_index = mask.trailing_zeros() as usize;
            mask &= mask - 1; // Clear lowest set bit

            // Single Acquire load for synchronization
            // (SIMD already did Relaxed loads to find matches)
            let packed = bucket.items[slot_index].load(Ordering::Acquire);

            // Re-verify after Acquire (state may have changed)
            if packed == 0 || (packed & GHOST_LOCATION) == GHOST_LOCATION {
                continue;
            }
            if (packed & TAG_MASK) != tag_shifted {
                continue;
            }

            let location = Hashbucket::location(packed);

            // Prefetch segment data while we verify - pipeline the memory access
            verifier.prefetch(location);

            // Verify key matches using the verifier
            if verifier.verify(key, location, false) {
                // Update frequency (best effort)
                let freq = Hashbucket::freq(packed);
                if freq < 127 {
                    if let Some(new_packed) = Hashbucket::try_update_freq(packed, freq) {
                        let _ = bucket.items[slot_index].compare_exchange(
                            packed,
                            new_packed,
                            Ordering::Release,
                            Ordering::Relaxed,
                        );
                    }
                }

                return Some((location, freq));
            }
        }

        None
    }

    /// Search a bucket for existence (no frequency update).
    fn search_bucket_exists(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        verifier: &impl KeyVerifier,
    ) -> bool {
        let bucket = self.bucket(bucket_index);

        const TAG_MASK: u64 = 0xFFF0_0000_0000_0000;
        const GHOST_LOCATION: u64 = 0x0000_0FFF_FFFF_FFFF;
        let tag_shifted = (tag as u64) << 52;

        // Use SIMD to find slots with matching tags (filters out empty/ghost)
        let mut mask = Self::find_tag_matches_simd(bucket, tag_shifted);

        // Iterate only over slots with matching tags
        while mask != 0 {
            let slot_index = mask.trailing_zeros() as usize;
            mask &= mask - 1;

            // Single Acquire load for synchronization
            let packed = bucket.items[slot_index].load(Ordering::Acquire);

            // Re-verify after Acquire (state may have changed)
            if packed == 0 || (packed & GHOST_LOCATION) == GHOST_LOCATION {
                continue;
            }
            if (packed & TAG_MASK) != tag_shifted {
                continue;
            }

            let location = Hashbucket::location(packed);

            // Prefetch segment data while we verify
            verifier.prefetch(location);

            if verifier.verify(key, location, false) {
                return true;
            }
        }

        false
    }

    /// Search for a ghost entry's frequency.
    fn search_bucket_for_ghost(&self, bucket_index: usize, tag: u16) -> Option<u8> {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let slot = &bucket.items[slot_index];

            // Speculative Relaxed load to check tag
            let speculative = slot.load(Ordering::Relaxed);

            if Hashbucket::is_ghost(speculative) && Hashbucket::tag(speculative) == tag {
                // Tag matches - do Acquire load to synchronize
                let packed = slot.load(Ordering::Acquire);

                // Re-check after Acquire load (slot may have changed)
                if Hashbucket::is_ghost(packed) && Hashbucket::tag(packed) == tag {
                    return Some(Hashbucket::freq(packed));
                }
            }
        }

        None
    }

    /// Search for frequency of a specific item.
    fn search_bucket_for_freq(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        verifier: &impl KeyVerifier,
    ) -> Option<u8> {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let slot = &bucket.items[slot_index];

            // Speculative Relaxed load to check tag
            let speculative = slot.load(Ordering::Relaxed);

            if speculative == 0 || Hashbucket::is_ghost(speculative) {
                continue;
            }

            if Hashbucket::tag(speculative) == tag {
                // Tag matches - do Acquire load to synchronize
                let packed = slot.load(Ordering::Acquire);

                // Re-check after Acquire load (slot may have changed)
                if packed == 0 || Hashbucket::is_ghost(packed) {
                    continue;
                }

                if Hashbucket::tag(packed) != tag {
                    continue;
                }

                let location = Hashbucket::location(packed);

                if verifier.verify(key, location, false) {
                    return Some(Hashbucket::freq(packed));
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
        location: Location,
    ) -> Option<u8> {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let slot = &bucket.items[slot_index];

            // Speculative Relaxed load to check tag
            let speculative = slot.load(Ordering::Relaxed);

            if speculative == 0 || Hashbucket::is_ghost(speculative) {
                continue;
            }

            if Hashbucket::tag(speculative) == tag {
                // Tag matches - do Acquire load to synchronize
                let packed = slot.load(Ordering::Acquire);

                // Re-check after Acquire load (slot may have changed)
                if packed == 0 || Hashbucket::is_ghost(packed) {
                    continue;
                }

                if Hashbucket::tag(packed) == tag && Hashbucket::location(packed) == location {
                    return Some(Hashbucket::freq(packed));
                }
            }
        }

        None
    }

    /// Try to link in a bucket, handling existing entries and ghosts.
    fn try_link_in_bucket(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        new_packed: u64,
        verifier: &impl KeyVerifier,
    ) -> Option<CacheResult<Option<Location>>> {
        let bucket = self.bucket(bucket_index);

        // First pass: look for existing entry or matching ghost
        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let slot = &bucket.items[slot_index];

            // Speculative Relaxed load to check tag
            let speculative = slot.load(Ordering::Relaxed);

            if Hashbucket::tag(speculative) == tag {
                // Tag matches - do Acquire load to synchronize before CAS
                let packed = slot.load(Ordering::Acquire);

                // Re-check tag after Acquire load
                if Hashbucket::tag(packed) != tag {
                    continue;
                }

                if Hashbucket::is_ghost(packed) {
                    // Replace ghost, preserving frequency
                    let freq = Hashbucket::freq(packed);
                    let new_with_freq = Hashbucket::with_freq(new_packed, freq);

                    match slot.compare_exchange(
                        packed,
                        new_with_freq,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return Some(Ok(None)),
                        Err(_) => continue,
                    }
                }

                let location = Hashbucket::location(packed);

                if verifier.verify(key, location, true) {
                    // Replace existing entry, preserving frequency
                    let freq = Hashbucket::freq(packed);
                    let new_with_freq = Hashbucket::with_freq(new_packed, freq);

                    match slot.compare_exchange(
                        packed,
                        new_with_freq,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            return Some(Ok(Some(location)));
                        }
                        Err(_) => continue,
                    }
                }
            }
        }

        // Second pass: look for empty slot
        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let slot = &bucket.items[slot_index];

            // Relaxed is sufficient here - CAS will verify the expected value
            let packed = slot.load(Ordering::Relaxed);

            if packed == 0 {
                match slot.compare_exchange(0, new_packed, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => return Some(Ok(None)),
                    Err(_) => continue,
                }
            }
        }

        // Third pass: look for any ghost to evict
        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let slot = &bucket.items[slot_index];

            // Speculative Relaxed load to check ghost status
            let speculative = slot.load(Ordering::Relaxed);

            if Hashbucket::is_ghost(speculative) {
                // Do Acquire load before CAS
                let packed = slot.load(Ordering::Acquire);

                if Hashbucket::is_ghost(packed) {
                    match slot.compare_exchange(
                        packed,
                        new_packed,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return Some(Ok(None)),
                        Err(_) => continue,
                    }
                }
            }
        }

        None // Bucket full, try another
    }

    /// Try to unlink an item from a bucket.
    fn try_unlink_in_bucket(&self, bucket_index: usize, tag: u16, expected: Location) -> bool {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let slot = &bucket.items[slot_index];

            // Speculative Relaxed load to check tag
            let speculative = slot.load(Ordering::Relaxed);

            if speculative == 0 || Hashbucket::is_ghost(speculative) {
                continue;
            }

            if Hashbucket::tag(speculative) == tag {
                // Tag matches - do Acquire load to synchronize before CAS
                let packed = slot.load(Ordering::Acquire);

                // Re-check after Acquire load
                if packed == 0 || Hashbucket::is_ghost(packed) {
                    continue;
                }

                if Hashbucket::tag(packed) == tag && Hashbucket::location(packed) == expected {
                    match slot.compare_exchange(packed, 0, Ordering::Release, Ordering::Relaxed) {
                        Ok(_) => return true,
                        Err(_) => continue,
                    }
                }
            }
        }

        false
    }

    /// Try to convert an item to ghost in a bucket.
    fn try_to_ghost_in_bucket(&self, bucket_index: usize, tag: u16, expected: Location) -> bool {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let slot = &bucket.items[slot_index];

            // Speculative Relaxed load to check tag
            let speculative = slot.load(Ordering::Relaxed);

            if speculative == 0 || Hashbucket::is_ghost(speculative) {
                continue;
            }

            if Hashbucket::tag(speculative) == tag {
                // Tag matches - do Acquire load to synchronize before CAS
                let packed = slot.load(Ordering::Acquire);

                // Re-check after Acquire load
                if packed == 0 || Hashbucket::is_ghost(packed) {
                    continue;
                }

                if Hashbucket::tag(packed) == tag && Hashbucket::location(packed) == expected {
                    let ghost = Hashbucket::to_ghost(packed);
                    match slot.compare_exchange(packed, ghost, Ordering::Release, Ordering::Relaxed)
                    {
                        Ok(_) => return true,
                        Err(_) => continue,
                    }
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
        old_location: Location,
        new_location: Location,
        preserve_freq: bool,
    ) -> bool {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let slot = &bucket.items[slot_index];

            // Speculative Relaxed load to check tag
            let speculative = slot.load(Ordering::Relaxed);

            if speculative == 0 || Hashbucket::is_ghost(speculative) {
                continue;
            }

            if Hashbucket::tag(speculative) == tag {
                // Tag matches - do Acquire load to synchronize before CAS
                let packed = slot.load(Ordering::Acquire);

                // Re-check after Acquire load
                if packed == 0 || Hashbucket::is_ghost(packed) {
                    continue;
                }

                if Hashbucket::tag(packed) == tag && Hashbucket::location(packed) == old_location {
                    let freq = if preserve_freq {
                        Hashbucket::freq(packed)
                    } else {
                        1
                    };
                    let new_packed = Hashbucket::pack(tag, freq, new_location);

                    if slot
                        .compare_exchange(packed, new_packed, Ordering::Release, Ordering::Relaxed)
                        .is_ok()
                    {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Check if key exists (for ADD semantics).
    fn check_key_exists(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        verifier: &impl KeyVerifier,
    ) -> bool {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let slot = &bucket.items[slot_index];

            // Speculative Relaxed load to check tag
            let speculative = slot.load(Ordering::Relaxed);

            if speculative == 0 || Hashbucket::is_ghost(speculative) {
                continue;
            }

            if Hashbucket::tag(speculative) == tag {
                // Tag matches - do Acquire load to synchronize
                let packed = slot.load(Ordering::Acquire);

                // Re-check after Acquire load
                if packed == 0 || Hashbucket::is_ghost(packed) {
                    continue;
                }

                if Hashbucket::tag(packed) != tag {
                    continue;
                }

                let location = Hashbucket::location(packed);

                if verifier.verify(key, location, false) {
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
        location: Location,
    ) -> Option<CacheResult<()>> {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let slot = &bucket.items[slot_index];

            // Speculative Relaxed load to check tag
            let speculative = slot.load(Ordering::Relaxed);

            if Hashbucket::is_ghost(speculative) && Hashbucket::tag(speculative) == tag {
                // Tag matches - do Acquire load to synchronize before CAS
                let packed = slot.load(Ordering::Acquire);

                // Re-check after Acquire load
                if Hashbucket::is_ghost(packed) && Hashbucket::tag(packed) == tag {
                    let freq = Hashbucket::freq(packed);
                    let new_packed = Hashbucket::pack(tag, freq, location);

                    match slot.compare_exchange(
                        packed,
                        new_packed,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return Some(Ok(())),
                        Err(_) => continue,
                    }
                }
            }
        }

        None
    }

    /// Try to insert into empty slot for ADD.
    fn try_insert_empty_for_add(
        &self,
        bucket_index: usize,
        tag: u16,
        new_packed: u64,
        key: &[u8],
        verifier: &impl KeyVerifier,
    ) -> Option<CacheResult<()>> {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let slot = &bucket.items[slot_index];

            // Relaxed is sufficient here - CAS will verify the expected value
            let packed = slot.load(Ordering::Relaxed);

            if packed == 0 {
                match slot.compare_exchange(0, new_packed, Ordering::Release, Ordering::Acquire) {
                    Ok(_) => {
                        // Double-check for race condition
                        if self.check_for_duplicate_after_insert(
                            bucket_index,
                            slot,
                            tag,
                            key,
                            verifier,
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
                            && Hashbucket::tag(new_current) == tag
                        {
                            let location = Hashbucket::location(new_current);
                            if verifier.verify(key, location, false) {
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
    fn check_for_duplicate_after_insert(
        &self,
        inserted_bucket: usize,
        inserted_slot: &AtomicU64,
        tag: u16,
        key: &[u8],
        verifier: &impl KeyVerifier,
    ) -> bool {
        let hash = self.hash_key(key);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            let bucket = self.bucket(bucket_index);

            for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
                let slot = &bucket.items[slot_index];

                if bucket_index == inserted_bucket && std::ptr::eq(slot, inserted_slot) {
                    continue;
                }

                // Speculative Relaxed load to check tag
                let speculative = slot.load(Ordering::Relaxed);

                if speculative == 0 || Hashbucket::is_ghost(speculative) {
                    continue;
                }

                if Hashbucket::tag(speculative) == tag {
                    // Tag matches - do Acquire load to synchronize
                    let packed = slot.load(Ordering::Acquire);

                    // Re-check after Acquire load
                    if packed == 0 || Hashbucket::is_ghost(packed) {
                        continue;
                    }

                    if Hashbucket::tag(packed) != tag {
                        continue;
                    }

                    let location = Hashbucket::location(packed);

                    if verifier.verify(key, location, false) {
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

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let slot = &bucket.items[slot_index];

            // Speculative Relaxed load to check ghost status
            let speculative = slot.load(Ordering::Relaxed);

            if Hashbucket::is_ghost(speculative) {
                // Do Acquire load before CAS
                let packed = slot.load(Ordering::Acquire);

                if Hashbucket::is_ghost(packed) {
                    match slot.compare_exchange(
                        packed,
                        new_packed,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return Some(Ok(())),
                        Err(_) => continue,
                    }
                }
            }
        }

        None
    }

    /// Try to replace existing entry for REPLACE semantics.
    fn try_replace_existing_for_replace(
        &self,
        bucket_index: usize,
        tag: u16,
        key: &[u8],
        new_location: Location,
        verifier: &impl KeyVerifier,
    ) -> Option<CacheResult<Location>> {
        let bucket = self.bucket(bucket_index);

        for slot_index in 0..Hashbucket::NUM_ITEM_SLOTS {
            let slot = &bucket.items[slot_index];

            // Speculative Relaxed load to check tag
            let speculative = slot.load(Ordering::Relaxed);

            if speculative == 0 || Hashbucket::is_ghost(speculative) {
                continue;
            }

            if Hashbucket::tag(speculative) == tag {
                // Tag matches - do Acquire load to synchronize before CAS
                let packed = slot.load(Ordering::Acquire);

                // Re-check after Acquire load
                if packed == 0 || Hashbucket::is_ghost(packed) {
                    continue;
                }

                if Hashbucket::tag(packed) != tag {
                    continue;
                }

                let old_location = Hashbucket::location(packed);

                if verifier.verify(key, old_location, false) {
                    let freq = Hashbucket::freq(packed);
                    let new_packed = Hashbucket::pack(tag, freq, new_location);

                    match slot.compare_exchange(
                        packed,
                        new_packed,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return Some(Ok(old_location)),
                        Err(_) => continue,
                    }
                }
            }
        }

        None
    }
}

impl Hashtable for CuckooHashtable {
    fn lookup(&self, key: &[u8], verifier: &impl KeyVerifier) -> Option<(Location, u8)> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);
        let num_choices = self.num_choices as usize;

        // Prefetch all candidate buckets upfront to hide memory latency
        for &bucket_index in &buckets[..num_choices] {
            self.prefetch_bucket(bucket_index);
        }

        for &bucket_index in &buckets[..num_choices] {
            if let Some(result) = self.search_bucket_for_get(bucket_index, tag, key, verifier) {
                return Some(result);
            }
        }

        None
    }

    fn contains(&self, key: &[u8], verifier: &impl KeyVerifier) -> bool {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);
        let num_choices = self.num_choices as usize;

        // Prefetch all candidate buckets upfront to hide memory latency
        for &bucket_index in &buckets[..num_choices] {
            self.prefetch_bucket(bucket_index);
        }

        for &bucket_index in &buckets[..num_choices] {
            if self.search_bucket_exists(bucket_index, tag, key, verifier) {
                return true;
            }
        }

        false
    }

    fn insert(
        &self,
        key: &[u8],
        location: Location,
        verifier: &impl KeyVerifier,
    ) -> CacheResult<Option<Location>> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);
        let choices = &buckets[..self.num_choices as usize];

        let new_packed = Hashbucket::pack(tag, 1, location);

        // First pass: try to find existing key or ghost in any bucket
        for &bucket_index in choices {
            if let Some(result) =
                self.try_link_in_bucket(bucket_index, tag, key, new_packed, verifier)
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

            if let Some(result) = self.try_link_in_bucket(target, tag, key, new_packed, verifier) {
                return result;
            }

            // Try remaining buckets in order of occupancy
            let mut sorted: Vec<_> = choices.to_vec();
            sorted.sort_by_key(|&b| self.count_occupied(b));
            for bucket_index in sorted {
                if let Some(result) =
                    self.try_link_in_bucket(bucket_index, tag, key, new_packed, verifier)
                {
                    return result;
                }
            }
        }

        Err(CacheError::HashTableFull)
    }

    fn insert_if_absent(
        &self,
        key: &[u8],
        location: Location,
        verifier: &impl KeyVerifier,
    ) -> CacheResult<()> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);
        let choices = &buckets[..self.num_choices as usize];

        // Phase 1: Check if key already exists in any bucket
        for &bucket_index in choices {
            if self.check_key_exists(bucket_index, tag, key, verifier) {
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
        let new_packed = Hashbucket::pack(tag, 1, location);

        // Sort buckets by occupancy (least-full first)
        let mut sorted: Vec<_> = choices.to_vec();
        sorted.sort_by_key(|&b| self.count_occupied(b));

        for bucket_index in &sorted {
            if let Some(result) =
                self.try_insert_empty_for_add(*bucket_index, tag, new_packed, key, verifier)
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

    fn update_if_present(
        &self,
        key: &[u8],
        location: Location,
        verifier: &impl KeyVerifier,
    ) -> CacheResult<Location> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if let Some(result) =
                self.try_replace_existing_for_replace(bucket_index, tag, key, location, verifier)
            {
                return result;
            }
        }

        Err(CacheError::KeyNotFound)
    }

    fn remove(&self, key: &[u8], expected: Location) -> bool {
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

    fn convert_to_ghost(&self, key: &[u8], expected: Location) -> bool {
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
        old_location: Location,
        new_location: Location,
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

    fn get_frequency(&self, key: &[u8], verifier: &impl KeyVerifier) -> Option<u8> {
        let hash = self.hash_key(key);
        let tag = Self::tag_from_hash(hash);
        let buckets = self.bucket_indices(hash);

        for &bucket_index in &buckets[..self.num_choices as usize] {
            if let Some(freq) = self.search_bucket_for_freq(bucket_index, tag, key, verifier) {
                return Some(freq);
            }
        }

        None
    }

    fn get_item_frequency(&self, key: &[u8], location: Location) -> Option<u8> {
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
/// Contains 8 item slots, each packed as:
/// `[12 bits tag][8 bits freq][44 bits location]`
///
/// Empty slots have value 0. No separate metadata - SIMD scans all 8 slots.
#[repr(C, align(64))]
pub struct Hashbucket {
    /// Item slots (8 items per bucket, no metadata).
    pub(crate) items: [AtomicU64; 8],
}

const _: () = assert!(std::mem::size_of::<Hashbucket>() == 64);
const _: () = assert!(std::mem::align_of::<Hashbucket>() == 64);

impl Hashbucket {
    /// Create a new empty bucket.
    pub fn new() -> Self {
        Self {
            items: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Pack an entry into a u64.
    ///
    /// Layout: `[12 bits tag][8 bits freq][44 bits location]`
    #[inline]
    pub fn pack(tag: u16, freq: u8, location: Location) -> u64 {
        let tag_64 = (tag as u64 & 0xFFF) << 52;
        let freq_64 = (freq as u64 & 0xFF) << 44;
        let loc_64 = location.as_raw() & Location::MAX_RAW;
        tag_64 | freq_64 | loc_64
    }

    /// Extract tag (12 bits).
    #[inline(always)]
    pub fn tag(packed: u64) -> u16 {
        (packed >> 52) as u16
    }

    /// Extract frequency (8 bits).
    #[inline(always)]
    pub fn freq(packed: u64) -> u8 {
        ((packed >> 44) & 0xFF) as u8
    }

    /// Extract location (44 bits).
    #[inline(always)]
    pub fn location(packed: u64) -> Location {
        Location::from_raw(packed)
    }

    /// Check if entry is a ghost.
    #[inline(always)]
    pub fn is_ghost(packed: u64) -> bool {
        packed != 0 && Self::location(packed).is_ghost()
    }

    /// Pack a ghost entry (tag + frequency only).
    #[inline]
    pub fn pack_ghost(tag: u16, freq: u8) -> u64 {
        Self::pack(tag, freq, Location::GHOST)
    }

    /// Convert a live entry to ghost.
    #[inline]
    pub fn to_ghost(packed: u64) -> u64 {
        Self::pack_ghost(Self::tag(packed), Self::freq(packed))
    }

    /// Update frequency in a packed value.
    #[inline]
    pub fn with_freq(packed: u64, freq: u8) -> u64 {
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
            Some(Self::with_freq(packed, freq + 1))
        } else {
            None
        }
    }

    /// Number of item slots per bucket.
    pub const NUM_ITEM_SLOTS: usize = 8;
}

impl Default for Hashbucket {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    // Mock verifier for testing
    struct MockVerifier {
        entries: Vec<(Vec<u8>, Location, bool)>, // (key, location, is_deleted)
    }

    impl MockVerifier {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
            }
        }

        fn add(&mut self, key: &[u8], location: Location, deleted: bool) {
            self.entries.push((key.to_vec(), location, deleted));
        }
    }

    impl KeyVerifier for MockVerifier {
        fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
            self.entries.iter().any(|(k, loc, deleted)| {
                k == key && *loc == location && (allow_deleted || !deleted)
            })
        }
    }

    #[test]
    fn test_pack_basic() {
        let tag = 0xABC;
        let freq = 42;
        let location = Location::new(0x123_4567_89AB);

        let packed = Hashbucket::pack(tag, freq, location);

        assert_eq!(Hashbucket::tag(packed), tag);
        assert_eq!(Hashbucket::freq(packed), freq);
        assert_eq!(Hashbucket::location(packed), location);
    }

    #[test]
    fn test_pack_max_values() {
        let tag = 0xFFF;
        let freq = 0xFF;
        let location = Location::new(Location::MAX_RAW - 1); // Not ghost

        let packed = Hashbucket::pack(tag, freq, location);

        assert_eq!(Hashbucket::tag(packed), tag);
        assert_eq!(Hashbucket::freq(packed), freq);
        assert_eq!(Hashbucket::location(packed), location);
        assert!(!Hashbucket::is_ghost(packed));
    }

    #[test]
    fn test_ghost_entries() {
        let tag = 0x123;
        let freq = 50;

        let ghost = Hashbucket::pack_ghost(tag, freq);

        assert!(Hashbucket::is_ghost(ghost));
        assert_eq!(Hashbucket::tag(ghost), tag);
        assert_eq!(Hashbucket::freq(ghost), freq);
        assert!(Hashbucket::location(ghost).is_ghost());
    }

    #[test]
    fn test_to_ghost() {
        let packed = Hashbucket::pack(0x456, 75, Location::new(1000));
        let ghost = Hashbucket::to_ghost(packed);

        assert!(Hashbucket::is_ghost(ghost));
        assert_eq!(Hashbucket::tag(ghost), 0x456);
        assert_eq!(Hashbucket::freq(ghost), 75);
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
        let mut verifier = MockVerifier::new();

        let location = Location::new(12345);
        verifier.add(b"test", location, false);

        let result = ht.insert(b"test", location, &verifier);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        let lookup = ht.lookup(b"test", &verifier);
        assert!(lookup.is_some());
        let (loc, freq) = lookup.unwrap();
        assert_eq!(loc, location);
        assert!(freq >= 1);
    }

    #[test]
    fn test_insert_if_absent() {
        let ht = CuckooHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location1 = Location::new(100);
        let location2 = Location::new(200);
        verifier.add(b"test", location1, false);
        verifier.add(b"test", location2, false);

        let result = ht.insert_if_absent(b"test", location1, &verifier);
        assert!(result.is_ok());

        let result = ht.insert_if_absent(b"test", location2, &verifier);
        assert!(matches!(result, Err(CacheError::KeyExists)));
    }

    #[test]
    fn test_update_if_present() {
        let ht = CuckooHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location1 = Location::new(100);
        let location2 = Location::new(200);
        verifier.add(b"test", location1, false);
        verifier.add(b"test", location2, false);

        // Try to update non-existent key
        let result = ht.update_if_present(b"test", location2, &verifier);
        assert!(matches!(result, Err(CacheError::KeyNotFound)));

        // Insert first
        ht.insert(b"test", location1, &verifier).unwrap();

        // Now update should work
        let result = ht.update_if_present(b"test", location2, &verifier);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), location1);
    }

    #[test]
    fn test_remove() {
        let ht = CuckooHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location = Location::new(12345);
        verifier.add(b"test", location, false);

        ht.insert(b"test", location, &verifier).unwrap();

        assert!(ht.contains(b"test", &verifier));
        assert!(ht.remove(b"test", location));
        assert!(!ht.contains(b"test", &verifier));
    }

    #[test]
    fn test_convert_to_ghost() {
        let ht = CuckooHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location = Location::new(12345);
        verifier.add(b"test", location, false);

        ht.insert(b"test", location, &verifier).unwrap();

        // Lookup to increase frequency
        for _ in 0..5 {
            ht.lookup(b"test", &verifier);
        }

        // Convert to ghost
        assert!(ht.convert_to_ghost(b"test", location));

        // Should not be found via normal lookup
        assert!(!ht.contains(b"test", &verifier));

        // But ghost frequency should be preserved
        let ghost_freq = ht.get_ghost_frequency(b"test");
        assert!(ghost_freq.is_some());
        assert!(ghost_freq.unwrap() > 1);
    }

    #[test]
    fn test_cas_location() {
        let ht = CuckooHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location1 = Location::new(100);
        let location2 = Location::new(200);
        verifier.add(b"test", location1, false);
        verifier.add(b"test", location2, false);

        ht.insert(b"test", location1, &verifier).unwrap();

        // CAS with correct old location should succeed
        assert!(ht.cas_location(b"test", location1, location2, true));

        // Verify new location
        let lookup = ht.lookup(b"test", &verifier);
        assert!(lookup.is_some());
        assert_eq!(lookup.unwrap().0, location2);
    }

    #[test]
    fn test_get_frequency() {
        let ht = CuckooHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location = Location::new(12345);
        verifier.add(b"test", location, false);

        // Key doesn't exist
        assert!(ht.get_frequency(b"test", &verifier).is_none());

        // Insert and check frequency
        ht.insert(b"test", location, &verifier).unwrap();

        let freq = ht.get_frequency(b"test", &verifier);
        assert!(freq.is_some());
        assert!(freq.unwrap() >= 1);
    }

    #[test]
    fn test_lookup_nonexistent() {
        let ht = CuckooHashtable::new(10);
        let verifier = MockVerifier::new();

        assert!(ht.lookup(b"nonexistent", &verifier).is_none());
    }

    #[test]
    fn test_contains_nonexistent() {
        let ht = CuckooHashtable::new(10);
        let verifier = MockVerifier::new();

        assert!(!ht.contains(b"nonexistent", &verifier));
    }

    #[test]
    fn test_remove_nonexistent() {
        let ht = CuckooHashtable::new(10);

        let location = Location::new(12345);
        assert!(!ht.remove(b"nonexistent", location));
    }

    #[test]
    fn test_cas_location_wrong_old() {
        let ht = CuckooHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location1 = Location::new(100);
        let wrong_old = Location::new(999);
        let new_loc = Location::new(200);
        verifier.add(b"test", location1, false);

        ht.insert(b"test", location1, &verifier).unwrap();

        // Try CAS with wrong old location
        assert!(!ht.cas_location(b"test", wrong_old, new_loc, true));
    }

    #[test]
    fn test_insert_multiple_keys() {
        let ht = CuckooHashtable::new(10);
        let mut verifier = MockVerifier::new();

        // Create multiple keys
        for i in 0..100 {
            let key = format!("key_{}", i);
            let location = Location::new(i as u64 * 1000);
            verifier.add(key.as_bytes(), location, false);
        }

        // Insert all keys
        for i in 0..100 {
            let key = format!("key_{}", i);
            let location = Location::new(i as u64 * 1000);
            let result = ht.insert(key.as_bytes(), location, &verifier);
            assert!(result.is_ok(), "Failed to insert key_{}", i);
        }

        // Verify all keys exist
        for i in 0..100 {
            let key = format!("key_{}", i);
            assert!(
                ht.contains(key.as_bytes(), &verifier),
                "key_{} not found",
                i
            );
        }
    }

    #[test]
    fn test_insert_overwrite() {
        let ht = CuckooHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location1 = Location::new(100);
        let location2 = Location::new(200);
        verifier.add(b"test", location1, false);
        verifier.add(b"test", location2, false);

        // First insert
        let result = ht.insert(b"test", location1, &verifier);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Second insert overwrites
        let result = ht.insert(b"test", location2, &verifier);
        assert!(result.is_ok());
        let old = result.unwrap();
        assert!(old.is_some());
        assert_eq!(old.unwrap(), location1);
    }

    #[test]
    fn test_hashbucket_default() {
        let bucket = Hashbucket::default();
        for slot in &bucket.items {
            assert_eq!(slot.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn test_with_freq() {
        let packed = Hashbucket::pack(0x123, 10, Location::new(100));
        let updated = Hashbucket::with_freq(packed, 50);

        assert_eq!(Hashbucket::freq(updated), 50);
        assert_eq!(Hashbucket::tag(updated), 0x123);
        assert_eq!(Hashbucket::location(updated), Location::new(100));
    }

    #[test]
    fn test_try_update_freq_max() {
        let packed = Hashbucket::pack(0x123, 127, Location::new(100));
        // At max frequency, should return None
        assert!(Hashbucket::try_update_freq(packed, 127).is_none());
    }

    #[test]
    fn test_try_update_freq_low() {
        let packed = Hashbucket::pack(0x123, 5, Location::new(100));
        // Low frequency always increments
        let result = Hashbucket::try_update_freq(packed, 5);
        assert!(result.is_some());
        let new_packed = result.unwrap();
        assert_eq!(Hashbucket::freq(new_packed), 6);
    }

    #[test]
    fn test_is_ghost_empty() {
        // Empty slot is not a ghost
        assert!(!Hashbucket::is_ghost(0));
    }

    #[test]
    fn test_is_ghost_live_entry() {
        let packed = Hashbucket::pack(0x123, 10, Location::new(100));
        assert!(!Hashbucket::is_ghost(packed));
    }

    #[test]
    fn test_ghost_resurrection() {
        let ht = CuckooHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location1 = Location::new(100);
        let location2 = Location::new(200);
        verifier.add(b"test", location1, false);
        verifier.add(b"test", location2, false);

        // Insert key
        ht.insert(b"test", location1, &verifier).unwrap();

        // Access to build up frequency
        for _ in 0..10 {
            ht.lookup(b"test", &verifier);
        }

        // Get the frequency before converting to ghost
        let freq_before = ht.get_frequency(b"test", &verifier).unwrap();
        assert!(freq_before > 1);

        // Convert to ghost
        assert!(ht.convert_to_ghost(b"test", location1));

        // Verify ghost frequency is preserved
        let ghost_freq = ht.get_ghost_frequency(b"test");
        assert!(ghost_freq.is_some());
        assert_eq!(ghost_freq.unwrap(), freq_before);

        // Insert over the ghost - frequency should be preserved
        let result = ht.insert(b"test", location2, &verifier);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // Ghost resurrection returns None

        // Verify the frequency was preserved
        let freq_after = ht.get_frequency(b"test", &verifier).unwrap();
        assert_eq!(freq_after, freq_before);
    }

    #[test]
    fn test_get_item_frequency() {
        let ht = CuckooHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location = Location::new(12345);
        verifier.add(b"test", location, false);

        // Key doesn't exist
        assert!(ht.get_item_frequency(b"test", location).is_none());

        // Insert and check frequency by exact location
        ht.insert(b"test", location, &verifier).unwrap();

        let freq = ht.get_item_frequency(b"test", location);
        assert!(freq.is_some());
        assert!(freq.unwrap() >= 1);

        // Wrong location returns None
        let wrong_location = Location::new(99999);
        assert!(ht.get_item_frequency(b"test", wrong_location).is_none());
    }

    #[test]
    fn test_deleted_entry() {
        let ht = CuckooHashtable::new(10);
        let mut verifier = MockVerifier::new();

        let location = Location::new(12345);
        verifier.add(b"test", location, true); // marked as deleted

        // Insert should still work (uses allow_deleted=true)
        ht.insert(b"test", location, &verifier).unwrap();

        // But lookup should not find it (uses allow_deleted=false)
        assert!(ht.lookup(b"test", &verifier).is_none());
        assert!(!ht.contains(b"test", &verifier));
    }
}

// -----------------------------------------------------------------------------
// Loom concurrency tests
// -----------------------------------------------------------------------------

#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use super::*;
    use crate::hashtable::Hashtable;
    use loom::sync::Arc;
    use loom::thread;

    /// Simple verifier that always returns true for testing hashtable mechanics.
    struct AlwaysVerifier;

    impl KeyVerifier for AlwaysVerifier {
        fn verify(&self, _key: &[u8], _location: Location, _allow_deleted: bool) -> bool {
            true
        }
    }

    /// Helper to call insert_if_absent on CuckooHashtable
    fn ht_insert(
        ht: &CuckooHashtable,
        key: &[u8],
        location: Location,
        verifier: &impl KeyVerifier,
    ) -> CacheResult<()> {
        <CuckooHashtable as Hashtable>::insert_if_absent(ht, key, location, verifier)
    }

    /// Helper to call lookup on CuckooHashtable
    fn ht_lookup(
        ht: &CuckooHashtable,
        key: &[u8],
        verifier: &impl KeyVerifier,
    ) -> Option<(Location, u8)> {
        <CuckooHashtable as Hashtable>::lookup(ht, key, verifier)
    }

    /// Helper to call remove on CuckooHashtable
    fn ht_remove(ht: &CuckooHashtable, key: &[u8], location: Location) -> bool {
        <CuckooHashtable as Hashtable>::remove(ht, key, location)
    }

    /// Helper to call cas_location on CuckooHashtable
    fn ht_cas(
        ht: &CuckooHashtable,
        key: &[u8],
        old_loc: Location,
        new_loc: Location,
        preserve_freq: bool,
    ) -> bool {
        <CuckooHashtable as Hashtable>::cas_location(ht, key, old_loc, new_loc, preserve_freq)
    }

    /// Helper to call get_ghost_frequency on CuckooHashtable
    fn ht_ghost_freq(ht: &CuckooHashtable, key: &[u8]) -> Option<u8> {
        <CuckooHashtable as Hashtable>::get_ghost_frequency(ht, key)
    }

    #[test]
    fn test_concurrent_insert_different_keys() {
        loom::model(|| {
            let ht = Arc::new(CuckooHashtable::new(4));
            let verifier = Arc::new(AlwaysVerifier);

            let ht1 = ht.clone();
            let v1 = verifier.clone();
            let t1 = thread::spawn(move || {
                let loc = Location::new(1);
                let _ = ht_insert(&ht1, b"key1", loc, &*v1);
            });

            let ht2 = ht.clone();
            let v2 = verifier.clone();
            let t2 = thread::spawn(move || {
                let loc = Location::new(2);
                let _ = ht_insert(&ht2, b"key2", loc, &*v2);
            });

            t1.join().unwrap();
            t2.join().unwrap();

            // Both keys should be present (or one may fail due to full bucket)
            let found1 = ht_lookup(&ht, b"key1", &*verifier).is_some();
            let found2 = ht_lookup(&ht, b"key2", &*verifier).is_some();

            // At least one should succeed
            assert!(found1 || found2);
        });
    }

    #[test]
    fn test_concurrent_insert_same_key() {
        loom::model(|| {
            let ht = Arc::new(CuckooHashtable::new(4));
            let verifier = Arc::new(AlwaysVerifier);

            let ht1 = ht.clone();
            let v1 = verifier.clone();
            let t1 = thread::spawn(move || {
                let loc = Location::new(1);
                ht_insert(&ht1, b"key", loc, &*v1)
            });

            let ht2 = ht.clone();
            let v2 = verifier.clone();
            let t2 = thread::spawn(move || {
                let loc = Location::new(2);
                ht_insert(&ht2, b"key", loc, &*v2)
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Exactly one should succeed, one should fail with KeyExists
            let successes = [r1.is_ok(), r2.is_ok()].iter().filter(|&&x| x).count();
            assert_eq!(successes, 1, "Exactly one insert should succeed");
        });
    }

    #[test]
    fn test_concurrent_lookup_frequency_update() {
        loom::model(|| {
            let ht = Arc::new(CuckooHashtable::new(4));
            let verifier = Arc::new(AlwaysVerifier);

            // Insert a key first
            let loc = Location::new(42);
            ht_insert(&ht, b"key", loc, &*verifier).unwrap();

            let ht1 = ht.clone();
            let v1 = verifier.clone();
            let t1 = thread::spawn(move || ht_lookup(&ht1, b"key", &*v1));

            let ht2 = ht.clone();
            let v2 = verifier.clone();
            let t2 = thread::spawn(move || ht_lookup(&ht2, b"key", &*v2));

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Both lookups should find the key
            assert!(r1.is_some());
            assert!(r2.is_some());

            // Both should return the same location
            assert_eq!(r1.unwrap().0, loc);
            assert_eq!(r2.unwrap().0, loc);
        });
    }

    #[test]
    fn test_concurrent_insert_and_remove() {
        loom::model(|| {
            let ht = Arc::new(CuckooHashtable::new(4));
            let verifier = Arc::new(AlwaysVerifier);

            // Insert a key first
            let loc = Location::new(42);
            ht_insert(&ht, b"key", loc, &*verifier).unwrap();

            let ht1 = ht.clone();
            let t1 = thread::spawn(move || ht_remove(&ht1, b"key", loc));

            let ht2 = ht.clone();
            let v2 = verifier.clone();
            let t2 = thread::spawn(move || {
                let new_loc = Location::new(99);
                ht_insert(&ht2, b"key2", new_loc, &*v2)
            });

            let removed = t1.join().unwrap();
            let _ = t2.join().unwrap();

            // Remove should have succeeded
            assert!(removed);

            // Original key should be gone (or converted to ghost)
            let lookup = ht_lookup(&ht, b"key", &*verifier);
            assert!(lookup.is_none() || ht_ghost_freq(&ht, b"key").is_some());
        });
    }

    #[test]
    fn test_concurrent_cas_operations() {
        loom::model(|| {
            let ht = Arc::new(CuckooHashtable::new(4));
            let verifier = Arc::new(AlwaysVerifier);

            // Insert a key first
            let loc1 = Location::new(1);
            ht_insert(&ht, b"key", loc1, &*verifier).unwrap();

            let ht1 = ht.clone();
            let t1 = thread::spawn(move || {
                let loc2 = Location::new(2);
                ht_cas(&ht1, b"key", loc1, loc2, true)
            });

            let ht2 = ht.clone();
            let t2 = thread::spawn(move || {
                let loc3 = Location::new(3);
                ht_cas(&ht2, b"key", loc1, loc3, true)
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Exactly one CAS should succeed
            let successes = [r1, r2].iter().filter(|&&x| x).count();
            assert_eq!(successes, 1, "Exactly one CAS should succeed");

            // The key should now point to either loc2 or loc3
            let lookup = ht_lookup(&ht, b"key", &*verifier);
            assert!(lookup.is_some());
            let final_loc = lookup.unwrap().0;
            assert!(final_loc == Location::new(2) || final_loc == Location::new(3));
        });
    }

    #[test]
    fn test_bucket_slot_cas_contention() {
        loom::model(|| {
            // Test direct slot CAS operations
            let bucket = Hashbucket::new();
            let slot = &bucket.items[0];

            let slot_ptr = slot as *const AtomicU64 as usize;

            let t1 = thread::spawn(move || {
                let slot = unsafe { &*(slot_ptr as *const AtomicU64) };
                let packed = Hashbucket::pack(0x123, 1, Location::new(1));
                slot.compare_exchange(0, packed, Ordering::Release, Ordering::Acquire)
            });

            let t2 = thread::spawn(move || {
                let slot = unsafe { &*(slot_ptr as *const AtomicU64) };
                let packed = Hashbucket::pack(0x456, 1, Location::new(2));
                slot.compare_exchange(0, packed, Ordering::Release, Ordering::Acquire)
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Exactly one should succeed (starting from 0)
            let successes = [r1.is_ok(), r2.is_ok()].iter().filter(|&&x| x).count();
            assert_eq!(successes, 1, "Exactly one CAS from 0 should succeed");
        });
    }
}
