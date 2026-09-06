//! Key verification for hashtable lookups.
//!
//! The verifier confirms that a key exists at a given location,
//! resolving potential tag collisions in the hashtable.
//!
//! This module provides two verifiers:
//! - `SlabVerifier`: RAM-only verification using the slab allocator
//! - `SlabTieredVerifier`: Multi-tier verification supporting both RAM and disk

use cache_core::disk::FilePool;
use cache_core::{ItemLocation, KeyVerifier, Location, RamPool, SegmentKeyVerify};

use crate::allocator::SlabAllocator;
use crate::location::SlabLocation;

/// Verifier for slab-based storage.
///
/// Implements the `KeyVerifier` trait to verify that a key exists
/// at a given slab location.
pub struct SlabVerifier<'a> {
    allocator: &'a SlabAllocator,
    /// Allow expired items (for cleanup operations).
    allow_expired: bool,
}

impl<'a> SlabVerifier<'a> {
    /// Create a new verifier.
    pub fn new(allocator: &'a SlabAllocator) -> Self {
        Self {
            allocator,
            allow_expired: false,
        }
    }

    /// Create a verifier that allows expired items.
    ///
    /// This is used for cleanup operations where we need to find
    /// expired items to remove them.
    pub fn allowing_expired(allocator: &'a SlabAllocator) -> Self {
        Self {
            allocator,
            allow_expired: true,
        }
    }
}

impl KeyVerifier for SlabVerifier<'_> {
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        // Don't verify ghost entries
        if location.is_ghost() {
            return false;
        }

        let slab_loc = SlabLocation::from_location(location);
        let (class_id, slab_id, slot_index) = slab_loc.unpack();

        // Get the class
        let class = match self.allocator.class(class_id) {
            Some(c) => c,
            None => return false,
        };

        // Check if slab is live (not evicted).
        // This is crucial: slab_count() returns the total slabs ever added,
        // not the currently live slabs. An evicted slab would pass the
        // slab_count() check but have a null pointer, causing a segfault.
        if !class.is_slab_live(slab_id) {
            return false;
        }

        // Get the header and verify
        unsafe {
            let header = class.header(slab_id, slot_index);

            // Check deleted flag
            if !allow_deleted && header.is_deleted() {
                return false;
            }

            // Check expiration
            if !self.allow_expired && header.is_expired() {
                return false;
            }

            // Compare key
            let stored_key = header.key();
            stored_key == key
        }
    }

    fn prefetch(&self, location: Location) {
        if location.is_ghost() {
            return;
        }

        let slab_loc = SlabLocation::from_location(location);
        let (class_id, slab_id, slot_index) = slab_loc.unpack();

        // Check if slab is live before accessing its memory
        if let Some(class) = self.allocator.class(class_id)
            && class.is_slab_live(slab_id)
        {
            // SAFETY: slab is live, so the pointer is valid
            unsafe {
                let ptr = class.slot_ptr(slab_id, slot_index);
                // Prefetch the header and likely the key
                #[cfg(target_arch = "x86_64")]
                {
                    std::arch::x86_64::_mm_prefetch(
                        ptr as *const i8,
                        std::arch::x86_64::_MM_HINT_T0,
                    );
                }
                // Note: ARM prefetch requires nightly, so we skip it for now
                #[cfg(not(target_arch = "x86_64"))]
                {
                    let _ = ptr; // suppress unused warning
                }
            }
        }
    }
}

/// Tiered verifier that supports both RAM (slab) and disk storage.
///
/// This verifier dispatches to the appropriate storage backend based on
/// the pool_id encoded in the location:
/// - RAM pool (pool_id = ram_pool_id): Uses SlabLocation encoding
/// - Disk pool (pool_id = disk_pool_id): Uses ItemLocation encoding
pub struct SlabTieredVerifier<'a> {
    allocator: &'a SlabAllocator,
    ram_pool_id: u8,
    disk_pool: Option<&'a FilePool>,
    disk_pool_id: u8,
    allow_expired: bool,
}

impl<'a> SlabTieredVerifier<'a> {
    /// Create a new tiered verifier with only RAM storage.
    pub fn new(allocator: &'a SlabAllocator, ram_pool_id: u8) -> Self {
        Self {
            allocator,
            ram_pool_id,
            disk_pool: None,
            disk_pool_id: 2,
            allow_expired: false,
        }
    }

    /// Create a new tiered verifier with both RAM and disk storage.
    pub fn with_disk(
        allocator: &'a SlabAllocator,
        ram_pool_id: u8,
        disk_pool: &'a FilePool,
        disk_pool_id: u8,
    ) -> Self {
        Self {
            allocator,
            ram_pool_id,
            disk_pool: Some(disk_pool),
            disk_pool_id,
            allow_expired: false,
        }
    }

    /// Create a verifier that allows expired items.
    #[allow(dead_code)]
    pub fn allowing_expired(mut self) -> Self {
        self.allow_expired = true;
        self
    }

    /// Verify a key in RAM storage.
    fn verify_ram(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        let slab_loc = SlabLocation::from_location(location);
        let (class_id, slab_id, slot_index) = slab_loc.unpack();

        // Get the class
        let class = match self.allocator.class(class_id) {
            Some(c) => c,
            None => return false,
        };

        // Check if slab is live (not evicted).
        // This is crucial: slab_count() returns the total slabs ever added,
        // not the currently live slabs. An evicted slab would pass the
        // slab_count() check but have a null pointer, causing a segfault.
        if !class.is_slab_live(slab_id) {
            return false;
        }

        // Get the header and verify
        unsafe {
            let header = class.header(slab_id, slot_index);

            // Check deleted flag
            if !allow_deleted && header.is_deleted() {
                return false;
            }

            // Check expiration
            if !self.allow_expired && header.is_expired() {
                return false;
            }

            // Compare key
            let stored_key = header.key();
            stored_key == key
        }
    }

    /// Verify a key in disk storage.
    fn verify_disk(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        let disk_pool = match self.disk_pool {
            Some(pool) => pool,
            None => return false,
        };

        let item_loc = ItemLocation::from_location(location);
        // The disk pool's own layout: pools with different segment sizes split
        // the location bits differently.
        let (_, segment_id, incarnation, offset) = item_loc.unpack(disk_pool.layout());
        let segment = match disk_pool.get(segment_id) {
            Some(s) => s,
            None => return false,
        };

        // A location naming a previous incarnation of this segment is not ours
        // to resolve: the segment was drained and refilled, and this offset now
        // holds a different item. Checked before touching any item bytes.
        if segment.incarnation() != incarnation {
            return false;
        }

        segment.verify_key_at_offset(offset, key, allow_deleted)
    }
}

impl KeyVerifier for SlabTieredVerifier<'_> {
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        // Don't verify ghost entries
        if location.is_ghost() {
            return false;
        }

        // Extract pool_id from the location (top 2 bits)
        let pool_id = SlabLocation::pool_id_from_location(location);

        if pool_id == self.ram_pool_id {
            self.verify_ram(key, location, allow_deleted)
        } else if pool_id == self.disk_pool_id {
            self.verify_disk(key, location, allow_deleted)
        } else {
            // Unknown pool
            false
        }
    }

    fn prefetch(&self, location: Location) {
        if location.is_ghost() {
            return;
        }

        let pool_id = SlabLocation::pool_id_from_location(location);

        if pool_id == self.ram_pool_id {
            let slab_loc = SlabLocation::from_location(location);
            let (class_id, slab_id, slot_index) = slab_loc.unpack();

            // Check if slab is live before accessing its memory
            if let Some(class) = self.allocator.class(class_id)
                && class.is_slab_live(slab_id)
            {
                unsafe {
                    let ptr = class.slot_ptr(slab_id, slot_index);
                    #[cfg(target_arch = "x86_64")]
                    {
                        std::arch::x86_64::_mm_prefetch(
                            ptr as *const i8,
                            std::arch::x86_64::_MM_HINT_T0,
                        );
                    }
                    #[cfg(not(target_arch = "x86_64"))]
                    {
                        let _ = ptr;
                    }
                }
            }
        }
        // No prefetch for disk - I/O latency dominates anyway
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SlabCacheConfig;
    use cache_core::HugepageSize;
    use std::time::Duration;

    fn test_config() -> SlabCacheConfig {
        SlabCacheConfig {
            heap_size: 1024 * 1024, // 1MB
            slab_size: 64 * 1024,   // 64KB slabs
            hugepage_size: HugepageSize::None,
            ..Default::default()
        }
    }

    /// A disk location from a previous incarnation must not resolve, even
    /// though the key really is at that offset.
    ///
    /// That is the whole hazard: segments are append-only from a fixed start,
    /// so the n-th item of the new incarnation lands where the n-th item of the
    /// old one was. The key compare alone says yes; only the tag says no.
    #[test]
    fn test_tiered_verifier_rejects_a_stale_disk_incarnation() {
        use cache_core::{FilePoolBuilder, ItemLocation, Segment, State};

        let dir = tempfile::tempdir().expect("temp dir");
        let disk_pool = FilePoolBuilder::new(2)
            .path(dir.path().join("disk.dat"))
            .segment_size(64 * 1024)
            .size(256 * 1024)
            .build()
            .expect("disk pool");

        // Age every segment through a used incarnation, so the tag on the item
        // written below is non-zero and a predecessor tag exists.
        for _ in 0..disk_pool.segment_count() {
            let id = disk_pool.reserve().expect("free segment");
            let segment = disk_pool.get(id).expect("segment");
            assert!(segment.cas_metadata(State::Reserved, State::Locked, None, None));
            disk_pool.release(id);
        }

        let id = disk_pool.reserve().expect("free segment");
        let segment = disk_pool.get(id).expect("segment");
        assert!(segment.cas_metadata(State::Reserved, State::Live, None, None));
        let offset = segment.append_item(b"key", b"value", &[]).expect("append");
        let tag = segment.incarnation();
        assert_ne!(tag, 0, "the segment must be past its first incarnation");

        let layout = *disk_pool.layout();
        let live = ItemLocation::new(&layout, 2, id, tag, offset);
        let stale = ItemLocation::new(&layout, 2, id, tag - 1, offset);

        let allocator = SlabAllocator::new(&test_config()).expect("allocator");
        let verifier = SlabTieredVerifier::with_disk(&allocator, 0, &disk_pool, 2);
        assert!(
            verifier.verify(b"key", live.to_location(), false),
            "the current incarnation must resolve"
        );
        assert!(
            !verifier.verify(b"key", stale.to_location(), false),
            "a location from a previous incarnation must not resolve"
        );
    }

    #[test]
    fn test_verifier_basic() {
        use crate::config::HEADER_SIZE;

        let config = test_config();
        let allocator = SlabAllocator::new(&config).unwrap();

        let key = b"test_key";
        let value = b"test_value";
        let item_size = HEADER_SIZE + key.len() + value.len();
        let class_id = allocator.select_class(item_size).unwrap();

        let (slab_id, slot_index) = allocator.allocate(class_id).unwrap();

        unsafe {
            allocator.write_item(
                class_id,
                slab_id,
                slot_index,
                key,
                value,
                Duration::from_secs(3600),
            );
        }

        let location = SlabLocation::new(class_id, slab_id, slot_index).to_location();
        let verifier = allocator.verifier();

        // Should verify correct key
        assert!(verifier.verify(key, location, false));

        // Should not verify wrong key
        assert!(!verifier.verify(b"wrong_key", location, false));
    }

    #[test]
    fn test_verifier_deleted() {
        use crate::config::HEADER_SIZE;

        let config = test_config();
        let allocator = SlabAllocator::new(&config).unwrap();

        let key = b"test_key";
        let value = b"test_value";
        let item_size = HEADER_SIZE + key.len() + value.len();
        let class_id = allocator.select_class(item_size).unwrap();

        let (slab_id, slot_index) = allocator.allocate(class_id).unwrap();

        unsafe {
            allocator.write_item(
                class_id,
                slab_id,
                slot_index,
                key,
                value,
                Duration::from_secs(3600),
            );
        }

        let location = SlabLocation::new(class_id, slab_id, slot_index).to_location();
        let verifier = allocator.verifier();

        // Mark as deleted
        unsafe {
            let header = allocator.header(SlabLocation::from_location(location));
            header.mark_deleted();
        }

        // Should not verify when allow_deleted is false
        assert!(!verifier.verify(key, location, false));

        // Should verify when allow_deleted is true
        assert!(verifier.verify(key, location, true));
    }

    #[test]
    fn test_verifier_ghost() {
        let config = test_config();
        let allocator = SlabAllocator::new(&config).unwrap();
        let verifier = allocator.verifier();

        // Ghost location should not verify
        assert!(!verifier.verify(b"any_key", Location::GHOST, false));
        assert!(!verifier.verify(b"any_key", Location::GHOST, true));
    }
}
