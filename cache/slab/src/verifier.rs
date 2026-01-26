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

        // Check if slab exists
        if slab_id as usize >= class.slab_count() {
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

        if let Some(class) = self.allocator.class(class_id)
            && (slab_id as usize) < class.slab_count()
        {
            // SAFETY: slab_id and slot_index are validated above
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

        // Check if slab exists
        if slab_id as usize >= class.slab_count() {
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
        let segment = match disk_pool.get(item_loc.segment_id()) {
            Some(s) => s,
            None => return false,
        };

        segment.verify_key_at_offset(item_loc.offset(), key, allow_deleted)
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

            if let Some(class) = self.allocator.class(class_id)
                && (slab_id as usize) < class.slab_count()
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
