//! Configuration types for the slab cache.

use cache_core::HugepageSize;
use std::time::Duration;

/// Default slab size (1MB).
pub const DEFAULT_SLAB_SIZE: usize = 1024 * 1024;

/// Default heap size (64MB).
pub const DEFAULT_HEAP_SIZE: usize = 64 * 1024 * 1024;

/// Default hashtable power (2^16 = 64K buckets).
pub const DEFAULT_HASHTABLE_POWER: u8 = 16;

/// Default TTL (1 hour).
pub const DEFAULT_TTL: Duration = Duration::from_secs(3600);

/// Header size in bytes (24 bytes).
pub const HEADER_SIZE: usize = 24;

/// Slab class sizes (memcached-style, ~1.25x growth factor).
///
/// From 64 bytes to 1MB, with ~1.25x growth factor.
/// Each size includes the header overhead.
pub const SLAB_CLASSES: &[usize] = &[
    64,      // 0
    80,      // 1
    100,     // 2
    128,     // 3
    160,     // 4
    200,     // 5
    256,     // 6
    320,     // 7
    400,     // 8
    512,     // 9
    640,     // 10
    800,     // 11
    1024,    // 12: 1KB
    1280,    // 13
    1600,    // 14
    2048,    // 15: 2KB
    2560,    // 16
    3200,    // 17
    4096,    // 18: 4KB
    5120,    // 19
    6400,    // 20
    8192,    // 21: 8KB
    10240,   // 22
    12800,   // 23
    16384,   // 24: 16KB
    20480,   // 25
    25600,   // 26
    32768,   // 27: 32KB
    40960,   // 28
    51200,   // 29
    65536,   // 30: 64KB
    81920,   // 31
    102400,  // 32: 100KB
    131072,  // 33: 128KB
    163840,  // 34
    204800,  // 35: 200KB
    262144,  // 36: 256KB
    327680,  // 37
    409600,  // 38: 400KB
    524288,  // 39: 512KB
    655360,  // 40
    819200,  // 41: 800KB
    1048576, // 42: 1MB
];

/// Find the smallest slab class that fits an item of the given size.
///
/// The item_size should include key + value + header overhead.
/// Returns `None` if the item is too large for any class.
#[inline]
pub fn select_class(item_size: usize) -> Option<u8> {
    // Binary search for the smallest class >= item_size
    match SLAB_CLASSES.binary_search(&item_size) {
        Ok(idx) => Some(idx as u8),
        Err(idx) => {
            if idx < SLAB_CLASSES.len() {
                Some(idx as u8)
            } else {
                None // Too large for any class
            }
        }
    }
}

/// Get the slot size for a given class ID.
#[inline]
pub fn slot_size(class_id: u8) -> Option<usize> {
    SLAB_CLASSES.get(class_id as usize).copied()
}

/// Builder for SlabCache configuration.
#[derive(Debug, Clone)]
pub struct SlabCacheConfig {
    /// Total heap size in bytes.
    pub heap_size: usize,
    /// Slab size in bytes (all slabs are the same size).
    pub slab_size: usize,
    /// Hashtable power (2^power buckets).
    pub hashtable_power: u8,
    /// Hugepage size preference.
    pub hugepage_size: HugepageSize,
    /// NUMA node to bind memory to (Linux only).
    pub numa_node: Option<u32>,
    /// Default TTL for items.
    pub default_ttl: Duration,
    /// Enable ghost entries for evicted items.
    pub enable_ghosts: bool,
}

impl Default for SlabCacheConfig {
    fn default() -> Self {
        Self {
            heap_size: DEFAULT_HEAP_SIZE,
            slab_size: DEFAULT_SLAB_SIZE,
            hashtable_power: DEFAULT_HASHTABLE_POWER,
            hugepage_size: HugepageSize::None,
            numa_node: None,
            default_ttl: DEFAULT_TTL,
            enable_ghosts: false,
        }
    }
}

impl SlabCacheConfig {
    /// Calculate the number of slabs that can be allocated.
    pub fn slab_count(&self) -> usize {
        self.heap_size / self.slab_size
    }

    /// Calculate slots per slab for a given class.
    pub fn slots_per_slab(&self, class_id: u8) -> Option<usize> {
        let slot_sz = slot_size(class_id)?;
        Some(self.slab_size / slot_sz)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_class_exact() {
        assert_eq!(select_class(64), Some(0));
        assert_eq!(select_class(1024), Some(12));
        assert_eq!(select_class(1048576), Some(42));
    }

    #[test]
    fn test_select_class_between() {
        // Between 64 and 80 -> should round up to 80
        assert_eq!(select_class(65), Some(1));
        assert_eq!(select_class(79), Some(1));
        // Between 1024 and 1280
        assert_eq!(select_class(1100), Some(13));
    }

    #[test]
    fn test_select_class_too_large() {
        // Larger than 1MB
        assert_eq!(select_class(1048577), None);
        assert_eq!(select_class(2_000_000), None);
    }

    #[test]
    fn test_select_class_small() {
        // Smaller than minimum class
        assert_eq!(select_class(1), Some(0));
        assert_eq!(select_class(63), Some(0));
    }

    #[test]
    fn test_slot_size() {
        assert_eq!(slot_size(0), Some(64));
        assert_eq!(slot_size(12), Some(1024));
        assert_eq!(slot_size(42), Some(1048576));
        assert_eq!(slot_size(43), None);
    }

    #[test]
    fn test_config_defaults() {
        let config = SlabCacheConfig::default();
        assert_eq!(config.heap_size, DEFAULT_HEAP_SIZE);
        assert_eq!(config.slab_size, DEFAULT_SLAB_SIZE);
        assert_eq!(config.hashtable_power, DEFAULT_HASHTABLE_POWER);
    }

    #[test]
    fn test_config_slab_count() {
        let config = SlabCacheConfig {
            heap_size: 64 * 1024 * 1024, // 64MB
            slab_size: 1024 * 1024,      // 1MB
            ..Default::default()
        };
        assert_eq!(config.slab_count(), 64);
    }

    #[test]
    fn test_config_slots_per_slab() {
        let config = SlabCacheConfig {
            slab_size: 1024 * 1024, // 1MB
            ..Default::default()
        };
        // Class 0 = 64 byte slots -> 1MB / 64 = 16384 slots
        assert_eq!(config.slots_per_slab(0), Some(16384));
        // Class 12 = 1024 byte slots -> 1MB / 1024 = 1024 slots
        assert_eq!(config.slots_per_slab(12), Some(1024));
    }

    #[test]
    fn test_slab_classes_growth() {
        // Verify roughly 1.25x growth between adjacent classes
        for i in 1..SLAB_CLASSES.len() {
            let ratio = SLAB_CLASSES[i] as f64 / SLAB_CLASSES[i - 1] as f64;
            // Allow 1.2x to 1.35x range
            assert!(
                (1.15..=1.35).contains(&ratio),
                "Class {} ({}) to {} ({}): ratio {:.3}",
                i - 1,
                SLAB_CLASSES[i - 1],
                i,
                SLAB_CLASSES[i],
                ratio
            );
        }
    }

    #[test]
    fn test_num_classes() {
        // Should have 43 classes (0-42)
        assert_eq!(SLAB_CLASSES.len(), 43);
    }
}
