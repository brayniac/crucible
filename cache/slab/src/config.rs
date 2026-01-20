//! Configuration types for the slab cache.

use cache_core::HugepageSize;
use std::time::Duration;

/// Eviction strategy flags (twemcache-style).
///
/// Strategies can be combined (stacked) using bitwise OR. When eviction is needed,
/// strategies are tried in order from highest to lowest bit:
/// - `SLAB_LRC` (8) - Least Recently Created slab
/// - `SLAB_LRA` (4) - Least Recently Accessed slab
/// - `RANDOM` (2) - Random slab
///
/// For example, `SLAB_LRA | RANDOM` (6) means: try slab LRA first, fall back to random.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvictionStrategy(u8);

impl EvictionStrategy {
    /// No eviction - return error when full.
    pub const NONE: Self = Self(0);
    /// Random slab eviction - evict all items from a randomly chosen slab.
    pub const RANDOM: Self = Self(2);
    /// Slab LRA eviction - evict all items from the least recently accessed slab.
    pub const SLAB_LRA: Self = Self(4);
    /// Slab LRC eviction - evict all items from the least recently created slab.
    pub const SLAB_LRC: Self = Self(8);

    /// Default strategy: slab LRA (evict least recently accessed slab).
    pub const DEFAULT: Self = Self::SLAB_LRA;

    /// Create from raw bits.
    #[inline]
    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    /// Get the raw bits.
    #[inline]
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Check if a specific strategy is enabled.
    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Combine two strategies.
    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Check if no eviction is configured.
    #[inline]
    pub const fn is_none(self) -> bool {
        self.0 == 0
    }

    /// Check if any slab-level eviction is enabled.
    #[inline]
    pub const fn has_slab_eviction(self) -> bool {
        (self.0 & (Self::RANDOM.0 | Self::SLAB_LRA.0 | Self::SLAB_LRC.0)) != 0
    }
}

impl Default for EvictionStrategy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl std::ops::BitOr for EvictionStrategy {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

/// Default slab size (1MB).
pub const DEFAULT_SLAB_SIZE: usize = 1024 * 1024;

/// Default heap size (64MB).
pub const DEFAULT_HEAP_SIZE: usize = 64 * 1024 * 1024;

/// Default hashtable power (2^16 = 64K buckets).
pub const DEFAULT_HASHTABLE_POWER: u8 = 16;

/// Default TTL (1 hour).
pub const DEFAULT_TTL: Duration = Duration::from_secs(3600);

/// Header size in bytes (12 bytes).
pub const HEADER_SIZE: usize = 12;

/// Default minimum slot size (64 bytes).
pub const DEFAULT_MIN_SLOT_SIZE: usize = 64;

/// Default growth factor (~1.25x).
pub const DEFAULT_GROWTH_FACTOR: f64 = 1.25;

/// Alignment for slot sizes (8 bytes for 64-bit safety).
const SLOT_ALIGNMENT: usize = 8;

/// Round up to alignment.
#[inline]
const fn align_up(size: usize, align: usize) -> usize {
    (size + align - 1) & !(align - 1)
}

/// Generate slab class sizes from min to max with the given growth factor.
///
/// Returns a vector of slot sizes, each approximately `growth_factor` times
/// larger than the previous. The last class will be <= `max_size`.
/// All sizes are aligned to 8 bytes for proper atomic access.
pub fn generate_slab_classes(min_size: usize, max_size: usize, growth_factor: f64) -> Vec<usize> {
    assert!(min_size > 0, "min_size must be > 0");
    assert!(max_size >= min_size, "max_size must be >= min_size");
    assert!(growth_factor > 1.0, "growth_factor must be > 1.0");

    let mut classes = Vec::new();
    let mut size = align_up(min_size, SLOT_ALIGNMENT);

    while size <= max_size {
        classes.push(size);
        // Calculate next size, ensuring we make progress
        let next_raw = ((size as f64) * growth_factor).ceil() as usize;
        let next = align_up(next_raw, SLOT_ALIGNMENT);
        if next <= size {
            size += SLOT_ALIGNMENT; // Ensure forward progress
        } else {
            size = next;
        }
    }

    classes
}

/// Builder for SlabCache configuration.
#[derive(Debug, Clone)]
pub struct SlabCacheConfig {
    /// Total heap size in bytes.
    pub heap_size: usize,
    /// Slab size in bytes (all slabs are the same size).
    pub slab_size: usize,
    /// Minimum slot size (smallest class).
    pub min_slot_size: usize,
    /// Growth factor between classes (~1.25 recommended).
    pub growth_factor: f64,
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
    /// Eviction strategy (twemcache-style).
    pub eviction_strategy: EvictionStrategy,
}

impl Default for SlabCacheConfig {
    fn default() -> Self {
        Self {
            heap_size: DEFAULT_HEAP_SIZE,
            slab_size: DEFAULT_SLAB_SIZE,
            min_slot_size: DEFAULT_MIN_SLOT_SIZE,
            growth_factor: DEFAULT_GROWTH_FACTOR,
            hashtable_power: DEFAULT_HASHTABLE_POWER,
            hugepage_size: HugepageSize::None,
            numa_node: None,
            default_ttl: DEFAULT_TTL,
            enable_ghosts: false,
            eviction_strategy: EvictionStrategy::DEFAULT,
        }
    }
}

impl SlabCacheConfig {
    /// Generate the slab classes for this configuration.
    ///
    /// Classes range from `min_slot_size` up to `slab_size` with
    /// approximately `growth_factor` between each class.
    pub fn generate_classes(&self) -> Vec<usize> {
        generate_slab_classes(self.min_slot_size, self.slab_size, self.growth_factor)
    }

    /// Calculate the number of slabs that can be allocated.
    pub fn slab_count(&self) -> usize {
        self.heap_size / self.slab_size
    }

    /// Calculate slots per slab for a given slot size.
    pub fn slots_per_slab_for_size(&self, slot_size: usize) -> usize {
        self.slab_size / slot_size
    }
}

/// Runtime slab class information.
///
/// This is generated from `SlabCacheConfig` and stored in the allocator.
#[derive(Debug, Clone)]
pub struct SlabClasses {
    /// Slot sizes for each class.
    sizes: Vec<usize>,
}

impl SlabClasses {
    /// Create slab classes from configuration.
    pub fn from_config(config: &SlabCacheConfig) -> Self {
        Self {
            sizes: config.generate_classes(),
        }
    }

    /// Create slab classes from explicit sizes (for testing).
    pub fn from_sizes(sizes: Vec<usize>) -> Self {
        Self { sizes }
    }

    /// Get the number of classes.
    #[inline]
    pub fn len(&self) -> usize {
        self.sizes.len()
    }

    /// Check if there are no classes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.sizes.is_empty()
    }

    /// Get the slot size for a class.
    #[inline]
    pub fn slot_size(&self, class_id: u8) -> Option<usize> {
        self.sizes.get(class_id as usize).copied()
    }

    /// Get all slot sizes.
    #[inline]
    pub fn sizes(&self) -> &[usize] {
        &self.sizes
    }

    /// Find the smallest class that fits an item of the given size.
    ///
    /// Returns `None` if the item is too large for any class.
    #[inline]
    pub fn select_class(&self, item_size: usize) -> Option<u8> {
        match self.sizes.binary_search(&item_size) {
            Ok(idx) => Some(idx as u8),
            Err(idx) => {
                if idx < self.sizes.len() {
                    Some(idx as u8)
                } else {
                    None
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_classes_basic() {
        let classes = generate_slab_classes(64, 1024, 2.0);
        // 64, 128, 256, 512, 1024 (all already 8-byte aligned)
        assert_eq!(classes, vec![64, 128, 256, 512, 1024]);
    }

    #[test]
    fn test_generate_classes_1_25x() {
        let classes = generate_slab_classes(64, 256, 1.25);
        assert!(classes.len() >= 4);
        assert_eq!(classes[0], 64);
        assert!(*classes.last().unwrap() <= 256);

        // All sizes must be 8-byte aligned
        for &size in &classes {
            assert_eq!(size % 8, 0, "size {} not 8-byte aligned", size);
        }

        // Check growth is approximately 1.25x (may be higher due to alignment)
        for i in 1..classes.len() {
            let ratio = classes[i] as f64 / classes[i - 1] as f64;
            assert!(
                ratio >= 1.1 && ratio <= 1.5,
                "ratio {} at index {}",
                ratio,
                i
            );
        }
    }

    #[test]
    fn test_generate_classes_large() {
        // Generate classes up to 16MB
        let classes = generate_slab_classes(64, 16 * 1024 * 1024, 1.25);
        assert!(classes.len() > 40);
        assert_eq!(classes[0], 64);
        assert!(*classes.last().unwrap() <= 16 * 1024 * 1024);
    }

    #[test]
    fn test_slab_classes_select() {
        let classes = SlabClasses::from_sizes(vec![64, 128, 256, 512, 1024]);

        // Exact match
        assert_eq!(classes.select_class(64), Some(0));
        assert_eq!(classes.select_class(256), Some(2));
        assert_eq!(classes.select_class(1024), Some(4));

        // Round up
        assert_eq!(classes.select_class(65), Some(1));
        assert_eq!(classes.select_class(127), Some(1));
        assert_eq!(classes.select_class(129), Some(2));

        // Too large
        assert_eq!(classes.select_class(1025), None);

        // Small
        assert_eq!(classes.select_class(1), Some(0));
    }

    #[test]
    fn test_slab_classes_from_config() {
        let config = SlabCacheConfig {
            slab_size: 1024,
            min_slot_size: 64,
            growth_factor: 2.0,
            ..Default::default()
        };
        let classes = SlabClasses::from_config(&config);
        // 64, 128, 256, 512, 1024 (all 8-byte aligned, 2x growth)
        assert_eq!(classes.sizes(), &[64, 128, 256, 512, 1024]);
    }

    #[test]
    fn test_alignment() {
        // Verify all generated classes are 8-byte aligned
        let classes = generate_slab_classes(64, 1024 * 1024, 1.25);
        for &size in &classes {
            assert_eq!(size % 8, 0, "size {} not 8-byte aligned", size);
        }
    }

    #[test]
    fn test_config_defaults() {
        let config = SlabCacheConfig::default();
        assert_eq!(config.heap_size, DEFAULT_HEAP_SIZE);
        assert_eq!(config.slab_size, DEFAULT_SLAB_SIZE);
        assert_eq!(config.min_slot_size, DEFAULT_MIN_SLOT_SIZE);
        assert!((config.growth_factor - DEFAULT_GROWTH_FACTOR).abs() < 0.01);
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
    fn test_config_generate_classes_default() {
        let config = SlabCacheConfig::default();
        let classes = config.generate_classes();

        // Should have many classes from 64 to 1MB
        assert!(classes.len() > 30);
        assert_eq!(classes[0], 64);
        assert!(*classes.last().unwrap() <= config.slab_size);
    }

    #[test]
    fn test_config_generate_classes_large_slab() {
        let config = SlabCacheConfig {
            slab_size: 16 * 1024 * 1024, // 16MB
            ..Default::default()
        };
        let classes = config.generate_classes();

        // Should have classes up to 16MB
        assert!(*classes.last().unwrap() <= 16 * 1024 * 1024);
        assert!(*classes.last().unwrap() > 8 * 1024 * 1024); // Should get close to max
    }
}
