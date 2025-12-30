//! Hashtable trait for cache operations.
//!
//! This module provides:
//! - [`Hashtable`] - Core trait for key -> (location, frequency) mapping
//! - [`SegmentProvider`] - Trait for segment access by pool/segment ID
//! - Support for ghost entries that preserve frequency after eviction

use crate::error::CacheResult;
use crate::location::ItemLocation;
use crate::segment::SegmentKeyVerify;

/// Core trait for hashtable operations.
///
/// A hashtable maps keys to `ItemLocation` values, tracking the physical
/// location of items in segment pools. It also maintains frequency counters
/// for each item, supporting eviction algorithms like S3FIFO.
///
/// # Ghost Entries
///
/// When an item is evicted, its hashtable entry can be converted to a "ghost"
/// entry. Ghosts preserve the frequency counter but mark the location as invalid.
/// When re-inserting a previously evicted key, the ghost's frequency can be
/// preserved, giving "second chance" semantics.
///
/// # Thread Safety
///
/// Implementations must be thread-safe (`Send + Sync`). The cuckoo hashtable
/// implementation uses lock-free CAS operations for all mutations.
pub trait Hashtable: Send + Sync {
    /// Look up a key and return its location and frequency.
    ///
    /// This also increments the frequency counter (probabilistically for
    /// values > 16 using the ASFC algorithm).
    ///
    /// # Returns
    /// `Some((location, frequency))` if found, `None` if not found or ghost.
    fn lookup<S: SegmentKeyVerify>(
        &self,
        key: &[u8],
        segments: &impl SegmentProvider<S>,
    ) -> Option<(ItemLocation, u8)>;

    /// Check if a key exists without updating frequency.
    ///
    /// Useful for conditional operations where you don't want to affect
    /// the item's hotness.
    fn contains<S: SegmentKeyVerify>(&self, key: &[u8], segments: &impl SegmentProvider<S>)
    -> bool;

    /// Insert or update a key's location.
    ///
    /// If the key already exists (live or ghost), updates the location and
    /// preserves the frequency. For ghosts, this "resurrects" the entry.
    ///
    /// # Returns
    /// - `Ok(Some(old_location))` if an existing entry was replaced
    /// - `Ok(None)` if this was a new entry or ghost resurrection
    /// - `Err(CacheError::HashTableFull)` if no space available
    fn insert<S: SegmentKeyVerify>(
        &self,
        key: &[u8],
        location: ItemLocation,
        segments: &impl SegmentProvider<S>,
    ) -> CacheResult<Option<ItemLocation>>;

    /// Insert a key only if it does NOT already exist (ADD semantics).
    ///
    /// If a matching ghost exists, its frequency is preserved (second chance).
    ///
    /// # Returns
    /// - `Ok(())` if inserted successfully
    /// - `Err(CacheError::KeyExists)` if key already exists
    /// - `Err(CacheError::HashTableFull)` if no space available
    fn insert_if_absent<S: SegmentKeyVerify>(
        &self,
        key: &[u8],
        location: ItemLocation,
        segments: &impl SegmentProvider<S>,
    ) -> CacheResult<()>;

    /// Update a key's location only if it DOES exist (REPLACE semantics).
    ///
    /// Does not match ghost entries.
    ///
    /// # Returns
    /// - `Ok(old_location)` if the key was found and updated
    /// - `Err(CacheError::KeyNotFound)` if key doesn't exist
    fn update_if_present<S: SegmentKeyVerify>(
        &self,
        key: &[u8],
        location: ItemLocation,
        segments: &impl SegmentProvider<S>,
    ) -> CacheResult<ItemLocation>;

    /// Remove a key from the hashtable.
    ///
    /// The entry must match the expected location (for ABA safety).
    ///
    /// # Returns
    /// `true` if the entry was found and removed, `false` if not found.
    fn remove(&self, key: &[u8], expected: ItemLocation) -> bool;

    /// Convert an entry to a ghost (preserves frequency).
    ///
    /// Used during eviction when `create_ghosts` is enabled.
    ///
    /// # Returns
    /// `true` if converted to ghost, `false` if not found or already ghost.
    fn convert_to_ghost(&self, key: &[u8], expected: ItemLocation) -> bool;

    /// Update an item's location atomically.
    ///
    /// Used during compaction and tier migration. The entry must match
    /// the expected old location for the update to succeed.
    ///
    /// # Parameters
    /// - `key`: The item's key
    /// - `old_location`: Expected current location
    /// - `new_location`: New location to set
    /// - `preserve_freq`: If true, keeps existing frequency; if false, resets to 1
    ///
    /// # Returns
    /// `true` if the update succeeded, `false` if not found or location mismatch.
    fn cas_location(
        &self,
        key: &[u8],
        old_location: ItemLocation,
        new_location: ItemLocation,
        preserve_freq: bool,
    ) -> bool;

    /// Get the frequency of an item by key.
    ///
    /// Does not match ghost entries.
    fn get_frequency<S: SegmentKeyVerify>(
        &self,
        key: &[u8],
        segments: &impl SegmentProvider<S>,
    ) -> Option<u8>;

    /// Get the frequency of an item at a specific location.
    ///
    /// More precise than `get_frequency` - verifies the location matches.
    fn get_item_frequency(&self, key: &[u8], location: ItemLocation) -> Option<u8>;

    /// Get the frequency of a ghost entry.
    ///
    /// Used to check if we should give "second chance" admission to a key
    /// that was previously evicted.
    fn get_ghost_frequency(&self, key: &[u8]) -> Option<u8>;
}

/// Trait for providing segment access by pool_id and segment_id.
///
/// This abstracts over different segment storage arrangements:
/// - Single pool (pool_id ignored)
/// - Multiple pools (tiered storage)
/// - External segment providers
pub trait SegmentProvider<S: SegmentKeyVerify> {
    /// Get a segment by pool_id and segment_id.
    ///
    /// Returns `None` if the segment doesn't exist or is invalid.
    fn get_segment(&self, pool_id: u8, segment_id: u32) -> Option<&S>;
}

/// Single-pool segment provider (pool_id is ignored).
///
/// Used when there's only one segment pool.
pub struct SinglePool<'a, S> {
    segments: &'a [S],
}

impl<'a, S> SinglePool<'a, S> {
    /// Create a new single-pool provider.
    pub fn new(segments: &'a [S]) -> Self {
        Self { segments }
    }
}

impl<S: SegmentKeyVerify> SegmentProvider<S> for SinglePool<'_, S> {
    fn get_segment(&self, _pool_id: u8, segment_id: u32) -> Option<&S> {
        self.segments.get(segment_id as usize)
    }
}

/// Multi-pool segment provider with up to 4 pools.
pub struct MultiPool<'a, S> {
    pools: [Option<&'a [S]>; 4],
}

impl<'a, S> MultiPool<'a, S> {
    /// Create a new multi-pool provider with no pools.
    pub fn new() -> Self {
        Self { pools: [None; 4] }
    }

    /// Add a pool at the specified ID.
    ///
    /// # Panics
    /// Panics if pool_id >= 4.
    pub fn with_pool(mut self, pool_id: u8, segments: &'a [S]) -> Self {
        assert!(pool_id < 4, "pool_id must be 0-3");
        self.pools[pool_id as usize] = Some(segments);
        self
    }
}

impl<S> Default for MultiPool<'_, S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: SegmentKeyVerify> SegmentProvider<S> for MultiPool<'_, S> {
    fn get_segment(&self, pool_id: u8, segment_id: u32) -> Option<&S> {
        if pool_id >= 4 {
            return None;
        }
        self.pools[pool_id as usize].and_then(|segments| segments.get(segment_id as usize))
    }
}

/// Callback-based segment provider for flexible integration.
///
/// Allows custom logic for resolving segment references.
pub struct FnProvider<F> {
    get_fn: F,
}

impl<F> FnProvider<F> {
    /// Create a new function-based provider.
    pub fn new(get_fn: F) -> Self {
        Self { get_fn }
    }
}

impl<S: SegmentKeyVerify, F: Fn(u8, u32) -> Option<*const S>> SegmentProvider<S> for FnProvider<F> {
    fn get_segment(&self, pool_id: u8, segment_id: u32) -> Option<&S> {
        (self.get_fn)(pool_id, segment_id).map(|ptr| unsafe { &*ptr })
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    // Mock segment for testing
    struct MockSegment {
        _id: u32,
        keys: Vec<(u32, Vec<u8>)>, // (offset, key)
    }

    impl SegmentKeyVerify for MockSegment {
        fn verify_key_at_offset(&self, offset: u32, key: &[u8], _allow_deleted: bool) -> bool {
            self.keys.iter().any(|(off, k)| *off == offset && k == key)
        }
    }

    #[test]
    fn test_single_pool_provider() {
        let segments = vec![
            MockSegment {
                _id: 0,
                keys: vec![],
            },
            MockSegment {
                _id: 1,
                keys: vec![],
            },
        ];
        let provider = SinglePool::new(&segments);

        // pool_id is ignored
        assert!(provider.get_segment(0, 0).is_some());
        assert!(provider.get_segment(1, 0).is_some());
        assert!(provider.get_segment(2, 1).is_some());
        assert!(provider.get_segment(0, 2).is_none());
    }

    #[test]
    fn test_multi_pool_provider() {
        let pool0 = vec![MockSegment {
            _id: 0,
            keys: vec![],
        }];
        let pool2 = vec![
            MockSegment {
                _id: 0,
                keys: vec![],
            },
            MockSegment {
                _id: 1,
                keys: vec![],
            },
        ];

        let provider = MultiPool::new().with_pool(0, &pool0).with_pool(2, &pool2);

        assert!(provider.get_segment(0, 0).is_some());
        assert!(provider.get_segment(0, 1).is_none());
        assert!(provider.get_segment(1, 0).is_none()); // pool 1 not set
        assert!(provider.get_segment(2, 0).is_some());
        assert!(provider.get_segment(2, 1).is_some());
        assert!(provider.get_segment(4, 0).is_none()); // invalid pool_id
    }

    #[test]
    fn test_multi_pool_default() {
        let provider: MultiPool<MockSegment> = MultiPool::default();
        // All pools should be empty
        assert!(provider.get_segment(0, 0).is_none());
        assert!(provider.get_segment(1, 0).is_none());
        assert!(provider.get_segment(2, 0).is_none());
        assert!(provider.get_segment(3, 0).is_none());
    }

    #[test]
    fn test_fn_provider() {
        let segments = [
            MockSegment {
                _id: 0,
                keys: vec![(0, b"key0".to_vec())],
            },
            MockSegment {
                _id: 1,
                keys: vec![(0, b"key1".to_vec())],
            },
        ];

        let provider = FnProvider::new(|_pool_id: u8, segment_id: u32| {
            segments
                .get(segment_id as usize)
                .map(|s| s as *const MockSegment)
        });

        let seg0 = provider.get_segment(0, 0);
        assert!(seg0.is_some());
        assert!(seg0.unwrap().verify_key_at_offset(0, b"key0", false));

        let seg1 = provider.get_segment(0, 1);
        assert!(seg1.is_some());
        assert!(seg1.unwrap().verify_key_at_offset(0, b"key1", false));

        // Invalid segment
        assert!(provider.get_segment(0, 2).is_none());
    }

    #[test]
    fn test_fn_provider_with_pool_id() {
        let pool0_segments = [MockSegment {
            _id: 0,
            keys: vec![(0, b"pool0_key".to_vec())],
        }];
        let pool1_segments = [MockSegment {
            _id: 0,
            keys: vec![(0, b"pool1_key".to_vec())],
        }];

        let provider = FnProvider::new(move |pool_id: u8, segment_id: u32| match pool_id {
            0 => pool0_segments
                .get(segment_id as usize)
                .map(|s| s as *const MockSegment),
            1 => pool1_segments
                .get(segment_id as usize)
                .map(|s| s as *const MockSegment),
            _ => None,
        });

        let seg = provider.get_segment(0, 0);
        assert!(seg.is_some());
        assert!(seg.unwrap().verify_key_at_offset(0, b"pool0_key", false));

        // Pool 2 doesn't exist
        assert!(provider.get_segment(2, 0).is_none());
    }

    #[test]
    fn test_single_pool_empty() {
        let segments: Vec<MockSegment> = vec![];
        let provider = SinglePool::new(&segments);
        assert!(provider.get_segment(0, 0).is_none());
    }

    #[test]
    fn test_multi_pool_all_pools() {
        let pool0 = vec![MockSegment {
            _id: 0,
            keys: vec![],
        }];
        let pool1 = vec![MockSegment {
            _id: 0,
            keys: vec![],
        }];
        let pool2 = vec![MockSegment {
            _id: 0,
            keys: vec![],
        }];
        let pool3 = vec![MockSegment {
            _id: 0,
            keys: vec![],
        }];

        let provider = MultiPool::new()
            .with_pool(0, &pool0)
            .with_pool(1, &pool1)
            .with_pool(2, &pool2)
            .with_pool(3, &pool3);

        assert!(provider.get_segment(0, 0).is_some());
        assert!(provider.get_segment(1, 0).is_some());
        assert!(provider.get_segment(2, 0).is_some());
        assert!(provider.get_segment(3, 0).is_some());
    }

    #[test]
    fn test_mock_segment_verify() {
        let segment = MockSegment {
            _id: 0,
            keys: vec![(10, b"test_key".to_vec()), (20, b"another".to_vec())],
        };

        assert!(segment.verify_key_at_offset(10, b"test_key", false));
        assert!(segment.verify_key_at_offset(20, b"another", false));
        assert!(!segment.verify_key_at_offset(10, b"wrong", false));
        assert!(!segment.verify_key_at_offset(30, b"test_key", false));
    }
}
