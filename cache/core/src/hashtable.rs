//! Hashtable trait and key verification for cache operations.
//!
//! This module provides:
//! - [`Hashtable`] - Core trait for key -> (location, frequency) mapping
//! - [`KeyVerifier`] - Trait for verifying keys at locations
//! - Support for ghost entries that preserve frequency after eviction

use crate::error::CacheResult;
use crate::location::Location;

/// Trait for verifying that a key exists at a location.
///
/// The hashtable calls this during lookup/insert to confirm that a tag match
/// corresponds to an actual key match (avoiding false positives from hash
/// collisions in the 12-bit tag).
///
/// # Implementors
///
/// Storage backends implement this trait to provide key verification:
/// - Segment caches: Verify key in segment at offset
/// - Arc caches: Verify key in slot at index
/// - SSD-backed caches: Verify key from disk
///
/// # Thread Safety
///
/// Implementations must be thread-safe (`Send + Sync`) as verification may be
/// called concurrently from multiple threads.
pub trait KeyVerifier: Send + Sync {
    /// Verify that `key` exists at `location`.
    ///
    /// # Parameters
    /// - `key`: The key to verify
    /// - `location`: The opaque location to check
    /// - `allow_deleted`: If `true`, match even if item is marked deleted
    ///
    /// # Returns
    /// `true` if the key matches at this location.
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool;
}

/// Core trait for hashtable operations.
///
/// A hashtable maps keys to `Location` values, tracking the physical
/// location of items in storage. It also maintains frequency counters
/// for each item, supporting eviction algorithms like S3FIFO.
///
/// # Ghost Entries
///
/// When an item is evicted, its hashtable entry can be converted to a "ghost"
/// entry. Ghosts preserve the frequency counter but mark the location as invalid
/// (`Location::GHOST`). When re-inserting a previously evicted key, the ghost's
/// frequency can be preserved, giving "second chance" semantics.
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
    fn lookup(&self, key: &[u8], verifier: &impl KeyVerifier) -> Option<(Location, u8)>;

    /// Check if a key exists without updating frequency.
    ///
    /// Useful for conditional operations where you don't want to affect
    /// the item's hotness.
    fn contains(&self, key: &[u8], verifier: &impl KeyVerifier) -> bool;

    /// Insert or update a key's location.
    ///
    /// If the key already exists (live or ghost), updates the location and
    /// preserves the frequency. For ghosts, this "resurrects" the entry.
    ///
    /// # Returns
    /// - `Ok(Some(old_location))` if an existing entry was replaced
    /// - `Ok(None)` if this was a new entry or ghost resurrection
    /// - `Err(CacheError::HashTableFull)` if no space available
    fn insert(
        &self,
        key: &[u8],
        location: Location,
        verifier: &impl KeyVerifier,
    ) -> CacheResult<Option<Location>>;

    /// Insert a key only if it does NOT already exist (ADD semantics).
    ///
    /// If a matching ghost exists, its frequency is preserved (second chance).
    ///
    /// # Returns
    /// - `Ok(())` if inserted successfully
    /// - `Err(CacheError::KeyExists)` if key already exists
    /// - `Err(CacheError::HashTableFull)` if no space available
    fn insert_if_absent(
        &self,
        key: &[u8],
        location: Location,
        verifier: &impl KeyVerifier,
    ) -> CacheResult<()>;

    /// Update a key's location only if it DOES exist (REPLACE semantics).
    ///
    /// Does not match ghost entries.
    ///
    /// # Returns
    /// - `Ok(old_location)` if the key was found and updated
    /// - `Err(CacheError::KeyNotFound)` if key doesn't exist
    fn update_if_present(
        &self,
        key: &[u8],
        location: Location,
        verifier: &impl KeyVerifier,
    ) -> CacheResult<Location>;

    /// Remove a key from the hashtable.
    ///
    /// The entry must match the expected location (for ABA safety).
    ///
    /// # Returns
    /// `true` if the entry was found and removed, `false` if not found.
    fn remove(&self, key: &[u8], expected: Location) -> bool;

    /// Convert an entry to a ghost (preserves frequency).
    ///
    /// Used during eviction when ghost tracking is enabled.
    ///
    /// # Returns
    /// `true` if converted to ghost, `false` if not found or already ghost.
    fn convert_to_ghost(&self, key: &[u8], expected: Location) -> bool;

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
        old_location: Location,
        new_location: Location,
        preserve_freq: bool,
    ) -> bool;

    /// Get the frequency of an item by key.
    ///
    /// Does not match ghost entries.
    fn get_frequency(&self, key: &[u8], verifier: &impl KeyVerifier) -> Option<u8>;

    /// Get the frequency of an item at a specific location.
    ///
    /// More precise than `get_frequency` - verifies the location matches.
    fn get_item_frequency(&self, key: &[u8], location: Location) -> Option<u8>;

    /// Get the frequency of a ghost entry.
    ///
    /// Used to check if we should give "second chance" admission to a key
    /// that was previously evicted.
    fn get_ghost_frequency(&self, key: &[u8]) -> Option<u8>;
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    // Mock verifier for testing
    struct MockVerifier {
        valid_locations: Vec<(Vec<u8>, Location, bool)>, // (key, location, deleted)
    }

    impl MockVerifier {
        fn new() -> Self {
            Self {
                valid_locations: Vec::new(),
            }
        }

        fn add(&mut self, key: &[u8], location: Location, deleted: bool) {
            self.valid_locations.push((key.to_vec(), location, deleted));
        }
    }

    impl KeyVerifier for MockVerifier {
        fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
            self.valid_locations.iter().any(|(k, loc, deleted)| {
                k == key && *loc == location && (allow_deleted || !deleted)
            })
        }
    }

    #[test]
    fn test_mock_verifier() {
        let mut verifier = MockVerifier::new();
        let loc = Location::new(100);

        verifier.add(b"key1", loc, false);
        verifier.add(b"key2", Location::new(200), true);

        assert!(verifier.verify(b"key1", loc, false));
        assert!(verifier.verify(b"key1", loc, true));
        assert!(!verifier.verify(b"key1", Location::new(101), false));
        assert!(!verifier.verify(b"wrong", loc, false));

        // Deleted item
        assert!(!verifier.verify(b"key2", Location::new(200), false));
        assert!(verifier.verify(b"key2", Location::new(200), true));
    }
}
