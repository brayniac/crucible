//! Hashtable trait and key verification for cache operations.
//!
//! This module provides:
//! - [`Hashtable`] - Core trait for key -> (location, frequency) mapping
//! - [`KeyVerifier`] - Trait for verifying keys at locations
//! - Support for ghost entries that preserve frequency after eviction

use crate::error::CacheResult;
use crate::location::Location;

/// The entries an insert unlinked, each with the guard its `pin` returned.
///
/// Holds one entry without allocating. More than one only when concurrent
/// inserts of the same absent key each published an entry.
///
/// An insert or [`Hashtable::resolve`] that met an entry whose key must be
/// read from disk also reports it in [`Self::unresolved`].
#[derive(Debug, PartialEq, Eq)]
pub struct Displaced<G> {
    entries: smallvec::SmallVec<[(Location, G); 1]>,
    unresolved: Option<Location>,
}

impl<G> Displaced<G> {
    /// No entries.
    pub fn new() -> Self {
        Self {
            entries: smallvec::SmallVec::new(),
            unresolved: None,
        }
    }

    /// Add an unlinked entry.
    pub fn push(&mut self, location: Location, guard: G) {
        self.entries.push((location, guard));
    }

    /// An entry for the key's tag whose key must be read from disk before
    /// the key can be known to have one entry. Read it and call
    /// [`Hashtable::resolve`].
    pub fn unresolved(&self) -> Option<Location> {
        self.unresolved
    }

    /// Record an entry whose key must be read; see [`Self::unresolved`].
    pub fn set_unresolved(&mut self, location: Location) {
        self.unresolved = Some(location);
    }

    /// Whether no entry was unlinked.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The number of entries unlinked.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// The unlinked entries' locations.
    pub fn locations(&self) -> impl Iterator<Item = Location> + '_ {
        self.entries.iter().map(|(location, _)| *location)
    }
}

impl<G> Default for Displaced<G> {
    fn default() -> Self {
        Self::new()
    }
}

impl<G> IntoIterator for Displaced<G> {
    type Item = (Location, G);
    type IntoIter = smallvec::IntoIter<[(Location, G); 1]>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

/// The answer to whether an entry holds a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The entry holds the key.
    Match,
    /// The entry holds another key, or no live item.
    Mismatch,
    /// The key cannot be compared without reading it from disk.
    Unknown,
}

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
    ///
    /// # What `false` does *not* mean
    ///
    /// `false` is not "this entry is some other key". An implementation
    /// reads item bytes at whatever location it is handed, and that location
    /// can stop being the entry's while the comparison is in flight — a
    /// concurrent publish moves the entry and delete-marks what it left
    /// behind. Such a comparison also answers `false`, about memory that is
    /// no longer the caller's business.
    ///
    /// Callers must therefore not read a single `false` as "this slot does
    /// not hold the key": doing so reports live keys absent. Bucket scans go
    /// through `MultiChoiceHashtable::verify_slot`, which re-reads the slot
    /// word to separate the two cases. Implementations only owe an honest
    /// answer about the location they were given.
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool;

    /// Check `key` at `location` as [`Self::verify`] does, with a third
    /// answer: [`Verdict::Unknown`] when the key cannot be compared without
    /// reading it from disk.
    ///
    /// On `Unknown` the hashtable calls [`Self::unresolved`] with the
    /// location and stops the operation before changing any entry: reads
    /// answer absent, and writes return [`CacheError::KeyUnresolved`]. The
    /// exception is an insert that has already published its entry: it
    /// keeps the entry and reports the location in
    /// [`Displaced::unresolved`]; see [`Hashtable::resolve`].
    ///
    /// The default answers from `verify` and never returns `Unknown`.
    ///
    /// [`CacheError::KeyUnresolved`]: crate::CacheError::KeyUnresolved
    #[inline]
    fn check(&self, key: &[u8], location: Location, allow_deleted: bool) -> Verdict {
        if self.verify(key, location, allow_deleted) {
            Verdict::Match
        } else {
            Verdict::Mismatch
        }
    }

    /// Called with the location each time [`Self::check`] answers
    /// [`Verdict::Unknown`]. The caller reads the key there and retries. The
    /// default does nothing.
    #[inline]
    fn unresolved(&self, _location: Location) {}

    /// Prefetch memory at the given location.
    ///
    /// Called by the hashtable after a tag match but before full verification.
    /// This allows overlapping memory prefetch with the Acquire barrier overhead.
    ///
    /// The default implementation is a no-op. Segment-based verifiers can
    /// override this to prefetch the segment memory at the given offset.
    #[inline]
    fn prefetch(&self, _location: Location) {
        // Default no-op - implementations can override for segment-based caches
    }
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
    /// - `Ok(displaced)`: the entries this call unlinked, as described for
    ///   [`Hashtable::insert_pinned`]. The caller retires each one.
    /// - `Err(CacheError::HashTableFull)` if no space available
    fn insert(
        &self,
        key: &[u8],
        location: Location,
        verifier: &impl KeyVerifier,
    ) -> CacheResult<Displaced<()>> {
        self.insert_pinned(key, location, verifier, |_| ())
    }

    /// As [`Hashtable::insert`], calling `pin` with the location of the
    /// entry about to be replaced before replacing it.
    ///
    /// `pin` returns a guard, typically one that keeps the old location's
    /// storage from being reused. It runs inside the loop that replaces the
    /// entry, so it can be called several times and must not wait: if the
    /// entry changes before it is replaced, the guard is dropped, and `pin`
    /// is called again if the entry still belongs to `key`. The guard for
    /// each unlinked entry is returned with its location, so the caller can
    /// retire the old item while the guard is held.
    ///
    /// A key has at most one entry once every insert of it has returned.
    /// Concurrent inserts of a key that is absent can each publish an entry;
    /// each then unlinks every entry for the key except the one at the
    /// highest slot position, possibly its own. So `location` may be among
    /// the displaced entries, and an entry published by another insert may be
    /// too.
    ///
    /// # Returns
    /// - `Ok(displaced)`: the entries this call unlinked, each with its
    ///   guard. Empty for a new key or a resurrected ghost; one entry for a
    ///   replaced entry.
    /// - `Err(CacheError::HashTableFull)` if no space available
    fn insert_pinned<G>(
        &self,
        key: &[u8],
        location: Location,
        verifier: &impl KeyVerifier,
        pin: impl FnMut(Location) -> G,
    ) -> CacheResult<Displaced<G>>;

    /// Unlink every live entry for `key` except the one at the highest slot
    /// position, as an insert does after publishing its entry, and return
    /// each with the guard `pin` returned for it. The caller retires each.
    ///
    /// An entry whose check answers [`Verdict::Unknown`] is left in place
    /// and reported in [`Displaced::unresolved`]; the others are resolved
    /// without it. The caller reads that entry's key and calls this again.
    /// Repeating is safe: every call keeps the same entry.
    fn resolve<G>(
        &self,
        key: &[u8],
        verifier: &impl KeyVerifier,
        pin: impl FnMut(Location) -> G,
    ) -> Displaced<G>;

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
    ) -> CacheResult<Location> {
        self.update_if_present_pinned(key, location, verifier, |_| ())
            .map(|(old, ())| old)
    }

    /// As [`Hashtable::update_if_present`], calling `pin` with the location
    /// of the entry about to be replaced before replacing it, as
    /// [`Hashtable::insert_pinned`] does.
    ///
    /// # Returns
    /// - `Ok((old_location, guard))` if the key was found and updated
    /// - `Err(CacheError::KeyNotFound)` if key doesn't exist
    fn update_if_present_pinned<G>(
        &self,
        key: &[u8],
        location: Location,
        verifier: &impl KeyVerifier,
        pin: impl FnMut(Location) -> G,
    ) -> CacheResult<(Location, G)>;

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

    // =========================================================================
    // S3-FIFO support methods
    // =========================================================================
    // These methods provide bucket-level access for the S3-FIFO eviction policy.
    // They allow efficient tracking of items by their bucket index rather than
    // requiring key hashing on every eviction check.

    /// Look up a key and return its bucket index and packed item info.
    ///
    /// Used by S3-FIFO to record inserts for tracking. Returns the bucket_index
    /// where the item is stored and the full item_info (tag, freq, location).
    ///
    /// Does NOT update frequency (use `lookup` for that).
    fn lookup_for_tracking(&self, _key: &[u8], _verifier: &impl KeyVerifier) -> Option<(u64, u64)> {
        None // Default: not supported
    }

    /// Get item info from a bucket matching a predicate.
    ///
    /// This is used by S3-FIFO to verify that a queue entry is still valid
    /// in the hashtable.
    ///
    /// # Arguments
    /// - `bucket_index`: The bucket to search
    /// - `predicate`: Function that returns true if the item info matches
    ///
    /// # Returns
    /// The full item info (tag, frequency, location) if found and predicate matches.
    fn get_info_at_bucket<F>(&self, _bucket_index: u64, _predicate: F) -> Option<u64>
    where
        F: Fn(u64) -> bool,
    {
        None // Default: not supported
    }

    /// Set the frequency for an item at a specific bucket and location.
    ///
    /// Used by S3-FIFO to decay frequency during eviction.
    fn set_frequency_at_bucket(&self, _bucket_index: u64, _location: u64, _freq: u8) {
        // Default: no-op
    }

    /// Convert an item to a ghost at a specific bucket and location.
    ///
    /// Used by S3-FIFO during eviction when ghost tracking is enabled.
    /// Returns `true` iff this call converted the entry; `false` if no entry
    /// at that location remains (it was overwritten, deleted or evicted).
    fn convert_to_ghost_at_bucket(&self, _bucket_index: u64, _location: u64) -> bool {
        false
    }

    /// Remove an item at a specific bucket and location.
    ///
    /// Used by S3-FIFO during eviction when ghost tracking is disabled.
    /// Returns `true` iff this call removed the entry; `false` if no entry at
    /// that location remains (it was overwritten, deleted or evicted). Only
    /// the caller that removed the entry may free the item it names.
    fn remove_at_bucket(&self, _bucket_index: u64, _location: u64) -> bool {
        false
    }

    /// Clear all entries from the hashtable.
    ///
    /// This resets all entries to empty (zero). Used by the segment
    /// backend's flush and by the cache resets.
    /// After calling this, all lookups will return None until new items
    /// are inserted.
    fn clear(&self);
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
