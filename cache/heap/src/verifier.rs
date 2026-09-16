//! KeyVerifier implementation for HeapCache.
//!
//! Verifies that a key at a location matches the expected key.
//!
//! This module provides multiple verifiers:
//! - `HeapCacheVerifier`: RAM-only verification using slot storage (strings only)
//! - `MultiTypeVerifier`: Verification across all value types (strings, hashes, lists, sets)

use cache_core::{KeyVerifier, Location};

use crate::hash_storage::HashStorage;
use crate::list_storage::ListStorage;
use crate::location::{SlotLocation, TypedLocation, ValueType};
use crate::set_storage::SetStorage;
use crate::storage::SlotStorage;

/// KeyVerifier for the heap cache.
///
/// Verifies keys by looking up the entry at the given location and
/// comparing the stored key with the expected key.
pub struct HeapCacheVerifier<'a> {
    storage: &'a SlotStorage,
}

impl<'a> HeapCacheVerifier<'a> {
    /// Create a new verifier for the given storage.
    pub fn new(storage: &'a SlotStorage) -> Self {
        Self { storage }
    }
}

impl KeyVerifier for HeapCacheVerifier<'_> {
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        let slot_loc = SlotLocation::from_location(location);
        let slot = match self.storage.get(slot_loc.slot_index()) {
            Some(s) => s,
            None => return false,
        };

        // Get the entry (handles reader counting internally)
        // Allow expired entries during verification to avoid race conditions
        // Pass allow_deleted to get_with_flags
        let entry = match slot.get_with_flags(slot_loc.generation(), true, allow_deleted) {
            Some(e) => e,
            None => return false,
        };

        let key_matches = entry.key() == key;

        // Release read hold
        slot.release_read();

        key_matches
    }
}

/// Multi-type verifier that can verify keys across all value types.
///
/// This verifier dispatches to the appropriate storage based on the value type
/// encoded in the location:
/// - String (type 0): Uses SlotStorage (lock-free entries)
/// - Hash (type 1): Uses HashStorage (RwLock per slot)
/// - List (type 2): Uses ListStorage (Mutex per slot)
/// - Set (type 3): Uses SetStorage (RwLock per slot)
pub struct MultiTypeVerifier<'a> {
    string_storage: &'a SlotStorage,
    hash_storage: &'a HashStorage,
    list_storage: &'a ListStorage,
    set_storage: &'a SetStorage,
}

impl<'a> MultiTypeVerifier<'a> {
    /// Create a new multi-type verifier.
    pub fn new(
        string_storage: &'a SlotStorage,
        hash_storage: &'a HashStorage,
        list_storage: &'a ListStorage,
        set_storage: &'a SetStorage,
    ) -> Self {
        Self {
            string_storage,
            hash_storage,
            list_storage,
            set_storage,
        }
    }
}

impl KeyVerifier for MultiTypeVerifier<'_> {
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        // Don't verify ghost entries
        if location.is_ghost() {
            return false;
        }

        let typed_loc = TypedLocation::from_location(location);

        match typed_loc.value_type() {
            ValueType::String => {
                // String verification uses the slot storage
                let slot = match self.string_storage.get(typed_loc.slot_index()) {
                    Some(s) => s,
                    None => return false,
                };

                let entry = match slot.get_with_flags(typed_loc.generation(), true, allow_deleted) {
                    Some(e) => e,
                    None => return false,
                };

                let key_matches = entry.key() == key;
                slot.release_read();
                key_matches
            }
            ValueType::Hash => {
                self.hash_storage
                    .verify_key(typed_loc.slot_index(), typed_loc.generation(), key)
            }
            ValueType::List => {
                self.list_storage
                    .verify_key(typed_loc.slot_index(), typed_loc.generation(), key)
            }
            ValueType::Set => {
                self.set_storage
                    .verify_key(typed_loc.slot_index(), typed_loc.generation(), key)
            }
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::entry::HeapEntry;
    use std::time::Duration;

    #[test]
    fn test_verifier_key_match() {
        let storage = SlotStorage::new(10);
        let loc = storage.allocate().unwrap();

        let entry = HeapEntry::allocate(b"test_key", b"value", Duration::ZERO, 1).unwrap();
        let slot = storage.get(loc.slot_index()).unwrap();
        slot.store(entry);

        let verifier = HeapCacheVerifier::new(&storage);
        assert!(verifier.verify(b"test_key", loc.to_location(), false));
        assert!(!verifier.verify(b"wrong_key", loc.to_location(), false));
    }

    #[test]
    fn test_verifier_wrong_generation() {
        let storage = SlotStorage::new(10);
        let loc = storage.allocate().unwrap();

        let entry = HeapEntry::allocate(b"key", b"value", Duration::ZERO, 1).unwrap();
        let slot = storage.get(loc.slot_index()).unwrap();
        slot.store(entry);

        // Create location with wrong generation
        let wrong_gen_loc = SlotLocation::new(loc.slot_index(), loc.generation() + 1);

        let verifier = HeapCacheVerifier::new(&storage);
        assert!(!verifier.verify(b"key", wrong_gen_loc.to_location(), false));
    }

    #[test]
    fn test_verifier_deleted_entry() {
        let storage = SlotStorage::new(10);
        let loc = storage.allocate().unwrap();

        let entry = HeapEntry::allocate(b"key", b"value", Duration::ZERO, 1).unwrap();
        let slot = storage.get(loc.slot_index()).unwrap();
        slot.store(entry);

        // Mark as deleted via the entry
        if let Some(e) = slot.get(loc.generation(), true) {
            e.mark_deleted();
            slot.release_read();
        }

        let verifier = HeapCacheVerifier::new(&storage);

        // Without allow_deleted, should fail
        assert!(!verifier.verify(b"key", loc.to_location(), false));

        // With allow_deleted, should succeed
        assert!(verifier.verify(b"key", loc.to_location(), true));
    }
}
