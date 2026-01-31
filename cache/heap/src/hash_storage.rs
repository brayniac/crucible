//! Hash storage for Redis-like hash data structures.
//!
//! Provides per-slot RwLock protected hash storage with efficient small/large
//! encoding for fields.
//!
//! # Design
//!
//! - Each slot holds a hash (field -> value mapping)
//! - Uses RwLock for concurrent read access with exclusive write
//! - Small hashes (≤8 fields) use SmallVec for cache efficiency
//! - Large hashes switch to HashMap for O(1) lookup
//! - Free list uses Treiber stack (lock-free allocation/deallocation)

use parking_lot::RwLock;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::time::Duration;

use crate::sync::{AtomicU16, AtomicU32, AtomicU64, Ordering, spin_loop};

/// Threshold for switching from SmallVec to HashMap.
const SMALL_HASH_THRESHOLD: usize = 8;

/// Sentinel value indicating an empty free list.
const EMPTY_FREE_LIST: u32 = u32::MAX;

/// Storage for hash field-value pairs.
///
/// Uses RwLock per slot for read-heavy workloads while allowing atomic field updates.
pub struct HashStorage {
    /// Slots containing hash data.
    slots: Vec<HashSlot>,
    /// Head of the free list (Treiber stack with version tag).
    free_head: AtomicU64,
    /// Number of currently occupied slots.
    occupied_count: AtomicU32,
}

/// A single hash slot with RwLock protection.
pub struct HashSlot {
    /// Generation counter for ABA protection.
    generation: AtomicU16,
    /// Next free slot index (when in free list).
    next_free: AtomicU32,
    /// Hash data protected by RwLock.
    data: RwLock<Option<HashSlotData>>,
}

/// Internal hash data for an occupied slot.
struct HashSlotData {
    /// The key this hash is stored under (for verification).
    key: Box<[u8]>,
    /// Hash fields (small or large representation).
    fields: HashFields,
    /// Expiration time in seconds since Unix epoch (0 = no expiry).
    expire_at: u32,
    /// CAS token for optimistic locking (reserved for future use).
    #[allow(dead_code)]
    cas_token: u64,
}

/// Hash fields storage - either small (SmallVec) or large (HashMap).
enum HashFields {
    /// Small hash with up to 8 fields inline.
    Small(SmallVec<[(Box<[u8]>, Box<[u8]>); SMALL_HASH_THRESHOLD]>),
    /// Large hash using HashMap for O(1) lookup.
    Large(HashMap<Box<[u8]>, Box<[u8]>>),
}

impl HashFields {
    /// Create a new empty hash fields storage.
    fn new() -> Self {
        HashFields::Small(SmallVec::new())
    }

    /// Get a field value.
    fn get(&self, field: &[u8]) -> Option<&[u8]> {
        match self {
            HashFields::Small(vec) => vec
                .iter()
                .find(|(f, _)| f.as_ref() == field)
                .map(|(_, v)| v.as_ref()),
            HashFields::Large(map) => map.get(field).map(|v| v.as_ref()),
        }
    }

    /// Set a field value. Returns true if the field was newly created.
    fn set(&mut self, field: &[u8], value: &[u8]) -> bool {
        match self {
            HashFields::Small(vec) => {
                // Check if field exists
                if let Some(entry) = vec.iter_mut().find(|(f, _)| f.as_ref() == field) {
                    entry.1 = value.into();
                    return false;
                }

                // Add new field
                if vec.len() < SMALL_HASH_THRESHOLD {
                    vec.push((field.into(), value.into()));
                    true
                } else {
                    // Upgrade to large
                    let mut map: HashMap<Box<[u8]>, Box<[u8]>> =
                        vec.drain(..).map(|(f, v)| (f, v)).collect();
                    map.insert(field.into(), value.into());
                    *self = HashFields::Large(map);
                    true
                }
            }
            HashFields::Large(map) => {
                use std::collections::hash_map::Entry;
                match map.entry(field.into()) {
                    Entry::Occupied(mut e) => {
                        e.insert(value.into());
                        false
                    }
                    Entry::Vacant(e) => {
                        e.insert(value.into());
                        true
                    }
                }
            }
        }
    }

    /// Delete a field. Returns true if the field was removed.
    /// Delete a field and return its size if it existed.
    fn delete(&mut self, field: &[u8]) -> Option<usize> {
        match self {
            HashFields::Small(vec) => {
                if let Some(pos) = vec.iter().position(|(f, _)| f.as_ref() == field) {
                    let (f, v) = vec.swap_remove(pos);
                    Some(f.len() + v.len() + 16) // field + value + Box overhead
                } else {
                    None
                }
            }
            HashFields::Large(map) => map.remove(field).map(|v| field.len() + v.len() + 32), // field + value + HashMap overhead
        }
    }

    /// Check if a field exists.
    fn contains(&self, field: &[u8]) -> bool {
        match self {
            HashFields::Small(vec) => vec.iter().any(|(f, _)| f.as_ref() == field),
            HashFields::Large(map) => map.contains_key(field),
        }
    }

    /// Get the number of fields.
    fn len(&self) -> usize {
        match self {
            HashFields::Small(vec) => vec.len(),
            HashFields::Large(map) => map.len(),
        }
    }

    /// Check if empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get all field names.
    fn keys(&self) -> Vec<Vec<u8>> {
        match self {
            HashFields::Small(vec) => vec.iter().map(|(f, _)| f.to_vec()).collect(),
            HashFields::Large(map) => map.keys().map(|f| f.to_vec()).collect(),
        }
    }

    /// Get all field values.
    fn values(&self) -> Vec<Vec<u8>> {
        match self {
            HashFields::Small(vec) => vec.iter().map(|(_, v)| v.to_vec()).collect(),
            HashFields::Large(map) => map.values().map(|v| v.to_vec()).collect(),
        }
    }

    /// Get all field-value pairs.
    fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        match self {
            HashFields::Small(vec) => Box::new(vec.iter().map(|(f, v)| (f.as_ref(), v.as_ref())))
                as Box<dyn Iterator<Item = (&[u8], &[u8])>>,
            HashFields::Large(map) => Box::new(map.iter().map(|(f, v)| (f.as_ref(), v.as_ref()))),
        }
    }
}

impl HashSlot {
    /// Create a new empty slot.
    fn new() -> Self {
        Self {
            generation: AtomicU16::new(0),
            next_free: AtomicU32::new(EMPTY_FREE_LIST),
            data: RwLock::new(None),
        }
    }

    /// Get the current generation.
    #[inline]
    pub fn generation(&self) -> u16 {
        self.generation.load(Ordering::Acquire)
    }

    /// Increment generation (wraps at 9 bits for TypedLocation compatibility).
    #[inline]
    fn increment_generation(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Check if the slot is occupied.
    #[inline]
    pub fn is_occupied(&self) -> bool {
        self.data.read().is_some()
    }
}

impl HashStorage {
    /// Create a new hash storage with the given capacity.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be positive");
        assert!(
            capacity <= u32::MAX as usize,
            "capacity exceeds maximum slot index"
        );

        let mut slots = Vec::with_capacity(capacity);

        // Initialize all slots and build free list
        for i in 0..capacity {
            slots.push(HashSlot::new());
            let next = if i + 1 < capacity {
                (i + 1) as u32
            } else {
                EMPTY_FREE_LIST
            };
            slots[i].next_free.store(next, Ordering::Relaxed);
        }

        Self {
            slots,
            free_head: AtomicU64::new(pack_head(0, 0)),
            occupied_count: AtomicU32::new(0),
        }
    }

    /// Allocate a slot.
    ///
    /// Returns the slot index and generation, or None if exhausted.
    pub fn allocate(&self) -> Option<(u32, u16)> {
        loop {
            let packed = self.free_head.load(Ordering::Acquire);
            let (head, version) = unpack_head(packed);
            if head == EMPTY_FREE_LIST {
                return None;
            }

            let slot = &self.slots[head as usize];
            let next = slot.next_free.load(Ordering::Acquire);

            let new_packed = pack_head(next, version.wrapping_add(1));
            match self.free_head.compare_exchange_weak(
                packed,
                new_packed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.occupied_count.fetch_add(1, Ordering::Relaxed);
                    let generation = slot.generation();
                    return Some((head, generation));
                }
                Err(_) => {
                    spin_loop();
                }
            }
        }
    }

    /// Deallocate a slot.
    pub fn deallocate(&self, idx: u32) {
        if idx as usize >= self.slots.len() {
            return;
        }

        let slot = &self.slots[idx as usize];

        // Clear the data
        {
            let mut guard = slot.data.write();
            *guard = None;
        }

        // Increment generation for ABA protection
        slot.increment_generation();

        // Decrement occupied count
        self.occupied_count.fetch_sub(1, Ordering::Relaxed);

        // Push onto free list
        loop {
            let packed = self.free_head.load(Ordering::Acquire);
            let (head, version) = unpack_head(packed);
            slot.next_free.store(head, Ordering::Release);

            let new_packed = pack_head(idx, version.wrapping_add(1));
            match self.free_head.compare_exchange_weak(
                packed,
                new_packed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => spin_loop(),
            }
        }
    }

    /// Get a slot by index.
    #[inline]
    pub fn get(&self, idx: u32) -> Option<&HashSlot> {
        self.slots.get(idx as usize)
    }

    /// Get the number of occupied slots.
    #[inline]
    pub fn occupied(&self) -> u32 {
        self.occupied_count.load(Ordering::Relaxed)
    }

    /// Get the total capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Check if a slot is currently occupied.
    #[inline]
    pub fn is_slot_occupied(&self, idx: u32) -> bool {
        self.slots
            .get(idx as usize)
            .is_some_and(|slot| slot.is_occupied())
    }

    /// Get the key stored in a slot (for eviction).
    pub fn get_key(&self, idx: u32, expected_gen: u16) -> Option<Vec<u8>> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }
        let guard = slot.data.read();
        guard.as_ref().map(|data| data.key.to_vec())
    }

    /// Initialize a slot with a new hash.
    pub fn init_slot(
        &self,
        idx: u32,
        key: &[u8],
        ttl: Option<Duration>,
        cas_token: u64,
    ) -> Option<u16> {
        let slot = self.get(idx)?;
        let expire_at = ttl.map(|d| expire_timestamp(d)).unwrap_or(0);

        let data = HashSlotData {
            key: key.into(),
            fields: HashFields::new(),
            expire_at,
            cas_token,
        };

        {
            let mut guard = slot.data.write();
            *guard = Some(data);
        }

        Some(slot.generation())
    }

    /// Set a field in a hash.
    ///
    /// Returns (fields_created, bytes_delta), or None if slot invalid.
    /// bytes_delta is positive when bytes are added, negative when replaced with smaller.
    pub fn hset(
        &self,
        idx: u32,
        expected_gen: u16,
        field: &[u8],
        value: &[u8],
    ) -> Option<(usize, isize)> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let mut guard = slot.data.write();
        let data = guard.as_mut()?;

        // Calculate size of new field+value
        let new_size = field.len() + value.len() + 16; // field + value + Box overhead

        // Check if field exists and get old size
        let (created, bytes_delta) = if let Some(old_value) = data.fields.get(field) {
            let old_size = field.len() + old_value.len() + 16;
            data.fields.set(field, value);
            (0, new_size as isize - old_size as isize)
        } else {
            data.fields.set(field, value);
            (1, new_size as isize)
        };

        Some((created, bytes_delta))
    }

    /// Get a field from a hash.
    pub fn hget(&self, idx: u32, expected_gen: u16, field: &[u8]) -> Option<Vec<u8>> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.read();
        let data = guard.as_ref()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        data.fields.get(field).map(|v| v.to_vec())
    }

    /// Get all fields and values from a hash.
    pub fn hgetall(&self, idx: u32, expected_gen: u16) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.read();
        let data = guard.as_ref()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        Some(
            data.fields
                .iter()
                .map(|(f, v)| (f.to_vec(), v.to_vec()))
                .collect(),
        )
    }

    /// Delete fields from a hash.
    ///
    /// Returns the number of fields removed.
    /// Delete fields from a hash.
    ///
    /// Returns (fields_removed, bytes_freed), or None if slot invalid.
    pub fn hdel(&self, idx: u32, expected_gen: u16, fields: &[&[u8]]) -> Option<(usize, usize)> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let mut guard = slot.data.write();
        let data = guard.as_mut()?;

        let mut removed = 0;
        let mut bytes_freed = 0;
        for field in fields {
            if let Some(size) = data.fields.delete(field) {
                removed += 1;
                bytes_freed += size;
            }
        }
        Some((removed, bytes_freed))
    }

    /// Check if a field exists in a hash.
    pub fn hexists(&self, idx: u32, expected_gen: u16, field: &[u8]) -> Option<bool> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.read();
        let data = guard.as_ref()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        Some(data.fields.contains(field))
    }

    /// Get the number of fields in a hash.
    pub fn hlen(&self, idx: u32, expected_gen: u16) -> Option<usize> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.read();
        let data = guard.as_ref()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        Some(data.fields.len())
    }

    /// Get all field names in a hash.
    pub fn hkeys(&self, idx: u32, expected_gen: u16) -> Option<Vec<Vec<u8>>> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.read();
        let data = guard.as_ref()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        Some(data.fields.keys())
    }

    /// Get all values in a hash.
    pub fn hvals(&self, idx: u32, expected_gen: u16) -> Option<Vec<Vec<u8>>> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.read();
        let data = guard.as_ref()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        Some(data.fields.values())
    }

    /// Verify that the key at a slot matches the expected key.
    pub fn verify_key(&self, idx: u32, expected_gen: u16, key: &[u8]) -> bool {
        let Some(slot) = self.get(idx) else {
            return false;
        };
        if slot.generation() != expected_gen {
            return false;
        }

        let guard = slot.data.read();
        match guard.as_ref() {
            Some(data) => data.key.as_ref() == key,
            None => false,
        }
    }

    /// Check if the hash is empty (used to determine if we should delete the key).
    pub fn is_empty(&self, idx: u32, expected_gen: u16) -> Option<bool> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.read();
        let data = guard.as_ref()?;
        Some(data.fields.is_empty())
    }

    /// Get estimated size of a hash in bytes.
    pub fn size_bytes(&self, idx: u32, expected_gen: u16) -> Option<usize> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.read();
        let data = guard.as_ref()?;

        // Base size: key + metadata
        let mut size = data.key.len() + 16; // key + expire_at + cas_token

        // Fields size
        match &data.fields {
            HashFields::Small(vec) => {
                for (f, v) in vec.iter() {
                    size += f.len() + v.len() + 16; // field + value + Box overhead
                }
            }
            HashFields::Large(map) => {
                for (f, v) in map.iter() {
                    size += f.len() + v.len() + 32; // field + value + HashMap entry overhead
                }
            }
        }

        Some(size)
    }
}

/// Pack index and version into a u64 tagged pointer.
#[inline]
fn pack_head(index: u32, version: u32) -> u64 {
    ((version as u64) << 32) | (index as u64)
}

/// Unpack index and version from a u64 tagged pointer.
#[inline]
fn unpack_head(packed: u64) -> (u32, u32) {
    let index = packed as u32;
    let version = (packed >> 32) as u32;
    (index, version)
}

/// Calculate expiration timestamp from TTL.
fn expire_timestamp(ttl: Duration) -> u32 {
    use clocksource::coarse::UnixInstant;
    let now = UnixInstant::now();
    let now_secs = now.duration_since(UnixInstant::EPOCH).as_secs() as u32;
    now_secs.saturating_add(ttl.as_secs() as u32)
}

/// Check if an expiration timestamp has passed.
fn is_expired(expire_at: u32) -> bool {
    use clocksource::coarse::UnixInstant;
    let now = UnixInstant::now();
    let now_secs = now.duration_since(UnixInstant::EPOCH).as_secs() as u32;
    now_secs >= expire_at
}

// Ensure HashStorage is Send + Sync
unsafe impl Send for HashStorage {}
unsafe impl Sync for HashStorage {}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_hash_fields_small() {
        let mut fields = HashFields::new();

        // Add fields (returns true when new, false when updating)
        assert!(fields.set(b"field1", b"value1"));
        assert!(fields.set(b"field2", b"value2"));
        assert!(!fields.set(b"field1", b"updated")); // Update existing - returns false

        assert_eq!(fields.len(), 2);
        assert_eq!(fields.get(b"field1"), Some(b"updated".as_slice()));
        assert_eq!(fields.get(b"field2"), Some(b"value2".as_slice()));
        assert!(fields.get(b"field3").is_none());
    }

    #[test]
    fn test_hash_fields_upgrade_to_large() {
        let mut fields = HashFields::new();

        // Fill up to threshold
        for i in 0..SMALL_HASH_THRESHOLD {
            let field = format!("field{}", i);
            let value = format!("value{}", i);
            assert!(fields.set(field.as_bytes(), value.as_bytes()));
        }

        // Should still be small
        assert!(matches!(fields, HashFields::Small(_)));

        // One more should upgrade
        assert!(fields.set(b"overflow", b"test"));
        assert!(matches!(fields, HashFields::Large(_)));
        assert_eq!(fields.len(), SMALL_HASH_THRESHOLD + 1);
    }

    #[test]
    fn test_hash_storage_basic() {
        let storage = HashStorage::new(10);

        // Allocate a slot
        let (idx, _generation) = storage.allocate().unwrap();
        assert_eq!(storage.occupied(), 1);

        // Initialize with a hash
        let generation = storage.init_slot(idx, b"mykey", None, 1).unwrap();

        // Set fields - returns (fields_created, bytes_delta)
        assert_eq!(
            storage.hset(idx, generation, b"name", b"Alice").unwrap().0,
            1
        );
        assert_eq!(storage.hset(idx, generation, b"age", b"30").unwrap().0, 1);
        assert_eq!(storage.hset(idx, generation, b"name", b"Bob").unwrap().0, 0); // Update

        // Get fields
        assert_eq!(storage.hget(idx, generation, b"name").unwrap(), b"Bob");
        assert_eq!(storage.hget(idx, generation, b"age").unwrap(), b"30");
        assert!(storage.hget(idx, generation, b"missing").is_none());

        // Check length
        assert_eq!(storage.hlen(idx, generation).unwrap(), 2);

        // Delete field - returns (fields_removed, bytes_freed)
        assert_eq!(
            storage
                .hdel(idx, generation, &[b"name".as_slice()])
                .unwrap()
                .0,
            1
        );
        assert_eq!(storage.hlen(idx, generation).unwrap(), 1);

        // Deallocate
        storage.deallocate(idx);
        assert_eq!(storage.occupied(), 0);
    }

    #[test]
    fn test_hash_storage_verify_key() {
        let storage = HashStorage::new(10);
        let (idx, _) = storage.allocate().unwrap();
        let generation = storage.init_slot(idx, b"mykey", None, 1).unwrap();

        assert!(storage.verify_key(idx, generation, b"mykey"));
        assert!(!storage.verify_key(idx, generation, b"wrongkey"));
    }

    #[test]
    fn test_hash_storage_hgetall() {
        let storage = HashStorage::new(10);
        let (idx, _) = storage.allocate().unwrap();
        let generation = storage.init_slot(idx, b"mykey", None, 1).unwrap();

        storage.hset(idx, generation, b"a", b"1").unwrap();
        storage.hset(idx, generation, b"b", b"2").unwrap();

        let all = storage.hgetall(idx, generation).unwrap();
        assert_eq!(all.len(), 2);
    }
}
