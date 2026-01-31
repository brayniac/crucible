//! List storage for Redis-like list data structures.
//!
//! Provides per-slot Mutex protected list storage using VecDeque for O(1)
//! push/pop from both ends.
//!
//! # Design
//!
//! - Each slot holds a list (ordered collection of elements)
//! - Uses Mutex since push/pop always mutate (RwLock offers no benefit)
//! - Implemented with VecDeque for efficient operations at both ends
//! - Free list uses Treiber stack (lock-free allocation/deallocation)

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::time::Duration;

use crate::sync::{AtomicU16, AtomicU32, AtomicU64, Ordering, spin_loop};

/// Sentinel value indicating an empty free list.
const EMPTY_FREE_LIST: u32 = u32::MAX;

/// Storage for list data structures.
///
/// Uses Mutex per slot since list operations almost always mutate.
pub struct ListStorage {
    /// Slots containing list data.
    slots: Vec<ListSlot>,
    /// Head of the free list (Treiber stack with version tag).
    free_head: AtomicU64,
    /// Number of currently occupied slots.
    occupied_count: AtomicU32,
}

/// A single list slot with Mutex protection.
pub struct ListSlot {
    /// Generation counter for ABA protection.
    generation: AtomicU16,
    /// Next free slot index (when in free list).
    next_free: AtomicU32,
    /// List data protected by Mutex.
    data: Mutex<Option<ListSlotData>>,
}

/// Internal list data for an occupied slot.
struct ListSlotData {
    /// The key this list is stored under (for verification).
    key: Box<[u8]>,
    /// List elements.
    items: VecDeque<Box<[u8]>>,
    /// Expiration time in seconds since Unix epoch (0 = no expiry).
    expire_at: u32,
    /// CAS token for optimistic locking (reserved for future use).
    #[allow(dead_code)]
    cas_token: u64,
}

impl ListSlot {
    /// Create a new empty slot.
    fn new() -> Self {
        Self {
            generation: AtomicU16::new(0),
            next_free: AtomicU32::new(EMPTY_FREE_LIST),
            data: Mutex::new(None),
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
        self.data.lock().is_some()
    }
}

impl ListStorage {
    /// Create a new list storage with the given capacity.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be positive");
        assert!(
            capacity <= u32::MAX as usize,
            "capacity exceeds maximum slot index"
        );

        let mut slots = Vec::with_capacity(capacity);

        // Initialize all slots and build free list
        for i in 0..capacity {
            slots.push(ListSlot::new());
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
            let mut guard = slot.data.lock();
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
    pub fn get(&self, idx: u32) -> Option<&ListSlot> {
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
        let guard = slot.data.lock();
        guard.as_ref().map(|data| data.key.to_vec())
    }

    /// Initialize a slot with a new empty list.
    pub fn init_slot(
        &self,
        idx: u32,
        key: &[u8],
        ttl: Option<Duration>,
        cas_token: u64,
    ) -> Option<u16> {
        let slot = self.get(idx)?;
        let expire_at = ttl.map(expire_timestamp).unwrap_or(0);

        let data = ListSlotData {
            key: key.into(),
            items: VecDeque::new(),
            expire_at,
            cas_token,
        };

        {
            let mut guard = slot.data.lock();
            *guard = Some(data);
        }

        Some(slot.generation())
    }

    /// Push elements to the left (head) of a list.
    ///
    /// Returns (new_length, bytes_added), or None if slot invalid.
    pub fn lpush(&self, idx: u32, expected_gen: u16, values: &[&[u8]]) -> Option<(usize, usize)> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let mut guard = slot.data.lock();
        let data = guard.as_mut()?;

        let mut bytes_added = 0;
        // Push in reverse order so they appear in order at the head
        for value in values.iter().rev() {
            bytes_added += value.len() + 8; // value + Box overhead
            data.items.push_front((*value).into());
        }

        Some((data.items.len(), bytes_added))
    }

    /// Push elements to the right (tail) of a list.
    ///
    /// Returns (new_length, bytes_added), or None if slot invalid.
    pub fn rpush(&self, idx: u32, expected_gen: u16, values: &[&[u8]]) -> Option<(usize, usize)> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let mut guard = slot.data.lock();
        let data = guard.as_mut()?;

        let mut bytes_added = 0;
        for value in values {
            bytes_added += value.len() + 8; // value + Box overhead
            data.items.push_back((*value).into());
        }

        Some((data.items.len(), bytes_added))
    }

    /// Pop an element from the left (head) of a list.
    ///
    /// Returns (value, bytes_freed), or None if empty/invalid.
    pub fn lpop(&self, idx: u32, expected_gen: u16) -> Option<(Vec<u8>, usize)> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let mut guard = slot.data.lock();
        let data = guard.as_mut()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        data.items.pop_front().map(|v| {
            let bytes_freed = v.len() + 8;
            (v.to_vec(), bytes_freed)
        })
    }

    /// Pop an element from the right (tail) of a list.
    ///
    /// Returns (value, bytes_freed), or None if empty/invalid.
    pub fn rpop(&self, idx: u32, expected_gen: u16) -> Option<(Vec<u8>, usize)> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let mut guard = slot.data.lock();
        let data = guard.as_mut()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        data.items.pop_back().map(|v| {
            let bytes_freed = v.len() + 8;
            (v.to_vec(), bytes_freed)
        })
    }

    /// Pop multiple elements from the left (head) of a list.
    ///
    /// Returns (values, bytes_freed), or None if invalid.
    pub fn lpop_count(
        &self,
        idx: u32,
        expected_gen: u16,
        count: usize,
    ) -> Option<(Vec<Vec<u8>>, usize)> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let mut guard = slot.data.lock();
        let data = guard.as_mut()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        let mut result = Vec::with_capacity(count.min(data.items.len()));
        let mut bytes_freed = 0;
        for _ in 0..count {
            match data.items.pop_front() {
                Some(v) => {
                    bytes_freed += v.len() + 8;
                    result.push(v.to_vec());
                }
                None => break,
            }
        }
        Some((result, bytes_freed))
    }

    /// Pop multiple elements from the right (tail) of a list.
    ///
    /// Returns (values, bytes_freed), or None if invalid.
    pub fn rpop_count(
        &self,
        idx: u32,
        expected_gen: u16,
        count: usize,
    ) -> Option<(Vec<Vec<u8>>, usize)> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let mut guard = slot.data.lock();
        let data = guard.as_mut()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        let mut result = Vec::with_capacity(count.min(data.items.len()));
        let mut bytes_freed = 0;
        for _ in 0..count {
            match data.items.pop_back() {
                Some(v) => {
                    bytes_freed += v.len() + 8;
                    result.push(v.to_vec());
                }
                None => break,
            }
        }
        Some((result, bytes_freed))
    }

    /// Get a range of elements from a list.
    ///
    /// Indices are 0-based. Negative indices count from the end.
    pub fn lrange(
        &self,
        idx: u32,
        expected_gen: u16,
        start: i64,
        stop: i64,
    ) -> Option<Vec<Vec<u8>>> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.lock();
        let data = guard.as_ref()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        let len = data.items.len() as i64;
        if len == 0 {
            return Some(Vec::new());
        }

        // Normalize indices
        let start_idx = normalize_index(start, len);
        let stop_idx = normalize_index(stop, len);

        if start_idx > stop_idx || start_idx >= len as usize {
            return Some(Vec::new());
        }

        let stop_idx = stop_idx.min(len as usize - 1);

        Some(
            data.items
                .iter()
                .skip(start_idx)
                .take(stop_idx - start_idx + 1)
                .map(|v| v.to_vec())
                .collect(),
        )
    }

    /// Get the length of a list.
    pub fn llen(&self, idx: u32, expected_gen: u16) -> Option<usize> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.lock();
        let data = guard.as_ref()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        Some(data.items.len())
    }

    /// Get an element at a specific index.
    ///
    /// Negative indices count from the end.
    pub fn lindex(&self, idx: u32, expected_gen: u16, index: i64) -> Option<Vec<u8>> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.lock();
        let data = guard.as_ref()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        let len = data.items.len() as i64;
        if len == 0 {
            return None;
        }

        let normalized = normalize_index(index, len);
        if normalized >= len as usize {
            return None;
        }

        data.items.get(normalized).map(|v| v.to_vec())
    }

    /// Set the value at a specific index.
    ///
    /// Returns true if successful, false if index out of range.
    pub fn lset(&self, idx: u32, expected_gen: u16, index: i64, value: &[u8]) -> Option<bool> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let mut guard = slot.data.lock();
        let data = guard.as_mut()?;

        let len = data.items.len() as i64;
        if len == 0 {
            return Some(false);
        }

        let normalized = normalize_index(index, len);
        if normalized >= len as usize {
            return Some(false);
        }

        data.items[normalized] = value.into();
        Some(true)
    }

    /// Trim a list to the specified range.
    pub fn ltrim(&self, idx: u32, expected_gen: u16, start: i64, stop: i64) -> Option<()> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let mut guard = slot.data.lock();
        let data = guard.as_mut()?;

        let len = data.items.len() as i64;
        if len == 0 {
            return Some(());
        }

        let start_idx = normalize_index(start, len);
        let stop_idx = normalize_index(stop, len);

        if start_idx > stop_idx || start_idx >= len as usize {
            data.items.clear();
            return Some(());
        }

        let stop_idx = stop_idx.min(len as usize - 1);

        // Remove from the end first
        data.items.truncate(stop_idx + 1);
        // Remove from the front
        data.items.drain(..start_idx);

        Some(())
    }

    /// Verify that the key at a slot matches the expected key.
    pub fn verify_key(&self, idx: u32, expected_gen: u16, key: &[u8]) -> bool {
        let Some(slot) = self.get(idx) else {
            return false;
        };
        if slot.generation() != expected_gen {
            return false;
        }

        let guard = slot.data.lock();
        match guard.as_ref() {
            Some(data) => data.key.as_ref() == key,
            None => false,
        }
    }

    /// Check if the list is empty.
    pub fn is_empty(&self, idx: u32, expected_gen: u16) -> Option<bool> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.lock();
        let data = guard.as_ref()?;
        Some(data.items.is_empty())
    }

    /// Get estimated size of a list in bytes.
    pub fn size_bytes(&self, idx: u32, expected_gen: u16) -> Option<usize> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.lock();
        let data = guard.as_ref()?;

        // Base size: key + metadata
        let mut size = data.key.len() + 16; // key + expire_at + cas_token

        // Items size
        for item in &data.items {
            size += item.len() + 8; // item + Box overhead
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
    let now_secs = now.duration_since(UnixInstant::EPOCH).as_secs();
    now_secs.saturating_add(ttl.as_secs() as u32)
}

/// Check if an expiration timestamp has passed.
fn is_expired(expire_at: u32) -> bool {
    use clocksource::coarse::UnixInstant;
    let now = UnixInstant::now();
    let now_secs = now.duration_since(UnixInstant::EPOCH).as_secs();
    now_secs >= expire_at
}

/// Normalize a Redis-style index to a positive index.
///
/// Negative indices count from the end (-1 = last element).
fn normalize_index(index: i64, len: i64) -> usize {
    if index < 0 {
        let normalized = len + index;
        if normalized < 0 {
            0
        } else {
            normalized as usize
        }
    } else {
        index as usize
    }
}

// Ensure ListStorage is Send + Sync
unsafe impl Send for ListStorage {}
unsafe impl Sync for ListStorage {}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_list_storage_basic() {
        let storage = ListStorage::new(10);

        // Allocate a slot
        let (idx, _) = storage.allocate().unwrap();
        assert_eq!(storage.occupied(), 1);

        // Initialize with a list
        let generation = storage.init_slot(idx, b"mylist", None, 1).unwrap();

        // Push elements - returns (new_length, bytes_added)
        assert_eq!(
            storage
                .rpush(idx, generation, &[b"a", b"b", b"c"])
                .unwrap()
                .0,
            3
        );
        assert_eq!(storage.lpush(idx, generation, &[b"z"]).unwrap().0, 4);

        // Check length
        assert_eq!(storage.llen(idx, generation).unwrap(), 4);

        // Check order: z, a, b, c
        assert_eq!(storage.lindex(idx, generation, 0).unwrap(), b"z");
        assert_eq!(storage.lindex(idx, generation, 1).unwrap(), b"a");
        assert_eq!(storage.lindex(idx, generation, -1).unwrap(), b"c");

        // Pop elements - returns (value, bytes_freed)
        assert_eq!(storage.lpop(idx, generation).unwrap().0, b"z");
        assert_eq!(storage.rpop(idx, generation).unwrap().0, b"c");
        assert_eq!(storage.llen(idx, generation).unwrap(), 2);

        // Deallocate
        storage.deallocate(idx);
        assert_eq!(storage.occupied(), 0);
    }

    #[test]
    fn test_list_lrange() {
        let storage = ListStorage::new(10);
        let (idx, _) = storage.allocate().unwrap();
        let generation = storage.init_slot(idx, b"mylist", None, 1).unwrap();

        storage
            .rpush(idx, generation, &[b"a", b"b", b"c", b"d", b"e"])
            .unwrap();

        // Full range
        let all = storage.lrange(idx, generation, 0, -1).unwrap();
        assert_eq!(all.len(), 5);

        // Partial range
        let partial = storage.lrange(idx, generation, 1, 3).unwrap();
        assert_eq!(partial, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);

        // Negative indices
        let last_two = storage.lrange(idx, generation, -2, -1).unwrap();
        assert_eq!(last_two, vec![b"d".to_vec(), b"e".to_vec()]);
    }

    #[test]
    fn test_list_lset_ltrim() {
        let storage = ListStorage::new(10);
        let (idx, _) = storage.allocate().unwrap();
        let generation = storage.init_slot(idx, b"mylist", None, 1).unwrap();

        storage
            .rpush(idx, generation, &[b"a", b"b", b"c", b"d", b"e"])
            .unwrap();

        // Set element
        assert!(storage.lset(idx, generation, 2, b"X").unwrap());
        assert_eq!(storage.lindex(idx, generation, 2).unwrap(), b"X");

        // Trim
        storage.ltrim(idx, generation, 1, 3).unwrap();
        assert_eq!(storage.llen(idx, generation).unwrap(), 3);
        assert_eq!(storage.lindex(idx, generation, 0).unwrap(), b"b");
    }

    #[test]
    fn test_normalize_index() {
        assert_eq!(normalize_index(0, 5), 0);
        assert_eq!(normalize_index(2, 5), 2);
        assert_eq!(normalize_index(-1, 5), 4);
        assert_eq!(normalize_index(-5, 5), 0);
        assert_eq!(normalize_index(-10, 5), 0); // Clamps to 0
    }
}
