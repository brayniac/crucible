//! Set storage for Redis-like set data structures.
//!
//! Provides per-slot RwLock protected set storage with efficient small/large
//! encoding for members.
//!
//! # Design
//!
//! - Each slot holds a set (collection of unique members)
//! - Uses RwLock for concurrent read access with exclusive write
//! - Small sets (≤16 members) use SmallVec sorted for binary search
//! - Large sets switch to HashSet for O(1) lookup
//! - Free list uses Treiber stack (lock-free allocation/deallocation)

use parking_lot::RwLock;
use smallvec::SmallVec;
use std::collections::HashSet;
use std::time::Duration;

use crate::sync::{AtomicU16, AtomicU32, AtomicU64, Ordering, spin_loop};

/// Threshold for switching from SmallVec to HashSet.
const SMALL_SET_THRESHOLD: usize = 16;

/// Sentinel value indicating an empty free list.
const EMPTY_FREE_LIST: u32 = u32::MAX;

/// Storage for set data structures.
///
/// Uses RwLock per slot for read-heavy workloads while allowing atomic member updates.
pub struct SetStorage {
    /// Slots containing set data.
    slots: Vec<SetSlot>,
    /// Head of the free list (Treiber stack with version tag).
    free_head: AtomicU64,
    /// Number of currently occupied slots.
    occupied_count: AtomicU32,
}

/// A single set slot with RwLock protection.
pub struct SetSlot {
    /// Generation counter for ABA protection.
    generation: AtomicU16,
    /// Next free slot index (when in free list).
    next_free: AtomicU32,
    /// Set data protected by RwLock.
    data: RwLock<Option<SetSlotData>>,
}

/// Internal set data for an occupied slot.
struct SetSlotData {
    /// The key this set is stored under (for verification).
    key: Box<[u8]>,
    /// Set members (small or large representation).
    members: SetMembers,
    /// Expiration time in seconds since Unix epoch (0 = no expiry).
    expire_at: u32,
    /// CAS token for optimistic locking (reserved for future use).
    #[allow(dead_code)]
    cas_token: u64,
}

/// Set members storage - either small (sorted SmallVec) or large (HashSet).
enum SetMembers {
    /// Small set with up to 16 members inline, sorted for binary search.
    Small(SmallVec<[Box<[u8]>; SMALL_SET_THRESHOLD]>),
    /// Large set using HashSet for O(1) lookup.
    Large(HashSet<Box<[u8]>>),
}

impl SetMembers {
    /// Create a new empty set members storage.
    fn new() -> Self {
        SetMembers::Small(SmallVec::new())
    }

    /// Add a member. Returns true if the member was newly added.
    /// Add a member, returning the bytes added if new (0 if already exists).
    fn add(&mut self, member: &[u8]) -> usize {
        match self {
            SetMembers::Small(vec) => {
                // Binary search to check if exists
                match vec.binary_search_by(|m| m.as_ref().cmp(member)) {
                    Ok(_) => 0, // Already exists
                    Err(pos) => {
                        let size = member.len() + 8; // member + Box overhead
                        if vec.len() < SMALL_SET_THRESHOLD {
                            vec.insert(pos, member.into());
                            size
                        } else {
                            // Upgrade to large
                            let mut set: HashSet<Box<[u8]>> = vec.drain(..).collect();
                            set.insert(member.into());
                            *self = SetMembers::Large(set);
                            size
                        }
                    }
                }
            }
            SetMembers::Large(set) => {
                if set.insert(member.into()) {
                    member.len() + 16 // member + HashSet overhead
                } else {
                    0
                }
            }
        }
    }

    /// Remove a member. Returns the bytes freed if removed, 0 otherwise.
    fn remove(&mut self, member: &[u8]) -> usize {
        match self {
            SetMembers::Small(vec) => {
                match vec.binary_search_by(|m| m.as_ref().cmp(member)) {
                    Ok(pos) => {
                        let m = vec.remove(pos);
                        m.len() + 8 // member + Box overhead
                    }
                    Err(_) => 0,
                }
            }
            SetMembers::Large(set) => {
                if set.remove(member) {
                    member.len() + 16 // member + HashSet overhead
                } else {
                    0
                }
            }
        }
    }

    /// Check if a member exists.
    fn contains(&self, member: &[u8]) -> bool {
        match self {
            SetMembers::Small(vec) => vec.binary_search_by(|m| m.as_ref().cmp(member)).is_ok(),
            SetMembers::Large(set) => set.contains(member),
        }
    }

    /// Get the number of members.
    fn len(&self) -> usize {
        match self {
            SetMembers::Small(vec) => vec.len(),
            SetMembers::Large(set) => set.len(),
        }
    }

    /// Check if empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get all members.
    fn members(&self) -> Vec<Vec<u8>> {
        match self {
            SetMembers::Small(vec) => vec.iter().map(|m| m.to_vec()).collect(),
            SetMembers::Large(set) => set.iter().map(|m| m.to_vec()).collect(),
        }
    }

    /// Pop a random member.
    /// Pop a member, returning (member, bytes_freed).
    fn pop(&mut self) -> Option<(Vec<u8>, usize)> {
        match self {
            SetMembers::Small(vec) => {
                if vec.is_empty() {
                    None
                } else {
                    // Use a simple "random" selection - just take the last one
                    // (for true randomness, would need to integrate with rand)
                    let m = vec.pop().unwrap();
                    let bytes_freed = m.len() + 8;
                    Some((m.to_vec(), bytes_freed))
                }
            }
            SetMembers::Large(set) => {
                // HashSet doesn't have pop, so take first element
                let member = set.iter().next()?.to_vec();
                let bytes_freed = member.len() + 16;
                set.remove(&member as &[u8]);
                Some((member, bytes_freed))
            }
        }
    }

    /// Get a random member without removing.
    fn random_member(&self) -> Option<Vec<u8>> {
        match self {
            SetMembers::Small(vec) => vec.last().map(|m| m.to_vec()),
            SetMembers::Large(set) => set.iter().next().map(|m| m.to_vec()),
        }
    }
}

impl SetSlot {
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

impl SetStorage {
    /// Create a new set storage with the given capacity.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be positive");
        assert!(
            capacity <= u32::MAX as usize,
            "capacity exceeds maximum slot index"
        );

        let mut slots = Vec::with_capacity(capacity);

        // Initialize all slots and build free list
        for i in 0..capacity {
            slots.push(SetSlot::new());
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
    pub fn get(&self, idx: u32) -> Option<&SetSlot> {
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

    /// Initialize a slot with a new empty set.
    pub fn init_slot(
        &self,
        idx: u32,
        key: &[u8],
        ttl: Option<Duration>,
        cas_token: u64,
    ) -> Option<u16> {
        let slot = self.get(idx)?;
        let expire_at = ttl.map(|d| expire_timestamp(d)).unwrap_or(0);

        let data = SetSlotData {
            key: key.into(),
            members: SetMembers::new(),
            expire_at,
            cas_token,
        };

        {
            let mut guard = slot.data.write();
            *guard = Some(data);
        }

        Some(slot.generation())
    }

    /// Add members to a set.
    ///
    /// Returns (members_added, bytes_added), or None if slot invalid.
    pub fn sadd(&self, idx: u32, expected_gen: u16, members: &[&[u8]]) -> Option<(usize, usize)> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let mut guard = slot.data.write();
        let data = guard.as_mut()?;

        let mut added = 0;
        let mut bytes_added = 0;
        for member in members {
            let bytes = data.members.add(member);
            if bytes > 0 {
                added += 1;
                bytes_added += bytes;
            }
        }
        Some((added, bytes_added))
    }

    /// Remove members from a set.
    ///
    /// Returns (members_removed, bytes_freed), or None if slot invalid.
    pub fn srem(&self, idx: u32, expected_gen: u16, members: &[&[u8]]) -> Option<(usize, usize)> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let mut guard = slot.data.write();
        let data = guard.as_mut()?;

        let mut removed = 0;
        let mut bytes_freed = 0;
        for member in members {
            let bytes = data.members.remove(member);
            if bytes > 0 {
                removed += 1;
                bytes_freed += bytes;
            }
        }
        Some((removed, bytes_freed))
    }

    /// Get all members of a set.
    pub fn smembers(&self, idx: u32, expected_gen: u16) -> Option<Vec<Vec<u8>>> {
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

        Some(data.members.members())
    }

    /// Check if a member exists in a set.
    pub fn sismember(&self, idx: u32, expected_gen: u16, member: &[u8]) -> Option<bool> {
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

        Some(data.members.contains(member))
    }

    /// Check if multiple members exist in a set.
    pub fn smismember(&self, idx: u32, expected_gen: u16, members: &[&[u8]]) -> Option<Vec<bool>> {
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

        Some(members.iter().map(|m| data.members.contains(m)).collect())
    }

    /// Get the number of members in a set.
    pub fn scard(&self, idx: u32, expected_gen: u16) -> Option<usize> {
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

        Some(data.members.len())
    }

    /// Pop a random member from a set.
    ///
    /// Returns (member, bytes_freed), or None if empty/invalid.
    pub fn spop(&self, idx: u32, expected_gen: u16) -> Option<(Vec<u8>, usize)> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let mut guard = slot.data.write();
        let data = guard.as_mut()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        data.members.pop()
    }

    /// Pop multiple random members from a set.
    ///
    /// Returns (members, bytes_freed), or None if invalid.
    pub fn spop_count(
        &self,
        idx: u32,
        expected_gen: u16,
        count: usize,
    ) -> Option<(Vec<Vec<u8>>, usize)> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let mut guard = slot.data.write();
        let data = guard.as_mut()?;

        // Check expiration
        if data.expire_at != 0 && is_expired(data.expire_at) {
            return None;
        }

        let mut result = Vec::with_capacity(count.min(data.members.len()));
        let mut bytes_freed = 0;
        for _ in 0..count {
            match data.members.pop() {
                Some((m, bytes)) => {
                    bytes_freed += bytes;
                    result.push(m);
                }
                None => break,
            }
        }
        Some((result, bytes_freed))
    }

    /// Get a random member without removing.
    pub fn srandmember(&self, idx: u32, expected_gen: u16) -> Option<Vec<u8>> {
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

        data.members.random_member()
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

    /// Check if the set is empty.
    pub fn is_empty(&self, idx: u32, expected_gen: u16) -> Option<bool> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.read();
        let data = guard.as_ref()?;
        Some(data.members.is_empty())
    }

    /// Get estimated size of a set in bytes.
    pub fn size_bytes(&self, idx: u32, expected_gen: u16) -> Option<usize> {
        let slot = self.get(idx)?;
        if slot.generation() != expected_gen {
            return None;
        }

        let guard = slot.data.read();
        let data = guard.as_ref()?;

        // Base size: key + metadata
        let mut size = data.key.len() + 16; // key + expire_at + cas_token

        // Members size
        match &data.members {
            SetMembers::Small(vec) => {
                for m in vec.iter() {
                    size += m.len() + 8; // member + Box overhead
                }
            }
            SetMembers::Large(set) => {
                for m in set.iter() {
                    size += m.len() + 24; // member + HashSet entry overhead
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

// Ensure SetStorage is Send + Sync
unsafe impl Send for SetStorage {}
unsafe impl Sync for SetStorage {}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_set_members_small() {
        let mut members = SetMembers::new();

        // Add members - returns bytes_added (> 0 for new, 0 for existing)
        assert!(members.add(b"b") > 0);
        assert!(members.add(b"a") > 0);
        assert!(members.add(b"c") > 0);
        assert_eq!(members.add(b"a"), 0); // Already exists

        assert_eq!(members.len(), 3);
        assert!(members.contains(b"a"));
        assert!(members.contains(b"b"));
        assert!(members.contains(b"c"));
        assert!(!members.contains(b"d"));

        // Remove - returns bytes_freed (> 0 for removed, 0 for not found)
        assert!(members.remove(b"b") > 0);
        assert_eq!(members.remove(b"b"), 0); // Already removed
        assert_eq!(members.len(), 2);
    }

    #[test]
    fn test_set_members_upgrade_to_large() {
        let mut members = SetMembers::new();

        // Fill up to threshold
        for i in 0..SMALL_SET_THRESHOLD {
            assert!(members.add(format!("member{:02}", i).as_bytes()) > 0);
        }

        // Should still be small
        assert!(matches!(members, SetMembers::Small(_)));

        // One more should upgrade
        assert!(members.add(b"overflow") > 0);
        assert!(matches!(members, SetMembers::Large(_)));
        assert_eq!(members.len(), SMALL_SET_THRESHOLD + 1);
    }

    #[test]
    fn test_set_storage_basic() {
        let storage = SetStorage::new(10);

        // Allocate a slot
        let (idx, _) = storage.allocate().unwrap();
        assert_eq!(storage.occupied(), 1);

        // Initialize with a set
        let generation = storage.init_slot(idx, b"myset", None, 1).unwrap();

        // Add members - returns (added_count, bytes_added)
        assert_eq!(
            storage
                .sadd(idx, generation, &[b"a", b"b", b"c"])
                .unwrap()
                .0,
            3
        );
        assert_eq!(storage.sadd(idx, generation, &[b"a", b"d"]).unwrap().0, 1); // a already exists

        // Check cardinality
        assert_eq!(storage.scard(idx, generation).unwrap(), 4);

        // Check membership
        assert!(storage.sismember(idx, generation, b"a").unwrap());
        assert!(storage.sismember(idx, generation, b"d").unwrap());
        assert!(!storage.sismember(idx, generation, b"z").unwrap());

        // Remove member - returns (removed_count, bytes_freed)
        assert_eq!(storage.srem(idx, generation, &[b"a"]).unwrap().0, 1);
        assert_eq!(storage.scard(idx, generation).unwrap(), 3);

        // Deallocate
        storage.deallocate(idx);
        assert_eq!(storage.occupied(), 0);
    }

    #[test]
    fn test_set_storage_smembers() {
        let storage = SetStorage::new(10);
        let (idx, _) = storage.allocate().unwrap();
        let generation = storage.init_slot(idx, b"myset", None, 1).unwrap();

        storage.sadd(idx, generation, &[b"x", b"y", b"z"]).unwrap();

        let members = storage.smembers(idx, generation).unwrap();
        assert_eq!(members.len(), 3);
        // Note: order is sorted in small set
    }

    #[test]
    fn test_set_storage_smismember() {
        let storage = SetStorage::new(10);
        let (idx, _) = storage.allocate().unwrap();
        let generation = storage.init_slot(idx, b"myset", None, 1).unwrap();

        storage.sadd(idx, generation, &[b"a", b"b"]).unwrap();

        let results = storage
            .smismember(idx, generation, &[b"a", b"b", b"c"])
            .unwrap();
        assert_eq!(results, vec![true, true, false]);
    }

    #[test]
    fn test_set_storage_spop() {
        let storage = SetStorage::new(10);
        let (idx, _) = storage.allocate().unwrap();
        let generation = storage.init_slot(idx, b"myset", None, 1).unwrap();

        storage.sadd(idx, generation, &[b"a", b"b", b"c"]).unwrap();

        // Pop one - returns (member, bytes_freed)
        let (popped, _bytes) = storage.spop(idx, generation).unwrap();
        assert!([b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].contains(&popped));
        assert_eq!(storage.scard(idx, generation).unwrap(), 2);

        // Pop multiple - returns (members, bytes_freed)
        let (popped, _bytes) = storage.spop_count(idx, generation, 5).unwrap();
        assert_eq!(popped.len(), 2); // Only 2 left
        assert_eq!(storage.scard(idx, generation).unwrap(), 0);
    }
}
