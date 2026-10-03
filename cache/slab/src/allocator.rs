//! Slab allocator managing memory pools and slab classes.
//!
//! The allocator coordinates between slab classes, allocating new slabs
//! from the heap when needed and managing eviction when memory is exhausted.

use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use crate::sync::{AtomicU32, Ordering};

use cache_core::{Hashtable, HugepageAllocation, allocate_on_node};
use crossbeam_deque::Injector;

use crate::class::SlabClass;
use crate::config::{EvictionStrategy, HEADER_SIZE, SlabCacheConfig, SlabClasses};
use crate::item::{SlabItemHeader, pack_slot_ref, unpack_slot_ref};
use crate::location::{MAX_SLOT_INDEX, SlabLocation};
use crate::verifier::SlabVerifier;

/// A slab reference and slot pin on one item, released on drop.
pub struct ItemPin<'a> {
    class: &'a SlabClass,
    slab_id: u32,
    slot_index: u32,
}

impl ItemPin<'_> {
    /// The pinned item's header.
    #[inline]
    pub fn header(&self) -> &SlabItemHeader {
        // SAFETY: the slab reference and slot pin keep the slot's memory in
        // this class and keep it from being rewritten.
        unsafe { self.class.header(self.slab_id, self.slot_index) }
    }

    /// Convert into a `ValueRef` over the item's value that keeps the slab
    /// reference and slot pin until it is dropped.
    pub fn into_value_ref(self) -> cache_core::ValueRef {
        let header = self.header();
        // SAFETY: a pinned slot holds a published item.
        let (value_ptr, value_len) = (unsafe { header.value_ptr() }, header.value_len());
        let ref_count = self.class.state_word(self.slab_id) as *const AtomicU32;
        let ctx = self.class as *const SlabClass as *const ();
        let arg = pack_slot_ref(self.slab_id, self.slot_index);
        std::mem::forget(self);
        // SAFETY: `ref_count` is the slab's state word, on which this pin
        // holds a reference; `ValueRef::drop` releases it after the hook
        // unpins the slot. `ctx` is the `SlabClass`, in the same `SlabCache`
        // as `ref_count`; both are valid while the `ValueRef` does not
        // outlive the cache.
        unsafe {
            cache_core::ValueRef::new(
                ref_count,
                value_ptr,
                value_len,
                std::ptr::null(),
                std::ptr::null(),
                0,
            )
            .with_release_hook(unpin_slot_hook, ctx, arg)
        }
    }
}

impl Drop for ItemPin<'_> {
    fn drop(&mut self) {
        self.class.unpin_slot(self.slab_id, self.slot_index);
        self.class.release_slab(self.slab_id);
    }
}

/// `ValueRef` release hook for a slab item: `ctx` is the `SlabClass` and
/// `arg` the packed slot.
unsafe fn unpin_slot_hook(ctx: *const (), arg: u64) {
    // SAFETY: `into_value_ref` passes a `SlabClass` pointer that outlives
    // the `ValueRef`.
    let class = unsafe { &*(ctx as *const SlabClass) };
    let (slab_id, slot_index) = unpack_slot_ref(arg);
    class.unpin_slot(slab_id, slot_index);
}

/// The main slab allocator.
pub struct SlabAllocator {
    /// Slab classes (indexed by class_id).
    classes: Vec<SlabClass>,
    /// Slab class configuration (slot sizes).
    slab_classes: SlabClasses,
    /// Heap memory allocation.
    heap: HugepageAllocation,
    /// Slab size in bytes.
    slab_size: usize,
    /// Total memory limit.
    memory_limit: usize,
    /// Current memory used (number of slabs allocated * slab_size).
    memory_used: AtomicUsize,
    /// Free slab pages (pointers to unallocated slab memory).
    /// Lock-free: multiple threads can steal slabs concurrently.
    free_slabs: Injector<*mut u8>,
    /// Eviction strategy (twemcache-style).
    eviction_strategy: EvictionStrategy,
}

// Safety: The allocator manages heap memory safely.
unsafe impl Send for SlabAllocator {}
unsafe impl Sync for SlabAllocator {}

impl SlabAllocator {
    /// Create a new slab allocator.
    pub fn new(config: &SlabCacheConfig) -> Result<Self, std::io::Error> {
        // A class_id has 6 bits. Checked here so `build` returns an error
        // instead of reaching the assert in `SlabClasses::from_config`.
        let class_count = config.generate_classes().len();
        if class_count > SlabClasses::MAX_CLASSES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "slab_size {} with growth_factor {} gives {} slab classes, more than {}; \
                     raise growth_factor or reduce slab_size",
                    config.slab_size,
                    config.growth_factor,
                    class_count,
                    SlabClasses::MAX_CLASSES
                ),
            ));
        }
        let slab_classes = SlabClasses::from_config(config);

        // The smallest class has the most slots per slab, and every slot
        // index has to fit in a location word.
        if let Some(&min_slot) = slab_classes.sizes().first() {
            let slots = config.slab_size / min_slot;
            if slots > MAX_SLOT_INDEX as usize + 1 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "slab_size {} with a smallest slot of {} bytes gives {} slots per slab; \
                         a slab can hold at most {} slots. Reduce slab_size or raise min_slot_size",
                        config.slab_size,
                        min_slot,
                        slots,
                        MAX_SLOT_INDEX as usize + 1
                    ),
                ));
            }
        }

        // Allocate the heap
        let heap = allocate_on_node(config.heap_size, config.hugepage_size, config.numa_node)?;

        // Create slab class instances
        let classes: Vec<SlabClass> = slab_classes
            .sizes()
            .iter()
            .enumerate()
            .map(|(i, &slot_size)| SlabClass::new(i as u8, slot_size, config.slab_size))
            .collect();

        // Initialize free slab list
        let free_slabs = Injector::new();
        let slab_count = config.heap_size / config.slab_size;
        let heap_ptr = heap.as_ptr();

        for i in 0..slab_count {
            let slab_ptr = unsafe { heap_ptr.add(i * config.slab_size) };
            free_slabs.push(slab_ptr);
        }

        Ok(Self {
            classes,
            slab_classes,
            heap,
            slab_size: config.slab_size,
            memory_limit: config.heap_size,
            memory_used: AtomicUsize::new(0),
            free_slabs,
            eviction_strategy: config.eviction_strategy,
        })
    }

    /// Select the smallest class that fits an item.
    ///
    /// `item_size` should be `key.len() + value.len() + HEADER_SIZE`.
    #[inline]
    pub fn select_class(&self, item_size: usize) -> Option<u8> {
        self.slab_classes.select_class(item_size)
    }

    /// Get a reference to a slab class.
    #[inline]
    pub fn class(&self, class_id: u8) -> Option<&SlabClass> {
        self.classes.get(class_id as usize)
    }

    /// Check if a slab is in Live state (readable).
    ///
    /// This is a quick check for the verifier to avoid reading from evicted
    /// slabs. Note that this check is racy - use `pin_item` to read an item.
    #[allow(dead_code)]
    #[inline]
    pub fn is_slab_live(&self, class_id: u8, slab_id: u32) -> bool {
        self.classes
            .get(class_id as usize)
            .map(|c| c.is_slab_live(slab_id))
            .unwrap_or(false)
    }

    /// Allocate a slot for an item.
    ///
    /// Returns `Some((slab_id, slot_index))` if successful.
    /// May allocate a new slab if needed.
    pub fn allocate(&self, class_id: u8) -> Option<(u32, u32)> {
        let class = self.classes.get(class_id as usize)?;

        // Try to allocate from the class's free list
        if let Some(slot) = class.allocate() {
            return Some(slot);
        }

        // Need to allocate a new slab for this class
        self.allocate_slab_for_class(class_id)
    }

    /// Allocate a new slab for a class.
    ///
    /// This is lock-free: multiple threads can concurrently allocate slabs
    /// for different (or even the same) classes. If two threads allocate
    /// slabs for the same class simultaneously, both slabs are added and
    /// used - this is intentional to avoid lock contention.
    fn allocate_slab_for_class(&self, class_id: u8) -> Option<(u32, u32)> {
        let class = self.classes.get(class_id as usize)?;

        // Try to get a free slab from the pool (lock-free steal)
        let slab_ptr = match self.free_slabs.steal() {
            crossbeam_deque::Steal::Success(ptr) => ptr,
            _ => return None, // No free slabs available
        };

        // Update memory usage
        self.memory_used
            .fetch_add(self.slab_size, Ordering::Relaxed);

        // Add the slab to the class (internally synchronized via slabs write lock)
        unsafe {
            class.add_slab(slab_ptr, self.slab_size);
        }

        // Now allocate from the class (lock-free)
        class.allocate()
    }

    /// Retire an item that has been removed from the hashtable, or never
    /// inserted: drop it from the class statistics, mark it deleted, and free
    /// its slot once no reader holds it.
    ///
    /// The caller must hold a reference on the item's slab: the write ref
    /// from allocation, or an `ItemPin` on the item.
    pub fn retire_held_item(&self, location: SlabLocation) {
        let (class_id, slab_id, slot_index) = location.unpack();
        if let Some(class) = self.classes.get(class_id as usize) {
            Self::retire_in_class(class, slab_id, slot_index);
        }
    }

    fn retire_in_class(class: &SlabClass, slab_id: u32, slot_index: u32) {
        // SAFETY: the caller's slab reference keeps the slab in this class,
        // and the caller owns this slot's retirement: it removed the slot's
        // hashtable entry or never inserted one.
        unsafe {
            let header = class.header(slab_id, slot_index);
            class.sub_bytes(header.item_size());
            class.remove_item();
            header.mark_deleted();
        }
        class.free_slot(slab_id, slot_index);
    }

    /// Hold the item at `location` for reading if it is live and its key is
    /// `key`. The slab cannot be evicted and the slot cannot be reused until
    /// the returned pin is dropped.
    ///
    /// Returns `None` if the slab is draining or gone, or the slot has been
    /// freed or now holds a different key; the caller can look the key up
    /// again.
    pub fn pin_item(&self, location: SlabLocation, key: &[u8]) -> Option<ItemPin<'_>> {
        let pin = self.pin_slot(location)?;
        let header = pin.header();
        // SAFETY: a pinned slot is published, so its header and key are
        // fully written and not being rewritten.
        if header.is_deleted() || unsafe { header.key() } != key {
            return None;
        }
        Some(pin)
    }

    /// Hold the slot at `location` for reading if it holds a published item,
    /// whatever its key and deleted flag. Returns `None` if the slab is
    /// draining or gone, or the slot is free or being written.
    pub fn pin_slot(&self, location: SlabLocation) -> Option<ItemPin<'_>> {
        let (class_id, slab_id, slot_index) = location.unpack();
        let class = self.classes.get(class_id as usize)?;
        if !class.acquire_slab(slab_id) {
            return None;
        }
        if !class.pin_slot(slab_id, slot_index) {
            class.release_slab(slab_id);
            return None;
        }
        Some(ItemPin {
            class,
            slab_id,
            slot_index,
        })
    }

    /// Release a slab reference acquired during allocation.
    ///
    /// Must be called after a successful `allocate()` once the write is complete.
    /// This decrements the slab's ref_count, allowing eviction to proceed.
    #[inline]
    pub fn release_write_ref(&self, class_id: u8, slab_id: u32) {
        if let Some(class) = self.classes.get(class_id as usize) {
            class.release_slab(slab_id);
        }
    }

    /// Maximum attempts to allocate with eviction before giving up.
    /// This handles the case where multiple threads are racing to evict
    /// and allocate. If our eviction attempt fails (another thread was
    /// already evicting), we should retry allocation since that thread
    /// may have freed space.
    const MAX_EVICTION_ATTEMPTS: usize = 8;

    /// Allocate a slot with eviction if needed.
    ///
    /// Tries to allocate a slot. If no free slots or slabs are available,
    /// uses the configured eviction strategy to free up space.
    ///
    /// Eviction strategies are tried in order from highest to lowest bit:
    /// - SLAB_LRC (8) - Evict least recently created slab
    /// - SLAB_LRA (4) - Evict least recently accessed slab
    /// - RANDOM (2) - Evict a random slab
    ///
    /// If no eviction strategy is configured (NONE), returns `None` when full.
    /// Allocate a slot, evicting a slab if necessary, offering each evicted
    /// item to `demote` before it is discarded.
    ///
    /// Pass `|_| false` for a cache with no disk tier. See
    /// [`Self::evict_slab_with_demoter`] for the demoter's contract.
    pub fn allocate_with_eviction_and_demoter<H, D>(
        &self,
        class_id: u8,
        hashtable: &H,
        mut demote: D,
    ) -> Option<(u32, u32)>
    where
        H: Hashtable,
        D: FnMut(&crate::class::EvictedItem<'_>) -> bool,
    {
        // First try normal allocation
        if let Some(slot) = self.allocate(class_id) {
            return Some(slot);
        }

        // Check if eviction is disabled
        if self.eviction_strategy.is_none() {
            return None;
        }

        // Try slab-level eviction with retries.
        // We may need to retry because:
        // 1. Another thread may be evicting the same slab (our eviction fails)
        // 2. Another thread may have grabbed the freed slab before us
        // 3. Our allocation may fail due to stale free_slots entries from evicted slabs
        if self.eviction_strategy.has_slab_eviction() {
            for _ in 0..Self::MAX_EVICTION_ATTEMPTS {
                // Try eviction (may fail if another thread is evicting)
                let _ = self.try_slab_eviction_with_demoter(hashtable, &mut demote);

                // Always try allocation - another thread's eviction may have succeeded
                if let Some(slot) = self.allocate(class_id) {
                    return Some(slot);
                }

                // Yield to let other threads make progress
                std::thread::yield_now();
            }
        }

        None
    }

    /// Find the least recently accessed slab across all classes.
    ///
    /// Returns `(class_id, slab_id, sequence)` of the LRA slab, or `None` if
    /// no slabs exist.
    pub fn find_lra_slab(&self) -> Option<(u8, u32, u64)> {
        // Ordered by last access, then by age: access times have one-second
        // resolution, so ties are common and the older slab goes first.
        self.classes
            .iter()
            .enumerate()
            .flat_map(|(class_id, class)| {
                class.slab_timestamps().into_iter().map(
                    move |(slab_id, last_accessed, sequence)| {
                        (
                            (last_accessed, sequence),
                            (class_id as u8, slab_id, sequence),
                        )
                    },
                )
            })
            .min_by_key(|(key, _)| *key)
            .map(|(_, slab)| slab)
    }

    /// Find the least recently created slab across all classes.
    ///
    /// Returns `(class_id, slab_id, sequence)` of the slab added first, by
    /// `Slab::sequence`, or `None` if no slabs exist.
    pub fn find_lrc_slab(&self) -> Option<(u8, u32, u64)> {
        self.classes
            .iter()
            .enumerate()
            .flat_map(|(class_id, class)| {
                class
                    .slab_timestamps()
                    .into_iter()
                    .map(move |(slab_id, _, sequence)| {
                        (sequence, (class_id as u8, slab_id, sequence))
                    })
            })
            .min_by_key(|(sequence, _)| *sequence)
            .map(|(_, slab)| slab)
    }

    /// Find a random slab across all classes.
    ///
    /// Returns `(class_id, slab_id, sequence)` of a random Live slab, or
    /// `None` if no slabs exist.
    pub fn find_random_slab(&self) -> Option<(u8, u32, u64)> {
        // Collect (class_id, slab_id, sequence) for Live slabs only
        let mut slabs = Vec::new();
        for (class_id, class) in self.classes.iter().enumerate() {
            // slab_timestamps() only returns Live slabs (filters by state)
            for (slab_id, _, sequence) in class.slab_timestamps() {
                slabs.push((class_id as u8, slab_id, sequence));
            }
        }

        if slabs.is_empty() {
            return None;
        }

        // Simple pseudo-random selection using timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as usize)
            .unwrap_or(0);
        let idx = now % slabs.len();
        Some(slabs[idx])
    }

    /// Evict a slab: offer each item to `demote`, remove the hashtable
    /// entries of the items it does not take, and return the slab's memory to
    /// the global free pool. Returns `true` if the slab was evicted.
    ///
    /// With `sequence`, the slab is evicted only if the slab now using
    /// `slab_id` has that `Slab::sequence`. A victim chosen by a
    /// `find_*_slab` call passes its sequence, so if that slab has been
    /// evicted and its id given to a newer slab in the meantime, the newer
    /// slab is left alone.
    ///
    /// `demote` returns `true` if it took ownership of the item -- it has
    /// written the value elsewhere and repointed the hashtable entry, so this
    /// must NOT then remove that entry. Returning `false` means the item is
    /// being discarded and the entry has to go.
    ///
    /// The RAM copy is delete-marked either way, immediately after the
    /// callback returns. That is correct for a demoted item too: the hashtable
    /// now names the disk copy, and the drain has already waited for
    /// `ref_count` to reach zero, so no reader can be mid-read of the slot
    /// being marked.
    pub fn evict_slab_with_demoter<H, D>(
        &self,
        class_id: u8,
        slab_id: u32,
        sequence: Option<u64>,
        hashtable: &H,
        mut demote: D,
    ) -> bool
    where
        H: Hashtable,
        D: FnMut(&crate::class::EvictedItem<'_>) -> bool,
    {
        let class = match self.classes.get(class_id as usize) {
            Some(c) => c,
            None => return false,
        };

        // Evict all items from the slab
        let slab_ptr = unsafe {
            class.evict_slab(slab_id, sequence, |item| {
                let location =
                    SlabLocation::new(item.class_id, item.slab_id, item.slot_index).to_location();

                // Offer it to the disk tier first. A successful demote has
                // already repointed the hashtable entry at the disk copy via
                // `cas_location`, so removing it here would delete the item we
                // just saved.
                if demote(&item) {
                    return;
                }

                let key = item.key;
                if !hashtable.remove(key, location) {
                    // `remove` retries the slot for as long as it keeps
                    // publishing `location` (cache-core's
                    // `try_unlink_in_bucket`), so a `false` means the entry
                    // stopped naming this slot: a racing SET republished the
                    // key elsewhere, or it was already unlinked. Delete-marking
                    // the item we are about to free is right in both cases, so
                    // there is nothing to do here.
                    //
                    // This code depends on that retry, which lives in
                    // cache-core. `SlabClass::evict_slab` delete-marks unconditionally
                    // immediately after this callback returns, so if `remove`
                    // could fail while the slot still published `location` — as
                    // it could before the retry was added — the table would be
                    // left publishing a tombstone. That is exactly the
                    // counterexample to the claim `verify_slot` rests on: every
                    // writer moves or clears the slot before delete-marking what
                    // it superseded.
                    //
                    // Checked rather than assumed, so that if the retry is ever
                    // removed this fails loudly here instead of silently
                    // leaking a dangling entry.
                    debug_assert!(
                        hashtable.get_item_frequency(key, location).is_none(),
                        "evict_slab is about to delete-mark an item the \
                         hashtable still publishes"
                    );
                }
            })
        };

        if let Some(ptr) = slab_ptr {
            // Return the slab to the global free pool
            self.free_slabs.push(ptr);

            // Update memory accounting (slab is now free but memory is still allocated)
            // Note: We don't decrement memory_used because the slab is still in our heap,
            // just available for reuse by any class

            true
        } else {
            false
        }
    }

    /// Try slab-level eviction using the configured strategy.
    ///
    /// Tries strategies in order: SLAB_LRC (8), SLAB_LRA (4), RANDOM (2).
    /// Returns `true` if a slab was evicted.
    /// As [`Self::try_slab_eviction`], but offering each evicted item to
    /// `demote` before it is discarded. See
    /// [`Self::evict_slab_with_demoter`] for the contract.
    pub fn try_slab_eviction_with_demoter<H, D>(&self, hashtable: &H, mut demote: D) -> bool
    where
        H: Hashtable,
        D: FnMut(&crate::class::EvictedItem<'_>) -> bool,
    {
        let strategy = self.eviction_strategy;

        // Try strategies in order from highest to lowest bit
        // SLAB_LRC (8)
        if strategy.contains(EvictionStrategy::SLAB_LRC)
            && let Some((class_id, slab_id, sequence)) = self.find_lrc_slab()
            && self.evict_slab_with_demoter(
                class_id,
                slab_id,
                Some(sequence),
                hashtable,
                &mut demote,
            )
        {
            return true;
        }

        // SLAB_LRA (4)
        if strategy.contains(EvictionStrategy::SLAB_LRA)
            && let Some((class_id, slab_id, sequence)) = self.find_lra_slab()
            && self.evict_slab_with_demoter(
                class_id,
                slab_id,
                Some(sequence),
                hashtable,
                &mut demote,
            )
        {
            return true;
        }

        // RANDOM (2)
        if strategy.contains(EvictionStrategy::RANDOM)
            && let Some((class_id, slab_id, sequence)) = self.find_random_slab()
            && self.evict_slab_with_demoter(
                class_id,
                slab_id,
                Some(sequence),
                hashtable,
                &mut demote,
            )
        {
            return true;
        }

        false
    }

    /// Get the eviction strategy.
    #[allow(dead_code)]
    pub fn eviction_strategy(&self) -> EvictionStrategy {
        self.eviction_strategy
    }

    /// Write an item to a slot.
    ///
    /// # Safety
    ///
    /// The slot must be one `allocate` claimed for this write and has not
    /// been published. Publishes the slot.
    pub unsafe fn write_item(
        &self,
        class_id: u8,
        slab_id: u32,
        slot_index: u32,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) {
        // SAFETY: Caller ensures slot was allocated and doesn't contain live item
        unsafe {
            let class = &self.classes[class_id as usize];
            let ptr = class.slot_ptr(slab_id, slot_index);

            // Initialize header
            SlabItemHeader::init(ptr, key.len(), value.len(), ttl);

            // Copy key
            std::ptr::copy_nonoverlapping(key.as_ptr(), ptr.add(HEADER_SIZE), key.len());

            // Copy value
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                ptr.add(HEADER_SIZE + key.len()),
                value.len(),
            );

            // Update stats
            class.add_bytes(HEADER_SIZE + key.len() + value.len());
            class.add_item();

            class.publish_slot(slab_id, slot_index);
        }
    }

    /// Get a reference to the header at a location.
    ///
    /// # Safety
    ///
    /// The location must point to a valid item.
    #[cfg(test)]
    #[inline]
    pub unsafe fn header(&self, location: SlabLocation) -> &SlabItemHeader {
        // SAFETY: Caller ensures location points to valid item
        unsafe {
            let (class_id, slab_id, slot_index) = location.unpack();
            self.classes[class_id as usize].header(slab_id, slot_index)
        }
    }

    /// Get a pointer to the slot at a location.
    ///
    /// # Safety
    ///
    /// The location must be valid.
    #[allow(dead_code)]
    #[inline]
    pub unsafe fn slot_ptr(&self, location: SlabLocation) -> *mut u8 {
        // SAFETY: Caller ensures location is valid
        unsafe {
            let (class_id, slab_id, slot_index) = location.unpack();
            self.classes[class_id as usize].slot_ptr(slab_id, slot_index)
        }
    }

    /// Touch a slab for LRA tracking.
    pub fn touch_slab(&self, location: SlabLocation) {
        let (class_id, slab_id, _slot_index) = location.unpack();
        if let Some(class) = self.classes.get(class_id as usize) {
            class.touch_slab(slab_id);
        }
    }

    /// Begin a two-phase write operation for zero-copy receive.
    ///
    /// This method:
    /// 1. Selects the appropriate size class
    /// 2. Allocates a slot (may allocate new slab or trigger eviction)
    /// 3. Initializes the header with key_len, value_len, ttl
    /// 4. Copies the key to slot memory
    /// 5. Returns the location and a pointer to the value area
    ///
    /// The caller then writes the value directly to the returned pointer,
    /// then calls `finalize_write_item` to update statistics.
    ///
    /// # Arguments
    /// * `key` - The key for this item
    /// * `value_len` - Length of the value to be written
    /// * `ttl` - Time-to-live for this item
    /// * `hashtable` - Hashtable for eviction callbacks
    ///
    /// # Returns
    /// `Some((location, value_ptr, item_size))` on success, `None` if allocation fails.
    pub fn begin_write_item<H, D>(
        &self,
        key: &[u8],
        value_len: usize,
        ttl: Duration,
        hashtable: &H,
        mut demote: D,
    ) -> Option<(SlabLocation, *mut u8, usize)>
    where
        H: Hashtable,
        D: FnMut(&crate::class::EvictedItem<'_>) -> bool,
    {
        // Calculate item size
        let item_size = HEADER_SIZE + key.len() + value_len;

        // Select class
        let class_id = self.select_class(item_size)?;
        let class = self.classes.get(class_id as usize)?;

        // Try direct allocation first
        if let Some((slab_id, slot_index, value_ptr, item_size)) =
            class.begin_write_item(key, value_len, ttl)
        {
            let location = SlabLocation::new(class_id, slab_id, slot_index);
            return Some((location, value_ptr, item_size));
        }

        // Need to allocate a new slab or evict
        // First try allocating a new slab
        if self.allocate_slab_for_class(class_id).is_some() {
            // Try allocation again
            if let Some((slab_id, slot_index, value_ptr, item_size)) =
                class.begin_write_item(key, value_len, ttl)
            {
                let location = SlabLocation::new(class_id, slab_id, slot_index);
                return Some((location, value_ptr, item_size));
            }
        }

        // Check if eviction is enabled
        if self.eviction_strategy.is_none() {
            return None;
        }

        // Try slab-level eviction
        if self.eviction_strategy.has_slab_eviction()
            && self.try_slab_eviction_with_demoter(hashtable, &mut demote)
        {
            // Eviction freed some slots, try allocation again
            if let Some((slab_id, slot_index, value_ptr, item_size)) =
                class.begin_write_item(key, value_len, ttl)
            {
                let location = SlabLocation::new(class_id, slab_id, slot_index);
                return Some((location, value_ptr, item_size));
            }
        }

        None
    }

    /// Finalize a two-phase write operation.
    ///
    /// Called after the value has been written to the pointer returned by
    /// `begin_write_item`. Updates statistics (bytes_used, item_count) and
    /// makes the slot readable. The write ref taken by `allocate` is still
    /// held; the caller releases it after inserting the location into the
    /// hashtable.
    ///
    /// # Arguments
    /// * `location` - The location returned by `begin_write_item`
    /// * `item_size` - The item_size returned by `begin_write_item`
    pub fn finalize_write_item(&self, location: SlabLocation, item_size: usize) {
        let (class_id, slab_id, slot_index) = location.unpack();
        if let Some(class) = self.classes.get(class_id as usize) {
            class.finalize_write_item(slab_id, slot_index, item_size);
        }
    }

    /// Cancel a two-phase write operation.
    ///
    /// Called if the write cannot be completed (e.g., connection closed),
    /// before `finalize_write_item`. Marks the item deleted, frees the
    /// slot, and releases the write ref.
    ///
    /// # Arguments
    /// * `location` - The location returned by `begin_write_item`
    pub fn cancel_write_item(&self, location: SlabLocation) {
        let (class_id, slab_id, slot_index) = location.unpack();
        if let Some(class) = self.classes.get(class_id as usize) {
            class.cancel_write_item(slab_id, slot_index);
        }
    }

    /// Get the total memory used.
    pub fn memory_used(&self) -> usize {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Get the memory limit.
    pub fn memory_limit(&self) -> usize {
        self.memory_limit
    }

    /// Get the slab size.
    #[allow(dead_code)]
    pub fn slab_size(&self) -> usize {
        self.slab_size
    }

    /// Get the number of slab classes.
    #[allow(dead_code)]
    pub fn num_classes(&self) -> usize {
        self.classes.len()
    }

    /// Get statistics for a class.
    pub fn class_stats(&self, class_id: u8) -> Option<ClassStats> {
        let class = self.classes.get(class_id as usize)?;
        Some(ClassStats {
            class_id,
            slot_size: class.slot_size(),
            slab_count: class.slab_count(),
            item_count: class.item_count(),
            bytes_used: class.bytes_used(),
        })
    }

    /// Create a verifier for this allocator.
    pub fn verifier(&self) -> SlabVerifier<'_> {
        SlabVerifier::new(self)
    }

    /// Create a verifier that allows expired items.
    ///
    /// Used for lazy cleanup of expired items.
    pub fn verifier_allowing_expired(&self) -> SlabVerifier<'_> {
        SlabVerifier::allowing_expired(self)
    }

    /// Reset the entire allocator, returning all memory to the free pool.
    ///
    /// This resets all slab classes and returns their slabs to the global
    /// free list. Used by `SlabCache::reset`.
    pub fn reset_all(&self) {
        // First, drain the existing free slab list
        loop {
            match self.free_slabs.steal() {
                crossbeam_deque::Steal::Empty => break,
                crossbeam_deque::Steal::Retry => continue,
                crossbeam_deque::Steal::Success(_) => continue,
            }
        }

        // Reset each class and collect all slab data pointers
        let mut all_slab_ptrs = Vec::new();
        for class in &self.classes {
            let ptrs = class.reset();
            all_slab_ptrs.extend(ptrs);
        }

        // Return all slabs to the free pool
        for ptr in all_slab_ptrs {
            self.free_slabs.push(ptr);
        }

        // Also add back the unused heap memory
        // (Re-build from scratch based on heap layout)
        let slab_count = self.memory_limit / self.slab_size;
        let heap_ptr = self.heap.as_ptr();

        // Clear and rebuild - push all slab addresses
        loop {
            match self.free_slabs.steal() {
                crossbeam_deque::Steal::Empty => break,
                crossbeam_deque::Steal::Retry => continue,
                crossbeam_deque::Steal::Success(_) => continue,
            }
        }

        for i in 0..slab_count {
            let slab_ptr = unsafe { heap_ptr.add(i * self.slab_size) };
            self.free_slabs.push(slab_ptr);
        }

        // Reset memory tracking
        self.memory_used.store(0, Ordering::Release);
    }
}

/// Statistics for a slab class.
#[derive(Debug, Clone)]
pub struct ClassStats {
    /// Class ID.
    pub class_id: u8,
    /// Slot size in bytes.
    pub slot_size: usize,
    /// Number of allocated slabs.
    pub slab_count: usize,
    /// Number of items.
    pub item_count: u64,
    /// Bytes used by items.
    pub bytes_used: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use cache_core::HugepageSize;

    fn test_config() -> SlabCacheConfig {
        SlabCacheConfig {
            heap_size: 4 * 1024 * 1024, // 4MB
            slab_size: 64 * 1024,       // 64KB slabs
            min_slot_size: 64,
            growth_factor: 1.25,
            hugepage_size: HugepageSize::None,
            ..Default::default()
        }
    }

    #[test]
    fn test_allocator_creation() {
        let config = test_config();
        let allocator = SlabAllocator::new(&config).unwrap();
        // Number of classes depends on slab_size and growth_factor
        assert!(allocator.num_classes() > 0);
        assert_eq!(allocator.slab_size(), 64 * 1024);
    }

    #[test]
    fn test_allocator_select_class() {
        let config = test_config();
        let allocator = SlabAllocator::new(&config).unwrap();

        // Small item -> class 0 (64 bytes)
        assert_eq!(allocator.select_class(50), Some(0));

        // Exact fit
        assert_eq!(allocator.select_class(64), Some(0));

        // Just over 64 -> next class
        let class = allocator.select_class(65);
        assert!(class.is_some());
        assert!(class.unwrap() > 0);
    }

    #[test]
    fn test_allocator_allocate() {
        let config = test_config();
        let allocator = SlabAllocator::new(&config).unwrap();

        // Allocate from class 0
        let slot = allocator.allocate(0);
        assert!(slot.is_some());
        let (slab_id, slot_index) = slot.unwrap();
        assert_eq!(slab_id, 0);
        assert_eq!(slot_index, 0);
    }

    #[test]
    fn test_allocator_write_item() {
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

            let location = SlabLocation::new(class_id, slab_id, slot_index);
            let header = allocator.header(location);
            assert_eq!(header.key(), key);
            assert_eq!(header.value(), value);
        }
    }

    /// Evicting a slab must leave NO hashtable entry publishing any location
    /// in it.
    ///
    /// `SlabClass::evict_slab` delete-marks each item unconditionally right
    /// after the eviction callback runs, so any entry the callback fails to
    /// unlink is left pointing at a tombstone in memory that is handed back to
    /// the global pool. Slab ids are reused, so such an entry would name a
    /// slot of whichever slab later takes the id.
    ///
    /// Single-threaded, so it pins the end-to-end invariant rather than the
    /// race that used to break it; that race is covered by cache-core's
    /// `loom_remove_survives_a_concurrent_frequency_bump`.
    #[test]
    fn evict_slab_leaves_no_entry_publishing_the_evicted_slab() {
        use cache_core::{Hashtable, KeyVerifier, Location, MultiChoiceHashtable};

        /// Verifies whatever the test published, by location alone — the slab
        /// memory is gone by the time we look, so reading it would be a
        /// use-after-free.
        struct ByLocation(Vec<(Vec<u8>, Location)>);
        impl KeyVerifier for ByLocation {
            fn verify(&self, key: &[u8], location: Location, _allow_deleted: bool) -> bool {
                self.0.iter().any(|(k, l)| k == key && *l == location)
            }
        }

        let config = test_config();
        let allocator = SlabAllocator::new(&config).unwrap();
        let hashtable = MultiChoiceHashtable::new(8);

        let value = b"value";
        let item_size = HEADER_SIZE + 16 + value.len();
        let class_id = allocator.select_class(item_size).unwrap();

        // Fill one slab and publish every item.
        let mut published = Vec::new();
        let mut slab_id = None;
        while let Some((sid, slot_index)) = allocator.allocate(class_id) {
            match slab_id {
                None => slab_id = Some(sid),
                Some(first) if first != sid => break, // moved to a second slab
                Some(_) => {}
            }
            let key = format!("key{slot_index:012}").into_bytes();
            unsafe {
                allocator.write_item(
                    class_id,
                    sid,
                    slot_index,
                    &key,
                    value,
                    Duration::from_secs(3600),
                );
            }
            // `allocate` hands back a write reference; eviction cannot drain
            // the slab until every one of them is released.
            allocator.release_write_ref(class_id, sid);
            let location = SlabLocation::new(class_id, sid, slot_index).to_location();
            published.push((key, location));
            if published.len() == 32 {
                break; // enough to exercise the walk without filling 64KB
            }
        }
        let slab_id = slab_id.expect("at least one slot must be allocatable");
        assert!(!published.is_empty(), "precondition: items were published");

        let verifier = ByLocation(published.clone());
        for (key, location) in &published {
            hashtable
                .insert_if_absent(key, *location, &verifier)
                .expect("seed insert must find a slot");
        }
        for (key, location) in &published {
            assert_eq!(
                hashtable.get_item_frequency(key, *location),
                Some(0),
                "precondition: the entry must be published before eviction"
            );
        }

        assert!(allocator.evict_slab_with_demoter(class_id, slab_id, None, &hashtable, |_| false));

        for (key, location) in &published {
            assert_eq!(
                hashtable.get_item_frequency(key, *location),
                None,
                "eviction left an entry publishing a location in the freed slab"
            );
        }
    }

    #[test]
    fn test_allocator_large_slab_size() {
        // Test with 16MB slab size - should have classes up to 16MB
        let config = SlabCacheConfig {
            heap_size: 64 * 1024 * 1024, // 64MB
            slab_size: 16 * 1024 * 1024, // 16MB slabs
            min_slot_size: 64,
            growth_factor: 1.25,
            hugepage_size: HugepageSize::None,
            ..Default::default()
        };
        let allocator = SlabAllocator::new(&config).unwrap();

        // Should be able to select a class for a 10MB item
        let class_id = allocator.select_class(10 * 1024 * 1024);
        assert!(class_id.is_some(), "Should have class for 10MB item");
    }
}
