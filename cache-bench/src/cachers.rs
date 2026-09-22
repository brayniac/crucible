//! cache-rs as a comparison backend.
//!
//! See `docs/superpowers/specs/2026-09-19-cache-rs-comparison-design.md`.

/// Translate crucible's hashtable power into cache-rs's.
///
/// The two count different things and the parameter has the same name in
/// both, which is the trap this function exists to close:
///
/// - crucible: `num_buckets = 1 << power`, 8 slots per bucket
/// - cache-rs: `bucket_power = power - 3`, i.e. power counts *slots*
///
/// Equal slot counts therefore need `cachers = crucible + 3`. Passing the
/// number through unconverted hands cache-rs a table 8x smaller, and table
/// pressure alone is worth ~5% miss ratio on the traces we use -- the same
/// magnitude as the effects being compared.
// Used by the feature-gated constructor, and by conversion tests that run
// in either build. Without this gate the non-feature binary carries it as
// dead code and plain `cargo clippy` fails, which CI's --all-features run
// would never reveal.
#[cfg(any(feature = "cache-rs", test))]
pub fn cachers_hash_power(crucible_power: u8) -> u8 {
    crucible_power + 3
}

/// Which cache-rs policy answers a given crucible policy.
///
/// Returns `None` where crucible has no counterpart, so an unmatched pair is
/// rejected at config time rather than silently compared against a default.
#[cfg(feature = "cache-rs")]
pub fn cachers_policy(policy: crate::config::EvictionPolicy) -> Option<cache_rs::Policy> {
    use crate::config::EvictionPolicy as P;
    Some(match policy {
        P::Fifo => cache_rs::Policy::Fifo,
        P::Cte => cache_rs::Policy::Cte,
        P::Random => cache_rs::Policy::Random,
        P::RandomFifo => cache_rs::Policy::RandomFifo,
        // Defaults on both sides, so the pair compares each engine as shipped
        // rather than as tuned. Chain length is the dominant variable here
        // (see the S3-FIFO design), so it is held at each engine's default
        // and swept separately if the pairs separate.
        P::Merge => cache_rs::Policy::Merge {
            max: 8,
            merge: 4,
            compact: 2,
        },
        P::S3Fifo => cache_rs::Policy::S3Fifo {
            admission_ratio: 0.10,
        },
        P::Lra | P::Lrc | P::Lfu | P::None => return None,
    })
}

#[cfg(feature = "cache-rs")]
mod adapter {
    use cache_core::{Cache, CacheError, CacheInternalStats, OwnedGuard, ValueRef};
    use std::time::Duration;

    /// cache-rs's `Segcache` behind crucible's `Cache` trait.
    pub struct CacheRs {
        pub(super) inner: cache_rs::Segcache,
    }

    impl CacheRs {
        /// Wrap a built cache-rs `Segcache` behind crucible's `Cache` trait.
        pub fn new(inner: cache_rs::Segcache) -> Self {
            Self { inner }
        }
    }

    impl Cache for CacheRs {
        fn get(&self, key: &[u8]) -> Option<OwnedGuard> {
            let item = self.inner.get(key)?;
            Some(OwnedGuard::new(value_bytes(&item)))
        }

        fn with_value<F, R>(&self, key: &[u8], f: F) -> Option<R>
        where
            F: FnOnce(&[u8]) -> R,
        {
            let item = self.inner.get(key)?;
            match item.value() {
                cache_rs::Value::Bytes(b) => Some(f(b)),
                cache_rs::Value::U64(n) => Some(f(n.to_string().as_bytes())),
            }
        }

        fn get_value_ref(&self, _key: &[u8]) -> Option<ValueRef> {
            // cache-rs has no equivalent of crucible's refcount-pinned
            // `ValueRef`, so there is nothing honest to return.
            //
            // This is load-bearing: a caller that reads `None` as "miss"
            // would see every lookup miss and report a catastrophic hit
            // ratio that looks like a result rather than a wiring error. The
            // replay driver uses `with_value` and never calls this. Do not
            // route anything through it without giving it a real
            // implementation first.
            None
        }

        fn set(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
            // crucible's `None` TTL means no expiry; cache-rs takes a
            // Duration, and zero is its no-expiry encoding.
            let ttl = ttl.unwrap_or(Duration::ZERO);
            self.inner
                .insert(key, value, None, ttl)
                .map_err(|e| match e {
                    cache_rs::SegcacheError::HashTableInsertEx => CacheError::HashTableFull,
                    cache_rs::SegcacheError::ItemOversized { .. } => CacheError::ValueTooLong,
                    cache_rs::SegcacheError::NoFreeSegments
                    | cache_rs::SegcacheError::EvictionEx => CacheError::OutOfMemory,
                    // Exists/NotFound/DataCorrupted/NotNumeric cannot occur from
                    // an upsert-style insert of `Value::Bytes`: this path never
                    // requires the key to be absent or present (unlike add/cas),
                    // never reads an existing item, and never treats the value
                    // as numeric. Kept as a defensive fallback in case cache-rs
                    // changes `insert`'s error surface underneath us.
                    cache_rs::SegcacheError::Exists
                    | cache_rs::SegcacheError::NotFound
                    | cache_rs::SegcacheError::DataCorrupted
                    | cache_rs::SegcacheError::NotNumeric => CacheError::OutOfMemory,
                })
        }

        fn delete(&self, key: &[u8]) -> bool {
            self.inner.delete(key)
        }

        fn contains(&self, key: &[u8]) -> bool {
            // `get` is `get_pinned(key, update_freq: true)` and bumps the
            // item's S3-FIFO frequency counter; `contains` must be a pure
            // presence check, since frequency drives the eviction behaviour
            // under measurement and every other `Cache::contains` impl in
            // crucible has no such side effect.
            self.inner.get_no_freq_incr(key).is_some()
        }

        fn flush(&self) {
            // cache-rs exposes no flush; the replay driver never calls it.
        }

        fn internal_stats(&self) -> Option<CacheInternalStats> {
            find_counter("segment_evict").map(|evictions| CacheInternalStats {
                evictions,
                // `item_current`, `segment_free` and `segment_current` are
                // static metrics registered alongside `segment_evict` under
                // the same `metrics` feature, so once the counter above is
                // present the gauges are too; `unwrap_or` only guards a
                // metric that is never actually absent here, not a real
                // backend value we would otherwise have.
                resident_items: find_gauge("item_current").unwrap_or(0),
                // "current number of free segments; includes the held-back
                // spare reserve (not available to normal writes)" per
                // cache-rs's own metric description -- `envelope_verdict`
                // must tolerate that reserve rather than expecting it to
                // reach zero on a saturated cache.
                free_segments: find_gauge("segment_free").unwrap_or(0),
                // "current total number of segments" -- set once at
                // construction, not a live per-write count.
                total_segments: find_gauge("segment_current").unwrap_or(0),
                ..Default::default()
            })
        }
    }

    /// Copy an item's value out as bytes.
    ///
    /// cache-rs stores numerics natively; the replay only ever writes byte
    /// strings, so `U64` is unreachable here but is rendered rather than
    /// panicking, because a panic in a measurement run costs the whole arm.
    fn value_bytes(item: &cache_rs::Item) -> Vec<u8> {
        match item.value() {
            cache_rs::Value::Bytes(b) => b.to_vec(),
            cache_rs::Value::U64(n) => n.to_string().into_bytes(),
        }
    }

    /// Look up a metriken counter by name in the global registry.
    ///
    /// cache-rs declares `mod metrics;` privately, so its counters (e.g.
    /// `SEGMENT_EVICT`) cannot be named by path. The `#[metric]` attribute
    /// registers them with metriken's global registry instead, so they are
    /// read by name -- the same pattern crucible's admin endpoint uses at
    /// `server/src/admin/mod.rs:179`.
    ///
    /// Returns `None` when the counter is absent: cache-rs gates metrics
    /// behind a default-on `metrics` feature, and if that is ever off, the
    /// honest answer is "no stats", which `envelope_verdict` treats as
    /// un-checkable. Substituting a non-zero placeholder would defeat the
    /// check this exists to feed.
    ///
    /// Shared by `internal_stats` (which needs presence/absence to decide
    /// `Some`/`None`) and `segment_evict_count` (the test's delta helper), so
    /// both read the counter through the exact same code path.
    fn find_counter(name: &str) -> Option<u64> {
        for metric in metriken::metrics().iter() {
            if metric.name() == name
                && let Some(metriken::Value::Counter(v)) = metric.value()
            {
                return Some(v);
            }
        }
        None
    }

    /// Look up a metriken *gauge* by name in the global registry.
    ///
    /// `item_current` ("current number of live items") is a `Gauge`, not a
    /// `Counter` -- `find_counter`'s `metriken::Value::Counter` pattern would
    /// silently return `None` for it. Gauges carry a signed `i64` (they can
    /// go negative transiently under concurrent inc/dec); negative readings
    /// are clamped to 0 since a negative resident-item count has no honest
    /// meaning for a caller.
    fn find_gauge(name: &str) -> Option<u64> {
        for metric in metriken::metrics().iter() {
            if metric.name() == name
                && let Some(metriken::Value::Gauge(v)) = metric.value()
            {
                return Some(v.max(0) as u64);
            }
        }
        None
    }

    /// Read cache-rs's segment-eviction counter out of metriken's registry.
    ///
    /// `segment_evict` is incremented at the top of `evict()` in every
    /// policy arm (Merge, S3Fifo, None), so it is a universal "an eviction
    /// pass ran" signal, not merge-specific.
    ///
    /// Absent (rather than merely zero) reports as zero here, since this
    /// helper is used only for a before/after delta in tests, where an
    /// absent counter and a present-but-unmoved counter are equally
    /// uninformative. `internal_stats`, which needs to distinguish
    /// "unchecked" from "checked and zero", goes through `find_counter`
    /// directly instead of through this helper.
    #[cfg(test)]
    pub(super) fn segment_evict_count() -> u64 {
        find_counter("segment_evict").unwrap_or(0)
    }

    /// Read cache-rs's `item_current` gauge out of metriken's registry.
    ///
    /// Mirrors `segment_evict_count`'s role for the gauge path: a delta
    /// helper for tests, where absence and "present but unmoved" are equally
    /// uninformative. `internal_stats` goes through `find_gauge` directly.
    #[cfg(test)]
    pub(super) fn item_current_gauge() -> u64 {
        find_gauge("item_current").unwrap_or(0)
    }
}

#[cfg(feature = "cache-rs")]
pub use adapter::CacheRs;

/// Placeholder so the non-feature build has a concrete `impl Cache` type to
/// name in its error return. Never constructed.
#[cfg(not(feature = "cache-rs"))]
pub enum Unavailable {}

#[cfg(not(feature = "cache-rs"))]
impl cache_core::Cache for Unavailable {
    fn get(&self, _key: &[u8]) -> Option<cache_core::OwnedGuard> {
        match *self {}
    }

    fn with_value<F, R>(&self, _key: &[u8], _f: F) -> Option<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        match *self {}
    }

    fn get_value_ref(&self, _key: &[u8]) -> Option<cache_core::ValueRef> {
        match *self {}
    }

    fn set(
        &self,
        _key: &[u8],
        _value: &[u8],
        _ttl: Option<std::time::Duration>,
    ) -> Result<(), cache_core::CacheError> {
        match *self {}
    }

    fn delete(&self, _key: &[u8]) -> bool {
        match *self {}
    }

    fn contains(&self, _key: &[u8]) -> bool {
        match *self {}
    }

    fn flush(&self) {
        match *self {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "cache-rs")]
    fn small_cachers() -> CacheRs {
        CacheRs {
            inner: cache_rs::Segcache::builder()
                .heap_size(8 * 1024 * 1024)
                .segment_size(1024 * 1024)
                .hash_power(cachers_hash_power(12))
                .build()
                .expect("build cache-rs"),
        }
    }

    #[cfg(feature = "cache-rs")]
    #[test]
    fn a_stored_value_reads_back_through_with_value() {
        use cache_core::Cache;
        let c = small_cachers();
        c.set(b"k", b"hello", Some(std::time::Duration::from_secs(60)))
            .expect("set");

        let seen = c.with_value(b"k", |v| v.to_vec());
        assert_eq!(seen.as_deref(), Some(&b"hello"[..]));
    }

    #[cfg(feature = "cache-rs")]
    #[test]
    fn a_missing_key_reads_back_as_none() {
        use cache_core::Cache;
        let c = small_cachers();
        assert!(c.with_value(b"absent", |v| v.to_vec()).is_none());
    }

    #[cfg(feature = "cache-rs")]
    #[test]
    fn a_deleted_key_stops_reading_back() {
        use cache_core::Cache;
        let c = small_cachers();
        c.set(b"k", b"v", None).expect("set");
        assert!(c.delete(b"k"));
        assert!(c.with_value(b"k", |v| v.to_vec()).is_none());
    }

    #[cfg(feature = "cache-rs")]
    #[test]
    fn an_oversized_value_is_reported_as_value_too_long_not_out_of_memory() {
        use cache_core::{Cache, CacheError};
        let c = small_cachers();
        // small_cachers() builds a 1MiB segment size; a value bigger than
        // that can never fit no matter how much space eviction frees, so it
        // must be reported distinctly from an out-of-memory/hashtable-full
        // condition (see `envelope_verdict` in replay.rs, which advises
        // raising `hashtable_power` for OutOfMemory -- wrong advice for an
        // oversized item).
        let huge_value = vec![0u8; 2 * 1024 * 1024];
        let result = c.set(b"k", &huge_value, None);
        assert_eq!(result, Err(CacheError::ValueTooLong));
    }

    #[test]
    fn equal_slot_counts_need_three_more_power_on_cache_rs() {
        // crucible power 22 = 2^22 buckets * 8 slots = 2^25 slots.
        // cache-rs reaches 2^25 slots at power 25.
        assert_eq!(cachers_hash_power(22), 25);
        assert_eq!(cachers_hash_power(10), 13);
    }

    #[test]
    fn the_conversion_is_not_the_identity() {
        // Guards the whole point: a future "simplification" to `power` would
        // pass every equality test written against one engine.
        for p in 7u8..30 {
            assert_ne!(
                cachers_hash_power(p),
                p,
                "power {p} converted to itself; cache-rs would get an 8x smaller table"
            );
        }
    }

    #[cfg(feature = "cache-rs")]
    #[test]
    fn each_crucible_segment_policy_maps_to_its_cache_rs_counterpart() {
        use crate::config::EvictionPolicy;
        assert!(matches!(
            cachers_policy(EvictionPolicy::Fifo),
            Some(cache_rs::Policy::Fifo)
        ));
        assert!(matches!(
            cachers_policy(EvictionPolicy::Cte),
            Some(cache_rs::Policy::Cte)
        ));
        assert!(matches!(
            cachers_policy(EvictionPolicy::Random),
            Some(cache_rs::Policy::Random)
        ));
        assert!(matches!(
            cachers_policy(EvictionPolicy::Merge),
            Some(cache_rs::Policy::Merge { .. })
        ));
        assert!(matches!(
            cachers_policy(EvictionPolicy::S3Fifo),
            Some(cache_rs::Policy::S3Fifo { .. })
        ));
    }

    #[cfg(feature = "cache-rs")]
    #[test]
    fn a_policy_with_no_counterpart_is_rejected_rather_than_defaulted() {
        use crate::config::EvictionPolicy;
        // Slab and heap policies have no segment-structured equivalent.
        // Silently substituting a default would compare two different
        // algorithms under one label.
        assert!(cachers_policy(EvictionPolicy::Lra).is_none());
        assert!(cachers_policy(EvictionPolicy::Lfu).is_none());
    }

    #[cfg(feature = "cache-rs")]
    #[test]
    fn internal_stats_reports_ram_segment_fill_from_the_segment_gauges() {
        use cache_core::Cache;
        // `segment_current` is SET (not incremented) once at construction to
        // the instance's total segment count -- "current total number of
        // segments" per cache-rs's own metric description -- and
        // `segment_free` decrements as segments are claimed. cache-rs's own
        // doc on `segment_free` says it "includes the held-back spare
        // reserve (not available to normal writes)", which is why this test
        // checks that most segments end up claimed rather than expecting
        // free to reach exactly zero.
        //
        // Both gauges live in metriken's process-global registry, the same
        // hazard documented on `resident_items_is_read_from_the_item_current_gauge`
        // above: a sibling test building its own cache-rs instance in
        // parallel can perturb these between this test's reads. The 8MB/1MB
        // sizing below matches `small_cachers()`, used throughout this file,
        // so any interference is from another test asserting the same total.
        let c = small_cachers();

        let before = c.internal_stats().expect("cache-rs must report stats");
        assert_eq!(
            before.total_segments, 8,
            "8MB heap / 1MB segments should report 8 total segments, got {}",
            before.total_segments
        );
        assert!(
            before.free_segments <= before.total_segments,
            "free_segments ({}) must not exceed total_segments ({})",
            before.free_segments,
            before.total_segments
        );

        // Write well past the heap size so segments are actually claimed.
        let value = vec![0xABu8; 512 * 1024];
        for i in 0..32u32 {
            let key = format!("fill_{i:08}");
            let _ = c.set(key.as_bytes(), &value, None);
        }

        let after = c.internal_stats().expect("cache-rs must report stats");
        assert_eq!(
            after.total_segments, 8,
            "writes must not change this instance's total segment count"
        );
        assert!(
            after.total_segments.saturating_sub(after.free_segments) >= 6,
            "expected at least 6 of 8 segments claimed after writing 16MB \
             into an 8MB cache (free={}, total={})",
            after.free_segments,
            after.total_segments
        );
    }

    #[cfg(feature = "cache-rs")]
    #[test]
    fn resident_items_is_read_from_the_item_current_gauge() {
        use cache_core::Cache;
        // `item_current` lives in metriken's process-global registry, same as
        // `segment_evict` above, so a sibling test running in parallel in
        // this binary can move it -- including nudging the delta *above*
        // what this test itself inserted (observed: 20 inserted, delta 21,
        // from another test's concurrent set). So, like the eviction test,
        // this only sandwiches the reported figure between before/after
        // reads; it does not bound the delta by this test's own insert
        // count, which parallel execution can make untrue.
        let before = super::adapter::item_current_gauge();

        let c = CacheRs {
            inner: cache_rs::Segcache::builder()
                .heap_size(8 * 1024 * 1024)
                .segment_size(1024 * 1024)
                .hash_power(cachers_hash_power(16))
                .build()
                .expect("build"),
        };

        let num_items = 20u32;
        let value = vec![0xABu8; 64];
        for i in 0..num_items {
            let key = format!("resident_{i:08}");
            c.set(key.as_bytes(), &value, None).expect("set");
        }

        let stats = c.internal_stats().expect("cache-rs must report stats");
        let after = super::adapter::item_current_gauge();

        let delta = after.saturating_sub(before);
        assert!(
            delta > 0,
            "inserting {num_items} items into a cache-rs instance did not \
             move item_current (before={before}, after={after})"
        );

        // This is what rejects a fabricated count, same as the eviction
        // sandwich below: `before`, `stats` and `after` are ordered in time,
        // so a truthful reader must land inside that window.
        //
        // Strictly greater than `before`, not `>=`: when this test runs
        // alone (or first) in the binary, `before` is 0, and a reader that
        // is wired to a nonexistent metric name also reports 0 -- `>=`
        // would let that fabricated-but-zero figure slide right past a
        // `before` of 0. `>` forces the reader to have actually observed
        // this run's insertions.
        assert!(
            stats.resident_items > before,
            "internal_stats reported {} resident items, no more than the {} \
             already on the gauge before this test wrote anything -- it is \
             not reading the gauge this workload affected",
            stats.resident_items,
            before
        );
        assert!(
            stats.resident_items <= after,
            "internal_stats reported {} resident items, more than the {} \
             the gauge had after the run -- the figure is fabricated, not \
             read",
            stats.resident_items,
            after
        );
    }

    #[cfg(feature = "cache-rs")]
    #[test]
    fn evictions_are_reported_so_the_envelope_check_can_see_them() {
        use cache_core::Cache;
        // A cache far smaller than what is written to it must evict, and must
        // say so. Without this, `envelope_verdict` reads an all-zero snapshot
        // and passes a saturated run as valid -- the exact failure the check
        // exists to prevent.
        //
        // Asserted as a DELTA, not an absolute. cache-rs's counters live in
        // metriken's process-global registry, so a sibling test running in
        // parallel in this same binary can contribute evictions. An absolute
        // `> 0` assertion could therefore pass on another test's work and
        // prove nothing about this wiring.
        let before = super::adapter::segment_evict_count();

        let c = CacheRs {
            inner: cache_rs::Segcache::builder()
                .heap_size(8 * 1024 * 1024)
                .segment_size(1024 * 1024)
                .hash_power(cachers_hash_power(16))
                .build()
                .expect("build"),
        };

        let value = vec![0xABu8; 8 * 1024];
        for i in 0..4000u32 {
            let key = format!("k{i:08}");
            let _ = c.set(key.as_bytes(), &value, None);
        }

        let stats = c.internal_stats().expect("cache-rs must report stats");
        let after = super::adapter::segment_evict_count();

        assert!(
            after.saturating_sub(before) > 0,
            "writing ~32 MB into an 8 MB cache evicted nothing that the \
             reader could see; envelope_verdict would pass this run"
        );
        // Sandwich the reported figure between the two reads that bracket
        // it. `before`, `stats` and `after` are ordered in time and the
        // counter is monotonic, so a truthful reader must land inside that
        // window no matter what sibling tests contribute in parallel.
        //
        // This is what rejects a fabricated count. A bare `> 0` (or a
        // comparison against the delta alone) is satisfied by a hard-coded
        // constant, which is precisely the substitution that would leave
        // `envelope_verdict` reading a number unconnected to any eviction.
        assert!(
            stats.evictions > before,
            "internal_stats reported {} evictions, no more than the {} \
             already on the counter before this test wrote anything -- it \
             is not reading evictions this workload caused",
            stats.evictions,
            before
        );
        assert!(
            stats.evictions <= after,
            "internal_stats reported {} evictions, more than the {} the \
             counter had after the run -- the figure is fabricated, not read",
            stats.evictions,
            after
        );
    }
}
