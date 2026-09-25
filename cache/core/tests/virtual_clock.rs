//! Expiry driven by a caller-supplied clock, through the public API.
//!
//! The override is per thread, so these cannot disturb other tests and need
//! no serialization: cargo runs each test on its own thread, which is
//! exactly the scope the clock uses. An earlier process-global version did
//! disturb them -- four tests across two modules went flaky -- which is the
//! reason the clock is scoped the way it is.

#![cfg(feature = "virtual-clock")]

use std::sync::Arc;
use std::time::Duration;

use cache_core::{
    FifoLayerBuilder, LayerConfig, MultiChoiceHashtable, TieredCache, TieredCacheBuilder,
    TtlLayerBuilder, clock,
};

/// Two layers wired as s3fifo, matching the unit suite's fixture: layer 0
/// must name layer 1 as its demotion target or nothing is ever promoted.
fn build_cache() -> TieredCache<MultiChoiceHashtable> {
    let hashtable = Arc::new(MultiChoiceHashtable::new(10));
    let fifo = FifoLayerBuilder::new()
        .layer_id(0)
        .pool_id(0)
        .segment_size(64 * 1024)
        .heap_size(256 * 1024)
        .spare_capacity(0)
        .config(
            LayerConfig::new()
                .with_next_layer(1)
                .with_demotion_threshold(1),
        )
        .build()
        .expect("fifo layer");
    let ttl = TtlLayerBuilder::new()
        .layer_id(1)
        .pool_id(1)
        .segment_size(64 * 1024)
        .heap_size(512 * 1024)
        .spare_capacity(0)
        .build()
        .expect("ttl layer");
    TieredCacheBuilder::new(hashtable)
        .with_fifo_layer(fifo)
        .with_ttl_layer(ttl)
        .eviction_threshold(1)
        .build()
}

/// The positive case the wall clock cannot test.
///
/// The unit suite's `test_expire` can only assert that an hour-long TTL has
/// not lapsed, because asserting that it *has* would mean waiting an hour.
#[test]
fn an_item_expires_once_the_clock_passes_its_ttl() {
    let cache = build_cache();

    clock::set_virtual_now(1_000_000);
    cache
        .set(b"expiring", b"value", b"", Duration::from_secs(60))
        .expect("set");
    assert!(
        cache.get(b"expiring").is_some(),
        "should be live inside its sixty second TTL"
    );

    clock::set_virtual_now(1_000_061);
    assert!(
        cache.get(b"expiring").is_none(),
        "should be gone once the clock passes its TTL"
    );

    clock::clear_virtual_now();
}

/// Advancing the clock must not expire an item that still has time left,
/// or the first test would pass for the wrong reason.
#[test]
fn an_item_survives_a_clock_advance_inside_its_ttl() {
    let cache = build_cache();

    clock::set_virtual_now(2_000_000);
    cache
        .set(b"surviving", b"value", b"", Duration::from_secs(600))
        .expect("set");

    clock::set_virtual_now(2_000_599);
    assert!(
        cache.get(b"surviving").is_some(),
        "one second short of a ten minute TTL should still be live"
    );

    clock::clear_virtual_now();
}

/// Clearing must restore the system clock, or a replay would leave the
/// process stuck in whatever year its trace came from.
#[test]
fn clearing_restores_the_system_clock() {
    clock::set_virtual_now(1_000_000);
    assert_eq!(clock::now_unix_secs(), 1_000_000);
    assert_eq!(clock::virtual_now(), Some(1_000_000));

    clock::clear_virtual_now();
    assert_eq!(clock::virtual_now(), None);

    let system = clock::system_now_unix_secs();
    let reported = clock::now_unix_secs();
    assert!(
        reported.abs_diff(system) <= 1,
        "after clearing, {reported} should track the system clock {system}"
    );
}
