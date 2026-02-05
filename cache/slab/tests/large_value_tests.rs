//! Large value tests for SlabCache.
//!
//! These tests verify that the slab-based cache correctly handles large values
//! with heavy eviction pressure, bypassing the server layer entirely.

use slab_cache::{SlabCache, SlabCacheBuilder};
use std::time::Duration;

/// Generate a verifiable large value with position-dependent pattern.
fn generate_large_value(size: usize, seed: u8) -> Vec<u8> {
    (0..size).map(|i| (i as u8).wrapping_add(seed)).collect()
}

/// Verify a value matches the expected pattern.
fn verify_value(data: &[u8], expected_size: usize, seed: u8) -> bool {
    if data.len() != expected_size {
        return false;
    }
    data.iter()
        .enumerate()
        .all(|(i, &b)| b == (i as u8).wrapping_add(seed))
}

/// Create a slab cache configured for large values.
fn create_large_value_cache(heap_size_mb: usize, slab_size_mb: usize) -> SlabCache {
    SlabCacheBuilder::new()
        .heap_size(heap_size_mb * 1024 * 1024)
        .slab_size(slab_size_mb * 1024 * 1024)
        .hashtable_power(18) // 256K buckets
        .min_slot_size(64)
        .growth_factor(2.0) // Larger growth for bigger items
        .build()
        .expect("Failed to create cache")
}

// =============================================================================
// Basic Large Value Tests
// =============================================================================

#[test]
fn test_single_5mb_value() {
    let cache = create_large_value_cache(128, 32);
    let value_size = 5 * 1024 * 1024; // 5MB
    let value = generate_large_value(value_size, 0);

    cache
        .set_item(b"large_key", &value, Duration::from_secs(3600))
        .expect("SET failed");

    let retrieved = cache.get_item(b"large_key").expect("GET returned None");
    assert!(
        verify_value(&retrieved, value_size, 0),
        "Data corruption in 5MB value"
    );
}

#[test]
fn test_single_10mb_value() {
    let cache = create_large_value_cache(128, 32);
    let value_size = 10 * 1024 * 1024; // 10MB
    let value = generate_large_value(value_size, 0);

    cache
        .set_item(b"large_key", &value, Duration::from_secs(3600))
        .expect("SET failed");

    let retrieved = cache.get_item(b"large_key").expect("GET returned None");
    assert!(
        verify_value(&retrieved, value_size, 0),
        "Data corruption in 10MB value"
    );
}

#[test]
fn test_single_15mb_value() {
    // SlabCache has MAX_VALUE_LEN of ~16MB, so test with 15MB
    let cache = create_large_value_cache(256, 64);
    let value_size = 15 * 1024 * 1024; // 15MB
    let value = generate_large_value(value_size, 0);

    cache
        .set_item(b"large_key", &value, Duration::from_secs(3600))
        .expect("SET failed");

    let retrieved = cache.get_item(b"large_key").expect("GET returned None");
    assert!(
        verify_value(&retrieved, value_size, 0),
        "Data corruption in 15MB value"
    );
}

// =============================================================================
// Heavy Eviction Tests with Large Values
// =============================================================================

#[test]
fn test_1000_5mb_values_with_eviction() {
    // 512MB heap with 32MB slabs, 5MB values
    let cache = create_large_value_cache(512, 32);
    let value_size = 5 * 1024 * 1024; // 5MB
    let num_items = 1000;

    println!(
        "Testing {} items of {} bytes each ({}MB total written)",
        num_items,
        value_size,
        (num_items * value_size) / (1024 * 1024)
    );

    let mut successful_sets = 0;
    let mut successful_gets = 0;

    for i in 0..num_items {
        let key = format!("key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        match cache.set_item(key.as_bytes(), &value, Duration::from_secs(3600)) {
            Ok(()) => successful_sets += 1,
            Err(e) => {
                if i < 10 {
                    panic!("Early SET failed at item {}: {:?}", i, e);
                }
            }
        }

        if i > 0 && i % 100 == 0 {
            let recent_key = format!("key_{:06}", i);
            if let Some(retrieved) = cache.get_item(recent_key.as_bytes())
                && verify_value(&retrieved, value_size, seed)
            {
                successful_gets += 1;
            }
            println!(
                "Progress: {} items written, {} verified",
                i, successful_gets
            );
        }
    }

    println!(
        "Completed: {} successful SETs, {} verified GETs",
        successful_sets, successful_gets
    );

    let mut found = 0;
    for i in (0..num_items).rev().take(20) {
        let key = format!("key_{:06}", i);
        let seed = (i % 256) as u8;
        if let Some(retrieved) = cache.get_item(key.as_bytes())
            && verify_value(&retrieved, value_size, seed)
        {
            found += 1;
        }
    }

    println!("Found {} of last 20 items in cache", found);
    assert!(
        found > 0,
        "Expected at least some recent items to be in cache"
    );
    assert!(
        successful_sets > 100,
        "Expected most SETs to succeed, got {}",
        successful_sets
    );
}

#[test]
fn test_500_10mb_values_with_eviction() {
    let cache = create_large_value_cache(512, 32);
    let value_size = 10 * 1024 * 1024; // 10MB
    let num_items = 500;

    println!(
        "Testing {} items of {} bytes each ({}MB total written)",
        num_items,
        value_size,
        (num_items * value_size) / (1024 * 1024)
    );

    let mut successful_sets = 0;

    for i in 0..num_items {
        let key = format!("key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        match cache.set_item(key.as_bytes(), &value, Duration::from_secs(3600)) {
            Ok(()) => successful_sets += 1,
            Err(e) => {
                if i < 5 {
                    panic!("Early SET failed at item {}: {:?}", i, e);
                }
            }
        }

        if i > 0 && i % 50 == 0 {
            println!("Progress: {} items written", i);
        }
    }

    println!("Completed: {} successful SETs", successful_sets);

    let mut found = 0;
    for i in (0..num_items).rev().take(10) {
        let key = format!("key_{:06}", i);
        let seed = (i % 256) as u8;
        if let Some(retrieved) = cache.get_item(key.as_bytes())
            && verify_value(&retrieved, value_size, seed)
        {
            found += 1;
        }
    }

    println!("Found {} of last 10 items in cache", found);
    assert!(found > 0, "Expected at least some recent items in cache");
}

// =============================================================================
// Eviction Strategy Tests
// =============================================================================

#[test]
fn test_large_values_lra_eviction() {
    let cache = SlabCacheBuilder::new()
        .heap_size(256 * 1024 * 1024)
        .slab_size(32 * 1024 * 1024)
        .hashtable_power(16)
        .eviction_strategy(slab_cache::EvictionStrategy::SLAB_LRA)
        .build()
        .expect("Failed to create cache");

    let value_size = 5 * 1024 * 1024;
    let num_items = 200;

    for i in 0..num_items {
        let key = format!("lra_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        let _ = cache.set_item(key.as_bytes(), &value, Duration::from_secs(3600));

        // Access some items to keep them "hot"
        if i > 10 && i % 5 == 0 {
            let hot_key = format!("lra_key_{:06}", i - 5);
            let _ = cache.get_item(hot_key.as_bytes());
        }
    }

    let mut found = 0;
    for i in 0..num_items {
        let key = format!("lra_key_{:06}", i);
        if cache.get_item(key.as_bytes()).is_some() {
            found += 1;
        }
    }

    println!("LRA eviction: found {} of {} items", found, num_items);
}

#[test]
fn test_large_values_lrc_eviction() {
    let cache = SlabCacheBuilder::new()
        .heap_size(256 * 1024 * 1024)
        .slab_size(32 * 1024 * 1024)
        .hashtable_power(16)
        .eviction_strategy(slab_cache::EvictionStrategy::SLAB_LRC)
        .build()
        .expect("Failed to create cache");

    let value_size = 5 * 1024 * 1024;
    let num_items = 200;

    for i in 0..num_items {
        let key = format!("lrc_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        let _ = cache.set_item(key.as_bytes(), &value, Duration::from_secs(3600));
    }

    // With LRC, most recently created should be retained
    let mut recent_found = 0;
    let mut old_found = 0;

    for i in 0..50 {
        let key = format!("lrc_key_{:06}", i);
        if cache.get_item(key.as_bytes()).is_some() {
            old_found += 1;
        }
    }

    for i in (num_items - 50)..num_items {
        let key = format!("lrc_key_{:06}", i);
        if cache.get_item(key.as_bytes()).is_some() {
            recent_found += 1;
        }
    }

    println!(
        "LRC eviction: {} recent items, {} old items found",
        recent_found, old_found
    );

    // Recent items should be more likely to be retained
    assert!(
        recent_found >= old_found,
        "LRC should retain recent items: recent={}, old={}",
        recent_found,
        old_found
    );
}

#[test]
fn test_large_values_random_eviction() {
    let cache = SlabCacheBuilder::new()
        .heap_size(256 * 1024 * 1024)
        .slab_size(32 * 1024 * 1024)
        .hashtable_power(16)
        .eviction_strategy(slab_cache::EvictionStrategy::RANDOM)
        .build()
        .expect("Failed to create cache");

    let value_size = 5 * 1024 * 1024;
    let num_items = 200;

    for i in 0..num_items {
        let key = format!("random_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        let _ = cache.set_item(key.as_bytes(), &value, Duration::from_secs(3600));
    }

    let mut found = 0;
    for i in 0..num_items {
        let key = format!("random_key_{:06}", i);
        if cache.get_item(key.as_bytes()).is_some() {
            found += 1;
        }
    }

    println!("Random eviction: found {} of {} items", found, num_items);
    assert!(found > 0, "Expected at least some items in cache");
}

// =============================================================================
// Edge Case Tests
// =============================================================================

#[test]
fn test_rapid_large_value_updates() {
    let cache = create_large_value_cache(256, 32);
    let value_size = 5 * 1024 * 1024;

    for i in 0..100 {
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        cache
            .set_item(b"rapid_update_key", &value, Duration::from_secs(3600))
            .expect("SET failed");

        let retrieved = cache
            .get_item(b"rapid_update_key")
            .expect("GET returned None after SET");
        assert!(
            verify_value(&retrieved, value_size, seed),
            "Data corruption on update {}",
            i
        );
    }
}

#[test]
fn test_interleaved_sizes() {
    let cache = create_large_value_cache(512, 32);

    let sizes = [
        64,
        1024,
        64 * 1024,
        512 * 1024,
        1024 * 1024,
        5 * 1024 * 1024,
        10 * 1024 * 1024,
    ];

    for (i, &size) in sizes.iter().cycle().take(100).enumerate() {
        let key = format!("interleaved_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(size, seed);

        cache
            .set_item(key.as_bytes(), &value, Duration::from_secs(3600))
            .expect("SET failed");
    }

    let mut verified = 0;
    for (i, &size) in sizes.iter().cycle().take(100).enumerate() {
        let key = format!("interleaved_key_{:06}", i);
        let seed = (i % 256) as u8;

        if let Some(retrieved) = cache.get_item(key.as_bytes())
            && verify_value(&retrieved, size, seed)
        {
            verified += 1;
        }
    }

    println!("Verified {} of 100 interleaved items", verified);
    assert!(verified > 0, "Expected at least some items to be verified");
}

#[test]
fn test_delete_large_values() {
    let cache = create_large_value_cache(256, 32);
    let value_size = 5 * 1024 * 1024;

    for i in 0..20 {
        let key = format!("delete_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        cache
            .set_item(key.as_bytes(), &value, Duration::from_secs(3600))
            .unwrap();
    }

    for i in 0..10 {
        let key = format!("delete_key_{:06}", i);
        cache.delete_item(key.as_bytes());
    }

    for i in 0..10 {
        let key = format!("delete_key_{:06}", i);
        assert!(
            cache.get_item(key.as_bytes()).is_none(),
            "Deleted item {} still present",
            i
        );
    }

    for i in 10..20 {
        let key = format!("delete_key_{:06}", i);
        let seed = (i % 256) as u8;
        if let Some(retrieved) = cache.get_item(key.as_bytes()) {
            assert!(
                verify_value(&retrieved, value_size, seed),
                "Data corruption in remaining item {}",
                i
            );
        }
    }
}

// =============================================================================
// Slab Class Statistics Tests
// =============================================================================

#[test]
fn test_slab_class_stats_with_large_values() {
    let cache = create_large_value_cache(256, 32);
    let value_size = 5 * 1024 * 1024;

    // Insert some large values
    for i in 0..10 {
        let key = format!("stats_key_{:06}", i);
        let value = generate_large_value(value_size, i as u8);
        cache
            .set_item(key.as_bytes(), &value, Duration::from_secs(3600))
            .unwrap();
    }

    // Get class stats for various class IDs
    println!("Slab class statistics:");
    for class_id in 0..32 {
        if let Some(stat) = cache.class_stats(class_id)
            && stat.slab_count > 0
        {
            println!(
                "  Class {}: slot_size={}, slabs={}, items={}, bytes={}",
                class_id, stat.slot_size, stat.slab_count, stat.item_count, stat.bytes_used
            );
        }
    }

    // Memory usage should be tracked
    let used = cache.memory_used();
    let limit = cache.memory_limit();
    println!("Memory: {} / {} bytes used", used, limit);
    assert!(used > 0, "Memory usage should be positive");
}

// =============================================================================
// Zero-Copy API Tests
// =============================================================================

#[test]
fn test_zero_copy_set_large_value() {
    let cache = create_large_value_cache(256, 32);
    let value_size = 5 * 1024 * 1024;

    // Use the two-phase SET API
    let key = b"zerocopy_key";
    let mut reservation = cache
        .begin_slab_set(key, value_size, Duration::from_secs(3600))
        .expect("begin_slab_set failed");

    // Write directly to the reserved slot
    let value_buf = reservation.value_mut();
    for (i, byte) in value_buf.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }

    // Commit the reservation
    cache
        .commit_slab_set(reservation)
        .expect("commit_slab_set failed");

    // Verify
    let retrieved = cache.get_item(key).expect("GET returned None");
    assert!(
        verify_value(&retrieved, value_size, 0),
        "Data corruption in zero-copy SET"
    );
}

#[test]
fn test_zero_copy_cancel() {
    let cache = create_large_value_cache(256, 32);
    let value_size = 5 * 1024 * 1024;

    let key = b"cancel_key";
    let reservation = cache
        .begin_slab_set(key, value_size, Duration::from_secs(3600))
        .expect("begin_slab_set failed");

    // Cancel instead of commit
    cache.cancel_slab_set(reservation);

    // Key should not exist
    assert!(
        cache.get_item(key).is_none(),
        "Cancelled key should not exist"
    );
}

// =============================================================================
// Stress Tests
// =============================================================================

#[test]
#[ignore] // Expensive test
fn test_2000_5mb_values_sustained() {
    let cache = create_large_value_cache(512, 32);
    let value_size = 5 * 1024 * 1024;
    let num_items = 2000;

    println!(
        "Sustained test: {} items of {}MB each",
        num_items,
        value_size / (1024 * 1024)
    );

    let mut successful = 0;
    for i in 0..num_items {
        let key = format!("sustained_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        if cache
            .set_item(key.as_bytes(), &value, Duration::from_secs(3600))
            .is_ok()
        {
            successful += 1;
        }

        if i > 0 && i % 200 == 0 {
            println!("Progress: {}/{} items", i, num_items);
        }
    }

    println!("Completed: {} successful SETs", successful);
    assert!(successful > 1000, "Expected most SETs to succeed");
}
