//! Large value tests for HeapCache.
//!
//! These tests verify that the heap-based cache correctly handles large values
//! with heavy eviction pressure, bypassing the server layer entirely.

use cache_core::Cache;
use heap_cache::HeapCache;
use std::time::Duration;

/// Generate a verifiable large value with position-dependent pattern.
fn generate_large_value(size: usize, seed: u8) -> Vec<u8> {
    (0..size)
        .map(|i| (i as u8).wrapping_add(seed))
        .collect()
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

/// Create a heap cache with the specified memory limit.
fn create_large_value_cache(memory_limit_mb: usize) -> HeapCache {
    HeapCache::builder()
        .memory_limit(memory_limit_mb * 1024 * 1024)
        .build()
        .expect("Failed to create cache")
}

// =============================================================================
// Basic Large Value Tests
// =============================================================================

#[test]
fn test_single_5mb_value() {
    let cache = create_large_value_cache(128);
    let value_size = 5 * 1024 * 1024; // 5MB
    let value = generate_large_value(value_size, 0);

    cache
        .set(b"large_key", &value, Some(Duration::from_secs(3600)))
        .expect("SET failed");

    let retrieved = cache.get(b"large_key").expect("GET returned None");
    assert!(
        verify_value(retrieved.as_ref(), value_size, 0),
        "Data corruption in 5MB value"
    );
}

#[test]
fn test_single_10mb_value() {
    let cache = create_large_value_cache(128);
    let value_size = 10 * 1024 * 1024; // 10MB
    let value = generate_large_value(value_size, 0);

    cache
        .set(b"large_key", &value, Some(Duration::from_secs(3600)))
        .expect("SET failed");

    let retrieved = cache.get(b"large_key").expect("GET returned None");
    assert!(
        verify_value(retrieved.as_ref(), value_size, 0),
        "Data corruption in 10MB value"
    );
}

#[test]
fn test_single_20mb_value() {
    let cache = create_large_value_cache(256);
    let value_size = 20 * 1024 * 1024; // 20MB
    let value = generate_large_value(value_size, 0);

    cache
        .set(b"large_key", &value, Some(Duration::from_secs(3600)))
        .expect("SET failed");

    let retrieved = cache.get(b"large_key").expect("GET returned None");
    assert!(
        verify_value(retrieved.as_ref(), value_size, 0),
        "Data corruption in 20MB value"
    );
}

// =============================================================================
// Heavy Eviction Tests with Large Values
// =============================================================================

#[test]
fn test_1000_5mb_values_with_eviction() {
    // 512MB limit, 5MB values
    // 512MB / 5MB = ~102 items max, so 1000 items forces heavy eviction
    let cache = create_large_value_cache(512);
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

        match cache.set(key.as_bytes(), &value, Some(Duration::from_secs(3600))) {
            Ok(()) => successful_sets += 1,
            Err(e) => {
                if i < 10 {
                    panic!("Early SET failed at item {}: {:?}", i, e);
                }
            }
        }

        // Periodically verify recent items
        if i > 0 && i % 100 == 0 {
            let recent_key = format!("key_{:06}", i);
            if let Some(retrieved) = cache.get(recent_key.as_bytes()) {
                if verify_value(retrieved.as_ref(), value_size, seed) {
                    successful_gets += 1;
                }
            }
            println!("Progress: {} items written, {} verified", i, successful_gets);
        }
    }

    println!(
        "Completed: {} successful SETs, {} verified GETs",
        successful_sets, successful_gets
    );

    // Verify at least some items are still in cache
    let mut found = 0;
    for i in (0..num_items).rev().take(20) {
        let key = format!("key_{:06}", i);
        let seed = (i % 256) as u8;
        if let Some(retrieved) = cache.get(key.as_bytes()) {
            if verify_value(retrieved.as_ref(), value_size, seed) {
                found += 1;
            }
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
    let cache = create_large_value_cache(512);
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

        match cache.set(key.as_bytes(), &value, Some(Duration::from_secs(3600))) {
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
        if let Some(retrieved) = cache.get(key.as_bytes()) {
            if verify_value(retrieved.as_ref(), value_size, seed) {
                found += 1;
            }
        }
    }

    println!("Found {} of last 10 items in cache", found);
    assert!(found > 0, "Expected at least some recent items in cache");
}

// =============================================================================
// Eviction Policy Tests
// =============================================================================

#[test]
fn test_large_values_s3fifo_eviction() {
    let cache = HeapCache::builder()
        .memory_limit(256 * 1024 * 1024)
        .eviction_policy(heap_cache::EvictionPolicy::S3Fifo)
        .build()
        .expect("Failed to create cache");

    let value_size = 5 * 1024 * 1024;
    let num_items = 200;

    for i in 0..num_items {
        let key = format!("s3fifo_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        let _ = cache.set(key.as_bytes(), &value, Some(Duration::from_secs(3600)));

        // Access some items multiple times
        if i > 10 && i % 5 == 0 {
            let hot_key = format!("s3fifo_key_{:06}", i - 5);
            let _ = cache.get(hot_key.as_bytes());
        }
    }

    let mut hot_found = 0;
    let mut cold_found = 0;

    for i in 0..num_items {
        let key = format!("s3fifo_key_{:06}", i);
        if cache.get(key.as_bytes()).is_some() {
            if i > 10 && (i - 5) % 5 == 0 {
                hot_found += 1;
            } else {
                cold_found += 1;
            }
        }
    }

    println!(
        "S3-FIFO retention: {} hot items, {} cold items",
        hot_found, cold_found
    );
}

#[test]
fn test_large_values_lfu_eviction() {
    let cache = HeapCache::builder()
        .memory_limit(256 * 1024 * 1024)
        .eviction_policy(heap_cache::EvictionPolicy::Lfu)
        .build()
        .expect("Failed to create cache");

    let value_size = 5 * 1024 * 1024;
    let num_items = 200;

    for i in 0..num_items {
        let key = format!("lfu_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        let _ = cache.set(key.as_bytes(), &value, Some(Duration::from_secs(3600)));

        // Access some items multiple times to increase frequency
        if i > 10 && i % 5 == 0 {
            let hot_key = format!("lfu_key_{:06}", i - 5);
            for _ in 0..5 {
                let _ = cache.get(hot_key.as_bytes());
            }
        }
    }

    let mut found = 0;
    for i in 0..num_items {
        let key = format!("lfu_key_{:06}", i);
        if cache.get(key.as_bytes()).is_some() {
            found += 1;
        }
    }

    println!("LFU eviction: found {} of {} items", found, num_items);
}

// =============================================================================
// Edge Case Tests
// =============================================================================

#[test]
fn test_rapid_large_value_updates() {
    let cache = create_large_value_cache(256);
    let value_size = 5 * 1024 * 1024;

    for i in 0..100 {
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        cache
            .set(b"rapid_update_key", &value, Some(Duration::from_secs(3600)))
            .expect("SET failed");

        let retrieved = cache
            .get(b"rapid_update_key")
            .expect("GET returned None after SET");
        assert!(
            verify_value(retrieved.as_ref(), value_size, seed),
            "Data corruption on update {}",
            i
        );
    }
}

#[test]
fn test_interleaved_sizes() {
    let cache = create_large_value_cache(512);

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
            .set(key.as_bytes(), &value, Some(Duration::from_secs(3600)))
            .expect("SET failed");
    }

    let mut verified = 0;
    for (i, &size) in sizes.iter().cycle().take(100).enumerate() {
        let key = format!("interleaved_key_{:06}", i);
        let seed = (i % 256) as u8;

        if let Some(retrieved) = cache.get(key.as_bytes()) {
            if verify_value(retrieved.as_ref(), size, seed) {
                verified += 1;
            }
        }
    }

    println!("Verified {} of 100 interleaved items", verified);
    assert!(verified > 0, "Expected at least some items to be verified");
}

#[test]
fn test_delete_large_values() {
    let cache = create_large_value_cache(256);
    let value_size = 5 * 1024 * 1024;

    for i in 0..20 {
        let key = format!("delete_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        cache
            .set(key.as_bytes(), &value, Some(Duration::from_secs(3600)))
            .unwrap();
    }

    for i in 0..10 {
        let key = format!("delete_key_{:06}", i);
        cache.delete(key.as_bytes());
    }

    for i in 0..10 {
        let key = format!("delete_key_{:06}", i);
        assert!(
            cache.get(key.as_bytes()).is_none(),
            "Deleted item {} still present",
            i
        );
    }

    for i in 10..20 {
        let key = format!("delete_key_{:06}", i);
        let seed = (i % 256) as u8;
        if let Some(retrieved) = cache.get(key.as_bytes()) {
            assert!(
                verify_value(retrieved.as_ref(), value_size, seed),
                "Data corruption in remaining item {}",
                i
            );
        }
    }
}

// =============================================================================
// Memory Tracking Tests
// =============================================================================

#[test]
fn test_memory_tracking_with_large_values() {
    let cache = create_large_value_cache(256);
    let value_size = 5 * 1024 * 1024;

    let initial_used = cache.bytes_used();
    println!("Initial bytes used: {}", initial_used);

    // Add some large values
    for i in 0..10 {
        let key = format!("memory_key_{:06}", i);
        let value = generate_large_value(value_size, i as u8);
        cache
            .set(key.as_bytes(), &value, Some(Duration::from_secs(3600)))
            .unwrap();
    }

    let after_inserts = cache.bytes_used();
    println!("After 10 inserts: {} bytes used", after_inserts);

    // Should have increased significantly
    assert!(
        after_inserts > initial_used + (5 * value_size),
        "Memory usage should increase substantially"
    );

    // Delete some
    for i in 0..5 {
        let key = format!("memory_key_{:06}", i);
        cache.delete(key.as_bytes());
    }

    let after_deletes = cache.bytes_used();
    println!("After 5 deletes: {} bytes used", after_deletes);

    // Should have decreased
    assert!(
        after_deletes < after_inserts,
        "Memory usage should decrease after deletes"
    );
}

// =============================================================================
// Stress Tests
// =============================================================================

#[test]
#[ignore] // Expensive test
fn test_2000_5mb_values_sustained() {
    let cache = create_large_value_cache(512);
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
            .set(key.as_bytes(), &value, Some(Duration::from_secs(3600)))
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
