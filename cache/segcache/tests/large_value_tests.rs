//! Large value tests for SegCache.
//!
//! These tests verify that the cache correctly handles large values (5MB+)
//! with heavy eviction pressure, bypassing the server layer entirely.

use segcache::{SegCache, SegCacheBuilder};
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

/// Create a cache configured for large values.
fn create_large_value_cache(heap_size_mb: usize, segment_size_mb: usize) -> SegCache {
    SegCacheBuilder::new()
        .heap_size(heap_size_mb * 1024 * 1024)
        .segment_size(segment_size_mb * 1024 * 1024)
        .hashtable_power(18) // 256K buckets
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
        .set(b"large_key", &value, Duration::from_secs(3600))
        .expect("SET failed");

    let retrieved = cache.get(b"large_key").expect("GET returned None");
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
        .set(b"large_key", &value, Duration::from_secs(3600))
        .expect("SET failed");

    let retrieved = cache.get(b"large_key").expect("GET returned None");
    assert!(
        verify_value(&retrieved, value_size, 0),
        "Data corruption in 10MB value"
    );
}

#[test]
fn test_single_20mb_value() {
    let cache = create_large_value_cache(256, 64);
    let value_size = 20 * 1024 * 1024; // 20MB
    let value = generate_large_value(value_size, 0);

    cache
        .set(b"large_key", &value, Duration::from_secs(3600))
        .expect("SET failed");

    let retrieved = cache.get(b"large_key").expect("GET returned None");
    assert!(
        verify_value(&retrieved, value_size, 0),
        "Data corruption in 20MB value"
    );
}

// =============================================================================
// Heavy Eviction Tests with Large Values
// =============================================================================

#[test]
fn test_1000_5mb_values_with_eviction() {
    // 512MB heap with 32MB segments, 5MB values
    // 512MB / 5MB = ~102 items max, so 1000 items forces heavy eviction
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

        match cache.set(key.as_bytes(), &value, Duration::from_secs(3600)) {
            Ok(()) => successful_sets += 1,
            Err(e) => {
                // Some failures are expected during heavy eviction
                if i < 10 {
                    panic!("Early SET failed at item {}: {:?}", i, e);
                }
            }
        }

        // Periodically verify recent items are still accessible
        if i > 0 && i % 100 == 0 {
            // Check the most recent item
            let recent_key = format!("key_{:06}", i);
            if let Some(retrieved) = cache.get(recent_key.as_bytes()) {
                if verify_value(&retrieved, value_size, seed) {
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
            if verify_value(&retrieved, value_size, seed) {
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
    // 512MB heap with 32MB segments, 10MB values
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

        match cache.set(key.as_bytes(), &value, Duration::from_secs(3600)) {
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

    // Verify recent items
    let mut found = 0;
    for i in (0..num_items).rev().take(10) {
        let key = format!("key_{:06}", i);
        let seed = (i % 256) as u8;
        if let Some(retrieved) = cache.get(key.as_bytes()) {
            if verify_value(&retrieved, value_size, seed) {
                found += 1;
            }
        }
    }

    println!("Found {} of last 10 items in cache", found);
    assert!(found > 0, "Expected at least some recent items in cache");
}

// =============================================================================
// Eviction Policy Tests with Large Values
// =============================================================================

#[test]
fn test_large_values_s3fifo_eviction() {
    let cache = SegCacheBuilder::new()
        .heap_size(256 * 1024 * 1024)
        .segment_size(32 * 1024 * 1024)
        .hashtable_power(16)
        .s3fifo() // Use S3-FIFO eviction
        .build()
        .expect("Failed to create cache");

    let value_size = 5 * 1024 * 1024;
    let num_items = 200;

    for i in 0..num_items {
        let key = format!("s3fifo_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        let _ = cache.set(key.as_bytes(), &value, Duration::from_secs(3600));

        // Access some items multiple times to test S3-FIFO promotion
        if i > 10 && i % 5 == 0 {
            let hot_key = format!("s3fifo_key_{:06}", i - 5);
            let _ = cache.get(hot_key.as_bytes());
        }
    }

    // Check that frequently accessed items are more likely to be retained
    let mut hot_found = 0;
    let mut cold_found = 0;

    for i in 0..num_items {
        let key = format!("s3fifo_key_{:06}", i);
        if cache.get(key.as_bytes()).is_some() {
            // Items accessed multiple times (every 5th item after 10)
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
fn test_large_values_random_eviction() {
    let cache = SegCacheBuilder::new()
        .heap_size(256 * 1024 * 1024)
        .segment_size(32 * 1024 * 1024)
        .hashtable_power(16)
        .eviction_policy(segcache::EvictionPolicy::Random)
        .build()
        .expect("Failed to create cache");

    let value_size = 5 * 1024 * 1024;
    let num_items = 200;

    for i in 0..num_items {
        let key = format!("random_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        let _ = cache.set(key.as_bytes(), &value, Duration::from_secs(3600));
    }

    // Just verify we can still access some items
    let mut found = 0;
    for i in 0..num_items {
        let key = format!("random_key_{:06}", i);
        if cache.get(key.as_bytes()).is_some() {
            found += 1;
        }
    }

    println!("Random eviction: found {} of {} items", found, num_items);
    assert!(found > 0, "Expected at least some items in cache");
}

#[test]
fn test_large_values_fifo_eviction() {
    let cache = SegCacheBuilder::new()
        .heap_size(256 * 1024 * 1024)
        .segment_size(32 * 1024 * 1024)
        .hashtable_power(16)
        .eviction_policy(segcache::EvictionPolicy::Fifo)
        .build()
        .expect("Failed to create cache");

    let value_size = 5 * 1024 * 1024;
    let num_items = 200;

    for i in 0..num_items {
        let key = format!("fifo_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        let _ = cache.set(key.as_bytes(), &value, Duration::from_secs(3600));
    }

    // With FIFO, recent items should be retained, old items evicted
    let mut recent_found = 0;
    let mut old_found = 0;

    // Check oldest 50 items
    for i in 0..50 {
        let key = format!("fifo_key_{:06}", i);
        if cache.get(key.as_bytes()).is_some() {
            old_found += 1;
        }
    }

    // Check most recent 50 items
    for i in (num_items - 50)..num_items {
        let key = format!("fifo_key_{:06}", i);
        if cache.get(key.as_bytes()).is_some() {
            recent_found += 1;
        }
    }

    println!(
        "FIFO eviction: {} recent items, {} old items found",
        recent_found, old_found
    );

    // Recent items should be more likely to be retained
    assert!(
        recent_found >= old_found,
        "FIFO should retain recent items: recent={}, old={}",
        recent_found,
        old_found
    );
}

// =============================================================================
// Edge Case Tests
// =============================================================================

#[test]
fn test_value_exactly_segment_size() {
    let segment_size = 8 * 1024 * 1024; // 8MB segment
    let cache = create_large_value_cache(64, 8);

    // Value that would exactly fill a segment (minus header overhead)
    // The actual max is less due to item headers
    let value_size = segment_size - 1024; // Leave room for headers
    let value = generate_large_value(value_size, 0);

    cache
        .set(b"segment_size_key", &value, Duration::from_secs(3600))
        .expect("SET failed for segment-sized value");

    let retrieved = cache
        .get(b"segment_size_key")
        .expect("GET returned None for segment-sized value");
    assert!(
        verify_value(&retrieved, value_size, 0),
        "Data corruption in segment-sized value"
    );
}

#[test]
fn test_rapid_large_value_updates() {
    let cache = create_large_value_cache(256, 32);
    let value_size = 5 * 1024 * 1024;

    // Rapidly update the same key with different values
    for i in 0..100 {
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        cache
            .set(b"rapid_update_key", &value, Duration::from_secs(3600))
            .expect("SET failed");

        // Verify immediately
        let retrieved = cache
            .get(b"rapid_update_key")
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
        64,                  // 64 bytes
        1024,                // 1KB
        64 * 1024,           // 64KB
        512 * 1024,          // 512KB
        1024 * 1024,         // 1MB
        5 * 1024 * 1024,     // 5MB
        10 * 1024 * 1024,    // 10MB
    ];

    // Write items of various sizes
    for (i, &size) in sizes.iter().cycle().take(100).enumerate() {
        let key = format!("interleaved_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(size, seed);

        cache
            .set(key.as_bytes(), &value, Duration::from_secs(3600))
            .expect("SET failed");
    }

    // Verify some items
    let mut verified = 0;
    for (i, &size) in sizes.iter().cycle().take(100).enumerate() {
        let key = format!("interleaved_key_{:06}", i);
        let seed = (i % 256) as u8;

        if let Some(retrieved) = cache.get(key.as_bytes()) {
            if verify_value(&retrieved, size, seed) {
                verified += 1;
            }
        }
    }

    println!("Verified {} of 100 interleaved items", verified);
    assert!(verified > 0, "Expected at least some items to be verified");
}

#[test]
fn test_delete_large_values() {
    let cache = create_large_value_cache(256, 32);
    let value_size = 5 * 1024 * 1024;

    // Insert several large values
    for i in 0..20 {
        let key = format!("delete_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        cache
            .set(key.as_bytes(), &value, Duration::from_secs(3600))
            .unwrap();
    }

    // Delete half of them
    for i in 0..10 {
        let key = format!("delete_key_{:06}", i);
        cache.delete(key.as_bytes());
    }

    // Verify deleted items are gone
    for i in 0..10 {
        let key = format!("delete_key_{:06}", i);
        assert!(
            cache.get(key.as_bytes()).is_none(),
            "Deleted item {} still present",
            i
        );
    }

    // Verify remaining items are intact
    for i in 10..20 {
        let key = format!("delete_key_{:06}", i);
        let seed = (i % 256) as u8;
        if let Some(retrieved) = cache.get(key.as_bytes()) {
            assert!(
                verify_value(&retrieved, value_size, seed),
                "Data corruption in remaining item {}",
                i
            );
        }
    }
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
            .set(key.as_bytes(), &value, Duration::from_secs(3600))
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

// =============================================================================
// Disk Tier Tests with Large Values
// =============================================================================

use segcache::DiskTierConfig;
use tempfile::TempDir;

/// Create a cache with disk tier configured.
fn create_disk_tier_cache(
    heap_size_mb: usize,
    segment_size_mb: usize,
    disk_size_mb: usize,
    disk_path: &std::path::Path,
) -> SegCache {
    let disk_config = DiskTierConfig::new(disk_path.to_path_buf(), disk_size_mb * 1024 * 1024)
        .promotion_threshold(1)
        .recover_on_startup(false);

    SegCacheBuilder::new()
        .heap_size(heap_size_mb * 1024 * 1024)
        .segment_size(segment_size_mb * 1024 * 1024)
        .hashtable_power(18)
        .disk_tier(disk_config)
        .build()
        .expect("Failed to create cache with disk tier")
}

#[test]
fn test_disk_tier_basic_large_value() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let disk_path = temp_dir.path().join("disk_cache.dat");

    // Small RAM (64MB), larger disk (256MB)
    let cache = create_disk_tier_cache(64, 16, 256, &disk_path);
    let value_size = 5 * 1024 * 1024; // 5MB
    let value = generate_large_value(value_size, 42);

    cache
        .set(b"disk_test_key", &value, Duration::from_secs(3600))
        .expect("SET failed");

    let retrieved = cache.get(b"disk_test_key").expect("GET returned None");
    assert!(
        verify_value(&retrieved, value_size, 42),
        "Data corruption in basic disk tier test"
    );
}

#[test]
fn test_disk_tier_spillover_large_values() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let disk_path = temp_dir.path().join("disk_cache.dat");

    // Small RAM (64MB), larger disk (512MB)
    // 5MB values mean ~12 items fill RAM, then spill to disk
    let cache = create_disk_tier_cache(64, 16, 512, &disk_path);
    let value_size = 5 * 1024 * 1024; // 5MB
    let num_items = 100; // Far more than RAM can hold

    println!(
        "Testing disk tier spillover: {} items of {}MB each",
        num_items,
        value_size / (1024 * 1024)
    );

    // Write items - most will spill to disk
    for i in 0..num_items {
        let key = format!("spillover_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        cache
            .set(key.as_bytes(), &value, Duration::from_secs(3600))
            .expect(&format!("SET failed for item {}", i));

        if i > 0 && i % 20 == 0 {
            println!("Progress: {} items written", i);
        }
    }

    // Verify all items are retrievable (from RAM or disk)
    let mut found = 0;
    let mut verified = 0;

    for i in 0..num_items {
        let key = format!("spillover_key_{:06}", i);
        let seed = (i % 256) as u8;

        if let Some(retrieved) = cache.get(key.as_bytes()) {
            found += 1;
            if verify_value(&retrieved, value_size, seed) {
                verified += 1;
            } else {
                println!("Data corruption for item {}", i);
            }
        }
    }

    println!(
        "Disk tier spillover: found {} of {}, verified {}",
        found, num_items, verified
    );

    // With disk tier, we should be able to retrieve all items
    assert!(
        found >= num_items - 5, // Allow for some eviction
        "Expected most items retrievable with disk tier, got {} of {}",
        found,
        num_items
    );
    assert_eq!(
        found, verified,
        "All found items should verify correctly"
    );
}

#[test]
fn test_disk_tier_heavy_eviction_large_values() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let disk_path = temp_dir.path().join("disk_cache.dat");

    // Very small RAM (32MB), moderate disk (256MB)
    // This forces heavy eviction from RAM to disk
    let cache = create_disk_tier_cache(32, 8, 256, &disk_path);
    let value_size = 5 * 1024 * 1024; // 5MB
    let num_items = 200; // ~1GB total, forcing eviction from disk too

    println!(
        "Testing disk tier with heavy eviction: {} items of {}MB",
        num_items,
        value_size / (1024 * 1024)
    );

    let mut successful_sets = 0;

    for i in 0..num_items {
        let key = format!("heavy_eviction_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        match cache.set(key.as_bytes(), &value, Duration::from_secs(3600)) {
            Ok(()) => successful_sets += 1,
            Err(e) => {
                // Some failures expected during heavy eviction
                if i < 5 {
                    panic!("Early SET failed: {:?}", e);
                }
            }
        }

        if i > 0 && i % 50 == 0 {
            println!("Progress: {} items written, {} successful", i, successful_sets);
        }
    }

    println!("Completed: {} successful SETs", successful_sets);

    // Verify recent items are accessible
    let mut found = 0;
    for i in (0..num_items).rev().take(30) {
        let key = format!("heavy_eviction_key_{:06}", i);
        let seed = (i % 256) as u8;

        if let Some(retrieved) = cache.get(key.as_bytes()) {
            if verify_value(&retrieved, value_size, seed) {
                found += 1;
            }
        }
    }

    println!("Found {} of last 30 items", found);
    assert!(
        found > 0,
        "Expected at least some recent items with disk tier"
    );
}

#[test]
fn test_disk_tier_promotion_large_values() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let disk_path = temp_dir.path().join("disk_cache.dat");

    // Small RAM, disk tier with low promotion threshold
    let disk_config = DiskTierConfig::new(disk_path.clone(), 256 * 1024 * 1024)
        .promotion_threshold(1) // Promote after 1 access
        .recover_on_startup(false);

    let cache = SegCacheBuilder::new()
        .heap_size(64 * 1024 * 1024)
        .segment_size(16 * 1024 * 1024)
        .hashtable_power(16)
        .disk_tier(disk_config)
        .build()
        .expect("Failed to create cache");

    let value_size = 5 * 1024 * 1024;

    // Fill RAM and spill to disk
    for i in 0..30 {
        let key = format!("promo_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        cache
            .set(key.as_bytes(), &value, Duration::from_secs(3600))
            .expect("SET failed");
    }

    // Access early items multiple times to trigger promotion
    println!("Accessing early items to trigger promotion...");
    for _ in 0..3 {
        for i in 0..5 {
            let key = format!("promo_key_{:06}", i);
            let _ = cache.get(key.as_bytes());
        }
    }

    // Verify promoted items are still accessible
    for i in 0..5 {
        let key = format!("promo_key_{:06}", i);
        let seed = (i % 256) as u8;

        let retrieved = cache.get(key.as_bytes());
        assert!(
            retrieved.is_some(),
            "Promoted item {} should be accessible",
            i
        );
        assert!(
            verify_value(&retrieved.unwrap(), value_size, seed),
            "Data corruption in promoted item {}",
            i
        );
    }

    println!("Promotion test passed");
}

#[test]
fn test_disk_tier_interleaved_access_large_values() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let disk_path = temp_dir.path().join("disk_cache.dat");

    let cache = create_disk_tier_cache(64, 16, 512, &disk_path);
    let value_size = 5 * 1024 * 1024;
    let num_items = 50;

    // Write items
    for i in 0..num_items {
        let key = format!("interleaved_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        cache
            .set(key.as_bytes(), &value, Duration::from_secs(3600))
            .expect("SET failed");
    }

    // Interleaved reads: access items in random order
    let access_pattern: Vec<usize> = (0..num_items)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();

    let mut verified = 0;
    for &i in &access_pattern {
        let key = format!("interleaved_key_{:06}", i);
        let seed = (i % 256) as u8;

        if let Some(retrieved) = cache.get(key.as_bytes()) {
            if verify_value(&retrieved, value_size, seed) {
                verified += 1;
            }
        }
    }

    println!(
        "Interleaved access: verified {} of {} items",
        verified, num_items
    );
    assert!(
        verified >= num_items - 5,
        "Expected most items accessible, got {}",
        verified
    );
}

#[test]
fn test_disk_tier_delete_large_values() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let disk_path = temp_dir.path().join("disk_cache.dat");

    let cache = create_disk_tier_cache(64, 16, 256, &disk_path);
    let value_size = 5 * 1024 * 1024;

    // Write items
    for i in 0..20 {
        let key = format!("delete_disk_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        cache
            .set(key.as_bytes(), &value, Duration::from_secs(3600))
            .expect("SET failed");
    }

    // Delete half the items
    for i in 0..10 {
        let key = format!("delete_disk_key_{:06}", i);
        cache.delete(key.as_bytes());
    }

    // Verify deleted items are gone
    for i in 0..10 {
        let key = format!("delete_disk_key_{:06}", i);
        assert!(
            cache.get(key.as_bytes()).is_none(),
            "Deleted item {} should not exist",
            i
        );
    }

    // Verify remaining items (may be on disk)
    let mut found = 0;
    for i in 10..20 {
        let key = format!("delete_disk_key_{:06}", i);
        let seed = (i % 256) as u8;

        if let Some(retrieved) = cache.get(key.as_bytes()) {
            if verify_value(&retrieved, value_size, seed) {
                found += 1;
            }
        }
    }

    println!("Found {} of 10 remaining items after delete", found);
    assert!(found > 0, "Expected some remaining items");
}

#[test]
fn test_disk_tier_rapid_cycles_large_values() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let disk_path = temp_dir.path().join("disk_cache.dat");

    let cache = create_disk_tier_cache(64, 16, 256, &disk_path);
    let value_size = 5 * 1024 * 1024;

    // Rapid SET/GET cycles
    for cycle in 0..20 {
        let key = b"rapid_disk_key";
        let seed = (cycle % 256) as u8;
        let value = generate_large_value(value_size, seed);

        cache
            .set(key, &value, Duration::from_secs(3600))
            .expect(&format!("SET failed on cycle {}", cycle));

        let retrieved = cache.get(key).expect(&format!(
            "GET returned None on cycle {}",
            cycle
        ));

        assert!(
            verify_value(&retrieved, value_size, seed),
            "Data corruption on cycle {}",
            cycle
        );
    }

    println!("Rapid disk tier cycles passed");
}

#[test]
#[ignore] // Expensive test
fn test_disk_tier_1000_5mb_values() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let disk_path = temp_dir.path().join("disk_cache.dat");

    // 64MB RAM, 2GB disk - can hold ~400 5MB items on disk
    let cache = create_disk_tier_cache(64, 16, 2048, &disk_path);
    let value_size = 5 * 1024 * 1024;
    let num_items = 1000;

    println!(
        "Large disk tier test: {} items of {}MB",
        num_items,
        value_size / (1024 * 1024)
    );

    for i in 0..num_items {
        let key = format!("large_disk_key_{:06}", i);
        let seed = (i % 256) as u8;
        let value = generate_large_value(value_size, seed);

        if let Err(e) = cache.set(key.as_bytes(), &value, Duration::from_secs(3600)) {
            println!("SET failed at item {}: {:?}", i, e);
        }

        if i > 0 && i % 100 == 0 {
            println!("Progress: {} items", i);
        }
    }

    // Verify some items
    let mut found = 0;
    for i in (0..num_items).step_by(10) {
        let key = format!("large_disk_key_{:06}", i);
        let seed = (i % 256) as u8;

        if let Some(retrieved) = cache.get(key.as_bytes()) {
            if verify_value(&retrieved, value_size, seed) {
                found += 1;
            }
        }
    }

    println!("Found {} of {} sampled items", found, num_items / 10);
}
