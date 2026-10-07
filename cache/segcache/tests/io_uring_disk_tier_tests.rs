//! Tests for the io_uring disk tier (DirectIO / NVMe) demotion and read path.
//!
//! These tests exercise the `IoUringDiskLayer` through the `SegCache` API to
//! verify that items are demoted to disk write buffers on eviction and can
//! be read back. They also verify that flush queue and disk read params are
//! produced correctly.
//!
//! NOTE: These tests use the in-RAM write buffer path (no actual file I/O).
//! The server handler is responsible for submitting io_uring writes/reads.

use cache_core::{Cache, LookupResult};
use segcache::{IoUringDiskTierConfig, SegCache, SegCacheBuilder};
use std::time::Duration;

/// Build a small cache with io_uring disk tier configured.
///
/// - 8MB RAM for S3-FIFO (split: ~1 small queue segment, ~7 main segments)
/// - 1MB segments
/// - 8 disk segments with 4 write buffers
fn create_small_disk_cache() -> SegCache {
    SegCacheBuilder::new()
        .heap_size(8 * 1024 * 1024) // 8MB RAM
        .segment_size(1024 * 1024) // 1MB segments
        .hashtable_power(14) // 16K buckets
        .io_uring_disk_tier(IoUringDiskTierConfig {
            segment_count: 8,
            block_size: 4096,
            promotion_threshold: 2,
            write_buffer_count: 4,
            demotion_threshold: 0,
        })
        .build()
        .expect("Failed to create cache with io_uring disk tier")
}

/// Write a deterministic value for a given key index.
fn make_value(i: usize) -> Vec<u8> {
    // 512-byte values to fit many per segment but still be non-trivial
    let seed = format!("v{:06}_", i);
    seed.as_bytes().iter().copied().cycle().take(512).collect()
}

#[test]
fn test_demotion_happens() {
    let cache = create_small_disk_cache();

    // Write enough items to fill 2MB of RAM and trigger eviction.
    // 1MB segments, ~2 segments in RAM. Each item is ~512B + overhead ~= 560B.
    // So ~1800 items per segment, ~3600 total in RAM.
    // Write 8000 items to guarantee eviction.
    let num_items = 8000;
    for i in 0..num_items {
        let key = format!("k:{}", i);
        let value = make_value(i);
        cache
            .set(key.as_bytes(), &value, Duration::from_secs(3600))
            .unwrap_or_else(|e| panic!("SET failed for key {}: {:?}", i, e));
    }

    let stats = cache
        .internal_stats()
        .expect("internal_stats should be Some for segcache");

    eprintln!(
        "demotions={}, evictions={}",
        stats.demotions, stats.evictions
    );

    assert!(
        stats.demotions > 0,
        "Expected demotions > 0, got {}. Items are not being demoted to disk.",
        stats.demotions
    );
}

/// `demotion_threshold` gates which RAM evictions reach disk. A write is
/// not a read, so at 1 an item nobody read is discarded, not demoted.
#[test]
fn demotion_threshold_one_demotes_only_items_that_were_read() {
    let build = |demotion_threshold| {
        SegCacheBuilder::new()
            .heap_size(8 * 1024 * 1024)
            .segment_size(1024 * 1024)
            .hashtable_power(14)
            .io_uring_disk_tier(IoUringDiskTierConfig {
                segment_count: 8,
                block_size: 4096,
                promotion_threshold: 2,
                write_buffer_count: 4,
                demotion_threshold,
            })
            .build()
            .expect("Failed to create cache with io_uring disk tier")
    };
    let fill = |cache: &SegCache, read: usize| {
        for i in 0..8000 {
            let key = format!("k:{}", i);
            cache
                .set(key.as_bytes(), &make_value(i), Duration::from_secs(3600))
                .unwrap_or_else(|e| panic!("SET failed for key {}: {:?}", i, e));
            // Read the first `read` items once while they are still in RAM.
            if i < read {
                assert!(cache.get(key.as_bytes()).is_some());
            }
        }
        cache.internal_stats().unwrap().demotions
    };

    let unread = fill(&build(1), 0);
    assert_eq!(unread, 0, "threshold 1 demoted {unread} items nobody read");

    let read = fill(&build(1), 1000);
    assert!(
        read > 0 && read <= 1000,
        "threshold 1 should demote only the 1000 read items, demoted {read}"
    );

    let all = fill(&build(0), 0);
    assert!(
        all > 1000,
        "threshold 0 should demote unread items, demoted {all}"
    );
}

#[test]
fn test_demoted_items_readable_from_write_buffer() {
    let cache = create_small_disk_cache();

    // Phase 1: Fill the cache to trigger demotion.
    let num_items = 8000;
    for i in 0..num_items {
        let key = format!("k:{}", i);
        let value = make_value(i);
        let _ = cache.set(key.as_bytes(), &value, Duration::from_secs(3600));
    }

    let stats = cache.internal_stats().unwrap();
    eprintln!(
        "demotions={}, evictions={}",
        stats.demotions, stats.evictions
    );

    // Phase 2: Read back ALL items and classify results.
    let mut ram_hits = 0;
    let mut disk_read_requests = 0;
    let mut misses = 0;

    for i in 0..num_items {
        let key = format!("k:{}", i);
        match cache.lookup(key.as_bytes()) {
            LookupResult::Hit(_) => ram_hits += 1,
            LookupResult::DiskRead(_) => disk_read_requests += 1,
            LookupResult::Miss => misses += 1,
        }
    }

    eprintln!(
        "ram_hits={}, disk_read_requests={}, misses={} (total={})",
        ram_hits,
        disk_read_requests,
        misses,
        ram_hits + disk_read_requests + misses
    );

    // With 4 write buffers, up to 4 segments of demoted items should be
    // readable from write buffers (returned as Hit, not DiskRead).
    // Items whose write buffers were flushed+detached would be DiskRead.
    // Items that couldn't be demoted (buffer pool exhausted) are misses.
    let total_found = ram_hits + disk_read_requests;
    assert!(
        total_found > 0,
        "Expected some items found (ram_hits + disk_reads), got 0. \
         demotions={}, evictions={}",
        stats.demotions,
        stats.evictions,
    );

    // The hit rate should be significantly better than without disk tier.
    // With 2MB RAM + 4 disk write buffers (4MB), we should retain ~6MB
    // worth of items out of ~4MB written.
    let hit_pct = (total_found as f64 / num_items as f64) * 100.0;
    eprintln!(
        "effective hit rate: {:.1}% ({} / {})",
        hit_pct, total_found, num_items
    );
}

#[test]
fn test_demoted_items_have_correct_values() {
    let cache = create_small_disk_cache();

    // Write items
    let num_items = 8000;
    for i in 0..num_items {
        let key = format!("k:{}", i);
        let value = make_value(i);
        let _ = cache.set(key.as_bytes(), &value, Duration::from_secs(3600));
    }

    // Read back and verify values. Use get() which returns data for
    // RAM hits and write-buffer hits, but returns None for DiskRead.
    let mut verified = 0;
    let mut found = 0;

    for i in 0..num_items {
        let key = format!("k:{}", i);
        let expected = make_value(i);
        if let Some(got) = cache.get(key.as_bytes()) {
            found += 1;
            assert_eq!(
                got,
                expected,
                "Data corruption for key {} (len got={}, expected={})",
                i,
                got.len(),
                expected.len()
            );
            verified += 1;
        }
    }

    let stats = cache.internal_stats().unwrap();
    eprintln!(
        "found={}, verified={}, demotions={}, evictions={}",
        found, verified, stats.demotions, stats.evictions
    );

    assert!(found > 0, "Expected some items readable via get(), got 0");
    assert_eq!(found, verified, "All found items should have correct data");
}

#[test]
fn test_lookup_returns_disk_read_after_buffer_detach() {
    // This test verifies the full lifecycle:
    // 1. Items are demoted to disk write buffers
    // 2. lookup() returns Hit (from write buffer) initially
    // 3. After simulating flush completion (buffer detached), lookup returns DiskRead
    use segcache::CacheLayer;

    let cache = create_small_disk_cache();

    // Fill the cache to trigger demotion.
    let num_items = 8000;
    for i in 0..num_items {
        let key = format!("k:{}", i);
        let value = make_value(i);
        let _ = cache.set(key.as_bytes(), &value, Duration::from_secs(3600));
    }

    let stats = cache.internal_stats().unwrap();
    eprintln!(
        "demotions={}, evictions={}",
        stats.demotions, stats.evictions
    );
    assert!(stats.demotions > 0, "Expected demotions > 0");

    // Count items by lookup result before flush
    let mut hit_count = 0;
    let mut disk_read_count = 0;
    let mut miss_count = 0;

    for i in 0..num_items {
        let key = format!("k:{}", i);
        match cache.lookup(key.as_bytes()) {
            LookupResult::Hit(_) => hit_count += 1,
            LookupResult::DiskRead(_) => disk_read_count += 1,
            LookupResult::Miss => miss_count += 1,
        }
    }

    eprintln!(
        "Before flush: hits={}, disk_reads={}, misses={}",
        hit_count, disk_read_count, miss_count
    );

    // Hits should include items in both RAM and disk write buffers.
    assert!(
        hit_count > 0,
        "Expected some hits (RAM + write buffer), got 0"
    );

    // Now drain the flush queue and complete all flushes.
    // This detaches write buffers, so subsequent lookups should return DiskRead.
    let disk_idx = (0..cache.layer_count())
        .find(|&i| matches!(cache.layer(i), Some(CacheLayer::IoUringDisk(_))))
        .expect("No IoUringDisk layer found");
    let disk_layer = match cache.layer(disk_idx) {
        Some(CacheLayer::IoUringDisk(dl)) => dl,
        _ => unreachable!(),
    };

    let flush_requests = disk_layer.take_flush_queue();
    eprintln!("flush_requests: {}", flush_requests.len());

    for req in &flush_requests {
        disk_layer.complete_flush(req.segment_id);
    }

    // Now re-lookup: items that were in write buffers should now be DiskRead.
    let mut hit_after = 0;
    let mut disk_read_after = 0;
    let mut miss_after = 0;

    for i in 0..num_items {
        let key = format!("k:{}", i);
        match cache.lookup(key.as_bytes()) {
            LookupResult::Hit(_) => hit_after += 1,
            LookupResult::DiskRead(params) => {
                disk_read_after += 1;
                // Verify the DiskReadParams look sane
                assert!(params.read_len > 0, "read_len should be > 0");
                assert!(
                    params.item_offset < params.read_len,
                    "item_offset should be within read_len for key k:{}",
                    i
                );
            }
            LookupResult::Miss => miss_after += 1,
        }
    }

    eprintln!(
        "After flush: hits={}, disk_reads={}, misses={}",
        hit_after, disk_read_after, miss_after
    );

    // After detaching write buffers, we should see DiskRead results
    // for items that were previously Hit from write buffers.
    assert!(
        disk_read_after > 0,
        "Expected some DiskRead results after flush completion, got 0. \
         The flush queue had {} requests. \
         Items are not transitioning from write-buffer to disk-read.",
        flush_requests.len()
    );
}

#[test]
fn test_disk_read_params_point_to_correct_data() {
    // Verify that DiskReadParams after flush point to correct item data.
    use segcache::{BasicHeader, CacheLayer};

    let segment_size = 1024 * 1024; // must match create_small_disk_cache
    let cache = create_small_disk_cache();

    // Fill cache to trigger demotion
    let num_items = 8000;
    for i in 0..num_items {
        let key = format!("k:{}", i);
        let value = make_value(i);
        let _ = cache.set(key.as_bytes(), &value, Duration::from_secs(3600));
    }

    let stats = cache.internal_stats().unwrap();
    assert!(stats.demotions > 0, "Expected demotions > 0");

    // Get the disk layer and flush all write buffers
    let disk_idx = (0..cache.layer_count())
        .find(|&i| matches!(cache.layer(i), Some(CacheLayer::IoUringDisk(_))))
        .expect("No IoUringDisk layer found");
    let disk_layer = match cache.layer(disk_idx) {
        Some(CacheLayer::IoUringDisk(dl)) => dl,
        _ => unreachable!(),
    };

    let flush_requests = disk_layer.take_flush_queue();
    eprintln!("flush_requests: {}", flush_requests.len());

    // Save the write buffer contents before detaching (simulating what
    // the server would write to the disk file).
    let mut disk_data: Vec<u8> = vec![0u8; 8 * segment_size]; // 8 disk segments
    for req in &flush_requests {
        let src = unsafe { std::slice::from_raw_parts(req.buffer_ptr, req.data_len as usize) };
        let offset = req.disk_offset as usize;
        disk_data[offset..offset + src.len()].copy_from_slice(src);
    }

    // Complete flushes (detaches write buffers)
    for req in &flush_requests {
        disk_layer.complete_flush(req.segment_id);
    }

    // Now lookup items and use DiskReadParams to read from our saved disk_data
    let mut verified = 0;
    let mut disk_reads = 0;
    let mut tag_collisions = 0;

    for i in 0..num_items {
        let key = format!("k:{}", i);
        let expected_value = make_value(i);

        if let LookupResult::DiskRead(params) = cache.lookup(key.as_bytes()) {
            disk_reads += 1;

            // Read from our saved disk data
            let offset = params.disk_offset as usize;
            let len = params.read_len as usize;
            if offset + len > disk_data.len() {
                continue; // Skip if out of range
            }

            let block = &disk_data[offset..offset + len];
            let item_offset = params.item_offset as usize;

            // Parse BasicHeader at item_offset
            if item_offset + BasicHeader::SIZE > block.len() {
                continue;
            }
            // Decoded from a private copy: `try_from_ptr` reads the flags
            // byte through an atomic view, which needs provenance permitting
            // writes, and a `&[u8]` cannot give that.
            let mut header_bytes = [0u8; BasicHeader::SIZE];
            header_bytes.copy_from_slice(&block[item_offset..item_offset + BasicHeader::SIZE]);
            let header = match unsafe { BasicHeader::try_from_ptr(header_bytes.as_mut_ptr()) } {
                Some(h) => h,
                None => continue,
            };

            // Extract key and value from the block
            let key_start = item_offset + BasicHeader::SIZE + header.optional_len() as usize;
            let key_end = key_start + header.key_len() as usize;
            let value_start = key_end;
            let value_end = value_start + header.value_len() as usize;

            if value_end > block.len() {
                continue;
            }

            // Verify the key matches — after write buffer detach, the hashtable
            // lookup trusts the 12-bit tag match without key verification. The
            // server verifies the key after the async disk read completes; we
            // must do the same here. Tag collisions are expected and harmless.
            let actual_key = &block[key_start..key_end];
            if actual_key != key.as_bytes() {
                tag_collisions += 1;
                continue;
            }

            let actual_value = &block[value_start..value_end];
            assert_eq!(
                actual_value,
                &expected_value[..],
                "Data mismatch for key k:{} (disk_offset={}, item_offset={})",
                i,
                params.disk_offset,
                params.item_offset
            );
            verified += 1;
        }
    }

    eprintln!(
        "disk_reads={}, verified={}, tag_collisions={}, demotions={}",
        disk_reads, verified, tag_collisions, stats.demotions
    );

    assert!(
        disk_reads > 0,
        "Expected some DiskRead results after flush, got 0"
    );
    assert!(
        verified > 0,
        "Expected some items verified from disk data, got 0"
    );
    // All disk reads should either verify correctly or be tag collisions
    assert_eq!(
        disk_reads,
        verified + tag_collisions,
        "All disk reads should either verify or be tag collisions"
    );
}

/// Fill the cache with numeric values, then complete every pending flush so
/// demoted items sit on committed disk segments. Returns a key whose value
/// is now only on disk (its lookup is `DiskRead`).
fn a_counter_on_committed_disk() -> (std::sync::Arc<SegCache>, String) {
    use segcache::CacheLayer;

    let cache = std::sync::Arc::new(create_small_disk_cache());
    // "10", zero-padded so items are large enough to force demotion. The
    // padding parses: `increment` trims and reads the value as a u64.
    let value = format!("{:0>512}", 10);
    for i in 0..8000 {
        let _ = cache.set(
            format!("k:{i}").as_bytes(),
            value.as_bytes(),
            Duration::from_secs(3600),
        );
    }

    let disk_idx = (0..cache.layer_count())
        .find(|&i| matches!(cache.layer(i), Some(CacheLayer::IoUringDisk(_))))
        .expect("no io_uring disk layer");
    let Some(CacheLayer::IoUringDisk(disk)) = cache.layer(disk_idx) else {
        unreachable!()
    };
    for req in disk.take_flush_queue() {
        disk.complete_flush(req.segment_id);
    }

    let key = (0..8000)
        .map(|i| format!("k:{i}"))
        .find(|k| matches!(cache.lookup(k.as_bytes()), LookupResult::DiskRead(_)))
        .expect("precondition: some item is on a committed disk segment");
    (cache, key)
}

/// `increment` and `decrement` must return for an item on a committed disk
/// segment, with an error rather than an answer from the wrong value.
///
/// They read through the synchronous item path, which returns `None` for a
/// committed io_uring segment -- that read has to go through the ring. Both
/// treat `None` with the key still indexed as "transiently unreadable" and
/// retry, and nothing changes between retries, so they spin forever. On a
/// server that is a worker thread lost to one incr of a cold counter.
///
/// `initial` is set, so a fix that read the item as absent would return 1 (a
/// fresh counter) and fail too.
#[test]
fn a_counter_on_committed_disk_does_not_hang() {
    for (name, op, want) in [("increment", true, 11u64), ("decrement", false, 9u64)] {
        let (cache, key) = a_counter_on_committed_disk();

        // The state the retry loop cannot leave: the synchronous read finds
        // nothing, and the hashtable still indexes the key. Checked directly,
        // so a timeout below is this and not some other stall.
        assert!(
            cache.get(key.as_bytes()).is_none(),
            "{key} is readable synchronously"
        );
        assert!(cache.contains(key.as_bytes()), "{key} is no longer indexed");

        let (tx, rx) = std::sync::mpsc::channel();
        {
            let (cache, key) = (cache.clone(), key.clone());
            std::thread::spawn(move || {
                let ttl = Some(Duration::from_secs(3600));
                let r = if op {
                    cache.increment(key.as_bytes(), 1, Some(0), ttl)
                } else {
                    cache.decrement(key.as_bytes(), 1, Some(0), ttl)
                };
                let _ = tx.send(r);
            });
        }

        // An error, not a guess: the read has to go through the ring, which a
        // synchronous incr cannot drive. Promoting the counter back into RAM
        // (the async path GET already takes) would make this `Ok(want)`.
        match rx.recv_timeout(Duration::from_secs(5)) {
            Err(_) => panic!("{name} on {key} did not return within 5s"),
            Ok(got) => assert_eq!(
                got,
                Err(cache_core::CacheError::SegmentNotAccessible),
                "{name} on {key} (the value is 10, so success would be {want})"
            ),
        }
    }
}

/// `append` and `prepend` on an item that is only on a committed disk
/// segment return an error and leave the item alone.
///
/// The synchronous read returns nothing for such an item. Reported as
/// `KeyNotFound`, RESP APPEND treated the key as deleted and stored the
/// appended bytes as the whole value.
#[test]
fn append_on_committed_disk_does_not_replace_the_value() {
    for (name, op) in [("append", true), ("prepend", false)] {
        let (cache, key) = a_counter_on_committed_disk();
        let r = if op {
            cache.append(key.as_bytes(), b"tail")
        } else {
            cache.prepend(key.as_bytes(), b"head")
        };
        assert_eq!(
            r,
            Err(cache_core::CacheError::SegmentNotAccessible),
            "{name} on {key}"
        );
        assert!(
            matches!(cache.lookup(key.as_bytes()), LookupResult::DiskRead(_)),
            "{name} on {key} replaced the item on disk"
        );
    }
}
