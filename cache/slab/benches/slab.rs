//! Benchmarks for slab-cache hot paths.
//!
//! These benchmarks exercise the performance-critical paths:
//! - Hashtable lookup with key verification (exercises verify path)
//! - Cache get operations (exercises prefetch + verify + value read)
//! - Cache set operations (exercises allocation + write)
//!
//! Run with: cargo bench -p slab-cache --bench slab

use cache_core::Cache;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use slab_cache::SlabCacheBuilder;
use std::time::Duration;

/// Generate a key from an index.
fn make_key(index: usize) -> Vec<u8> {
    format!("key:{:016x}", index).into_bytes()
}

/// Generate a value of specified size.
fn make_value(size: usize) -> Vec<u8> {
    vec![0xAB; size]
}

/// Benchmark cache get operations (exercises prefetch + verify + read).
fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("slab/get");

    for (num_items, value_size) in [(10_000, 64), (100_000, 64), (10_000, 1024)] {
        // Calculate heap size needed (rough estimate)
        let heap_size = num_items * (value_size + 64) * 2; // 2x for headroom
        let heap_size = heap_size.max(64 * 1024 * 1024); // minimum 64MB

        let cache = SlabCacheBuilder::new()
            .heap_size(heap_size)
            .slab_size(1024 * 1024)
            .build()
            .unwrap();

        // Pre-populate
        let mut keys = Vec::with_capacity(num_items);
        let value = make_value(value_size);

        for i in 0..num_items {
            let key = make_key(i);
            cache
                .set(&key, &value, Some(Duration::from_secs(3600)))
                .unwrap();
            keys.push(key);
        }

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("hit", format!("{}items_{}B", num_items, value_size)),
            &num_items,
            |b, _| {
                let mut idx = 0usize;
                b.iter(|| {
                    let key = &keys[idx];
                    let result = cache.get(black_box(key));
                    debug_assert!(result.is_some());
                    black_box(result);
                    idx = (idx + 1) % keys.len();
                });
            },
        );
    }

    // Benchmark miss case
    let cache = SlabCacheBuilder::new()
        .heap_size(64 * 1024 * 1024)
        .build()
        .unwrap();

    // Populate with some items
    for i in 0..10_000 {
        let key = make_key(i);
        cache
            .set(&key, &make_value(64), Some(Duration::from_secs(3600)))
            .unwrap();
    }

    // Generate miss keys
    let miss_keys: Vec<Vec<u8>> = (10_000..20_000).map(make_key).collect();

    group.throughput(Throughput::Elements(1));
    group.bench_function("miss", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let key = &miss_keys[idx];
            let result = cache.get(black_box(key));
            debug_assert!(result.is_none());
            black_box(result);
            idx = (idx + 1) % miss_keys.len();
        });
    });

    group.finish();
}

/// Benchmark cache set operations (exercises allocation + write).
fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("slab/set");

    for value_size in [64, 256, 1024] {
        let cache = SlabCacheBuilder::new()
            .heap_size(256 * 1024 * 1024)
            .slab_size(1024 * 1024)
            .build()
            .unwrap();

        let value = make_value(value_size);
        let ttl = Some(Duration::from_secs(3600));

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("new_key", format!("{}B", value_size)),
            &value_size,
            |b, _| {
                let mut idx = 0usize;
                b.iter(|| {
                    let key = make_key(idx);
                    let _ = cache.set(black_box(&key), black_box(&value), ttl);
                    idx = idx.wrapping_add(1);
                });
            },
        );
    }

    // Benchmark update (overwrite existing key)
    let cache = SlabCacheBuilder::new()
        .heap_size(64 * 1024 * 1024)
        .build()
        .unwrap();

    let num_items = 10_000;
    let keys: Vec<Vec<u8>> = (0..num_items).map(make_key).collect();
    let value = make_value(64);
    let ttl = Some(Duration::from_secs(3600));

    // Pre-populate
    for key in &keys {
        cache.set(key, &value, ttl).unwrap();
    }

    group.throughput(Throughput::Elements(1));
    group.bench_function("update", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let key = &keys[idx];
            let _ = cache.set(black_box(key), black_box(&value), ttl);
            idx = (idx + 1) % keys.len();
        });
    });

    group.finish();
}

/// Benchmark cache delete operations.
fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("slab/delete");

    group.throughput(Throughput::Elements(1));

    group.bench_function("existing", |b| {
        b.iter_batched(
            || {
                // Setup: create and populate cache
                let cache = SlabCacheBuilder::new()
                    .heap_size(64 * 1024 * 1024)
                    .build()
                    .unwrap();

                let num_items = 10_000;
                for i in 0..num_items {
                    let key = make_key(i);
                    cache
                        .set(&key, &make_value(64), Some(Duration::from_secs(3600)))
                        .unwrap();
                }

                (cache, 0usize, num_items)
            },
            |(cache, mut idx, num_items)| {
                // Benchmark: delete items
                for _ in 0..1000 {
                    let key = make_key(idx);
                    let _ = black_box(cache.delete(black_box(&key)));
                    idx = (idx + 1) % num_items;
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark contains check (fast path - no value read).
fn bench_contains(c: &mut Criterion) {
    let mut group = c.benchmark_group("slab/contains");

    let cache = SlabCacheBuilder::new()
        .heap_size(64 * 1024 * 1024)
        .build()
        .unwrap();

    let num_items = 10_000;
    let keys: Vec<Vec<u8>> = (0..num_items).map(make_key).collect();

    // Pre-populate
    for key in &keys {
        cache
            .set(key, &make_value(64), Some(Duration::from_secs(3600)))
            .unwrap();
    }

    let miss_keys: Vec<Vec<u8>> = (num_items..num_items * 2).map(make_key).collect();

    group.throughput(Throughput::Elements(1));

    group.bench_function("hit", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let key = &keys[idx];
            let result = cache.contains(black_box(key));
            debug_assert!(result);
            black_box(result);
            idx = (idx + 1) % keys.len();
        });
    });

    group.bench_function("miss", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let key = &miss_keys[idx];
            let result = cache.contains(black_box(key));
            debug_assert!(!result);
            black_box(result);
            idx = (idx + 1) % miss_keys.len();
        });
    });

    group.finish();
}

/// Benchmark mixed workload (80% reads, 20% writes).
fn bench_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("slab/mixed");

    let cache = SlabCacheBuilder::new()
        .heap_size(128 * 1024 * 1024)
        .build()
        .unwrap();

    let num_items = 50_000;
    let keys: Vec<Vec<u8>> = (0..num_items).map(make_key).collect();
    let value = make_value(64);
    let ttl = Some(Duration::from_secs(3600));

    // Pre-populate
    for key in &keys {
        cache.set(key, &value, ttl).unwrap();
    }

    group.throughput(Throughput::Elements(100));

    group.bench_function("80read_20write", |b| {
        let mut read_idx = 0usize;
        let mut write_idx = 0usize;

        b.iter(|| {
            // 80 reads
            for _ in 0..80 {
                let key = &keys[read_idx % num_items];
                black_box(cache.get(black_box(key)));
                read_idx = read_idx.wrapping_add(1);
            }
            // 20 writes
            for _ in 0..20 {
                let key = &keys[write_idx % num_items];
                let _ = black_box(cache.set(black_box(key), black_box(&value), ttl));
                write_idx = write_idx.wrapping_add(1);
            }
        });
    });

    group.finish();
}

/// Benchmark different value sizes.
fn bench_value_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("slab/value_size");

    for value_size in [32, 64, 128, 256, 512, 1024, 4096] {
        let cache = SlabCacheBuilder::new()
            .heap_size(128 * 1024 * 1024)
            .build()
            .unwrap();

        let num_items = 10_000;
        let keys: Vec<Vec<u8>> = (0..num_items).map(make_key).collect();
        let value = make_value(value_size);
        let ttl = Some(Duration::from_secs(3600));

        // Pre-populate
        for key in &keys {
            cache.set(key, &value, ttl).unwrap();
        }

        group.throughput(Throughput::Bytes(value_size as u64));
        group.bench_with_input(
            BenchmarkId::new("get", format!("{}B", value_size)),
            &value_size,
            |b, _| {
                let mut idx = 0usize;
                b.iter(|| {
                    let key = &keys[idx];
                    let result = cache.get(black_box(key));
                    black_box(result);
                    idx = (idx + 1) % keys.len();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_get,
    bench_set,
    bench_delete,
    bench_contains,
    bench_mixed,
    bench_value_sizes,
);

criterion_main!(benches);
