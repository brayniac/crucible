//! Benchmarks for MultiChoiceHashtable operations.
//!
//! Run with: cargo bench -p cache-core --bench hashtable

use cache_core::{Hashtable, KeyVerifier, Location, MultiChoiceHashtable};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

/// A verifier that uses a pre-built map for fast lookups.
/// This simulates a real cache's segment storage verification.
struct MapVerifier {
    map: HashMap<(Vec<u8>, u64), bool>,
}

impl MapVerifier {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    fn add(&mut self, key: &[u8], location: Location) {
        self.map.insert((key.to_vec(), location.as_raw()), true);
    }
}

impl KeyVerifier for MapVerifier {
    fn verify(&self, key: &[u8], location: Location, _allow_deleted: bool) -> bool {
        self.map.contains_key(&(key.to_vec(), location.as_raw()))
    }
}

/// A verifier that always returns true - useful for benchmarking
/// hashtable operations without verification overhead.
struct AlwaysTrueVerifier;

impl KeyVerifier for AlwaysTrueVerifier {
    fn verify(&self, _key: &[u8], _location: Location, _allow_deleted: bool) -> bool {
        true
    }
}

/// Generate a key from an index.
fn make_key(index: usize) -> Vec<u8> {
    format!("key:{:016x}", index).into_bytes()
}

/// Benchmark insert operations.
fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashtable/insert");

    for power in [14, 16, 18] {
        let num_buckets = 1usize << power;
        let capacity = num_buckets * 8; // 8 slots per bucket

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("single", format!("2^{}", power)),
            &power,
            |b, &power| {
                let ht = MultiChoiceHashtable::new(power);
                let verifier = AlwaysTrueVerifier;
                let mut idx = 0usize;

                b.iter(|| {
                    let key = make_key(idx);
                    let location = Location::new(idx as u64);
                    let _ = black_box(ht.insert(black_box(&key), location, &verifier));
                    idx = idx.wrapping_add(1) % capacity;
                });
            },
        );
    }

    group.finish();
}

/// Benchmark lookup hit operations.
fn bench_lookup_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashtable/lookup_hit");

    for power in [14, 16, 18] {
        let num_buckets = 1usize << power;
        let num_items = num_buckets * 4; // ~50% load factor

        // Pre-populate the hashtable
        let ht = MultiChoiceHashtable::new(power);
        let mut verifier = MapVerifier::new();

        for i in 0..num_items {
            let key = make_key(i);
            let location = Location::new(i as u64);
            verifier.add(&key, location);
            let _ = ht.insert(&key, location, &verifier);
        }

        let keys: Vec<Vec<u8>> = (0..num_items).map(make_key).collect();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("hit", format!("2^{}", power)),
            &power,
            |b, _| {
                let mut idx = 0usize;
                b.iter(|| {
                    let key = &keys[idx];
                    let result = black_box(ht.lookup(black_box(key), &verifier));
                    debug_assert!(result.is_some());
                    idx = (idx + 1) % num_items;
                });
            },
        );
    }

    group.finish();
}

/// Benchmark lookup miss operations.
fn bench_lookup_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashtable/lookup_miss");

    for power in [14, 16, 18] {
        let num_buckets = 1usize << power;
        let num_items = num_buckets * 4; // ~50% load factor

        // Pre-populate the hashtable
        let ht = MultiChoiceHashtable::new(power);
        let mut verifier = MapVerifier::new();

        for i in 0..num_items {
            let key = make_key(i);
            let location = Location::new(i as u64);
            verifier.add(&key, location);
            let _ = ht.insert(&key, location, &verifier);
        }

        // Generate keys that don't exist
        let miss_keys: Vec<Vec<u8>> = (num_items..num_items * 2).map(make_key).collect();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("miss", format!("2^{}", power)),
            &power,
            |b, _| {
                let mut idx = 0usize;
                b.iter(|| {
                    let key = &miss_keys[idx];
                    let result = black_box(ht.lookup(black_box(key), &verifier));
                    debug_assert!(result.is_none());
                    idx = (idx + 1) % num_items;
                });
            },
        );
    }

    group.finish();
}

/// Benchmark contains operations.
fn bench_contains(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashtable/contains");

    let power = 16u8;
    let num_buckets = 1usize << power;
    let num_items = num_buckets * 4;

    // Pre-populate
    let ht = MultiChoiceHashtable::new(power);
    let mut verifier = MapVerifier::new();

    for i in 0..num_items {
        let key = make_key(i);
        let location = Location::new(i as u64);
        verifier.add(&key, location);
        let _ = ht.insert(&key, location, &verifier);
    }

    let hit_keys: Vec<Vec<u8>> = (0..num_items).map(make_key).collect();
    let miss_keys: Vec<Vec<u8>> = (num_items..num_items * 2).map(make_key).collect();

    group.throughput(Throughput::Elements(1));

    group.bench_function("hit", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let key = &hit_keys[idx];
            let result = black_box(ht.contains(black_box(key), &verifier));
            debug_assert!(result);
            idx = (idx + 1) % num_items;
        });
    });

    group.bench_function("miss", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let key = &miss_keys[idx];
            let result = black_box(ht.contains(black_box(key), &verifier));
            debug_assert!(!result);
            idx = (idx + 1) % num_items;
        });
    });

    group.finish();
}

/// Benchmark remove operations.
fn bench_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashtable/remove");

    let power = 16u8;
    let num_buckets = 1usize << power;
    let num_items = num_buckets * 4;

    group.throughput(Throughput::Elements(1));

    group.bench_function("existing", |b| {
        b.iter_batched(
            || {
                // Setup: create and populate hashtable
                let ht = MultiChoiceHashtable::new(power);
                let mut verifier = MapVerifier::new();

                for i in 0..num_items {
                    let key = make_key(i);
                    let location = Location::new(i as u64);
                    verifier.add(&key, location);
                    let _ = ht.insert(&key, location, &verifier);
                }

                (ht, 0usize)
            },
            |(ht, mut idx)| {
                // Benchmark: remove items
                for _ in 0..1000 {
                    let key = make_key(idx);
                    let location = Location::new(idx as u64);
                    let _ = black_box(ht.remove(black_box(&key), location));
                    idx = (idx + 1) % num_items;
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark mixed workload (80% reads, 20% writes).
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashtable/mixed");

    let power = 16u8;
    let num_buckets = 1usize << power;
    let num_items = num_buckets * 4;

    // Pre-populate
    let ht = MultiChoiceHashtable::new(power);
    let mut verifier = MapVerifier::new();

    for i in 0..num_items {
        let key = make_key(i);
        let location = Location::new(i as u64);
        verifier.add(&key, location);
        let _ = ht.insert(&key, location, &verifier);
    }

    // Pre-generate all keys we might need for writes
    for i in num_items..num_items * 2 {
        let key = make_key(i);
        let location = Location::new(i as u64);
        verifier.add(&key, location);
    }

    let keys: Vec<Vec<u8>> = (0..num_items * 2).map(make_key).collect();

    group.throughput(Throughput::Elements(100));

    group.bench_function("80read_20write", |b| {
        let mut read_idx = 0usize;
        let mut write_idx = num_items;

        b.iter(|| {
            // 80 reads
            for _ in 0..80 {
                let key = &keys[read_idx % num_items];
                black_box(ht.lookup(black_box(key), &verifier));
                read_idx = read_idx.wrapping_add(1);
            }
            // 20 writes
            for _ in 0..20 {
                let key = &keys[write_idx % (num_items * 2)];
                let location = Location::new(write_idx as u64);
                let _ = black_box(ht.insert(black_box(key), location, &verifier));
                write_idx = write_idx.wrapping_add(1);
            }
        });
    });

    group.finish();
}

/// Benchmark concurrent operations.
fn bench_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashtable/concurrent");

    let power = 18u8;
    let num_buckets = 1usize << power;
    let num_items = num_buckets * 4;

    for num_threads in [2, 4, 8] {
        // Pre-populate
        let ht = Arc::new(MultiChoiceHashtable::new(power));
        let verifier = AlwaysTrueVerifier;

        for i in 0..num_items {
            let key = make_key(i);
            let location = Location::new(i as u64);
            let _ = ht.insert(&key, location, &verifier);
        }

        let items_per_thread = 10_000usize;

        group.throughput(Throughput::Elements(
            (num_threads * items_per_thread) as u64,
        ));

        group.bench_with_input(
            BenchmarkId::new("lookup", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let handles: Vec<_> = (0..num_threads)
                        .map(|t| {
                            let ht = Arc::clone(&ht);
                            thread::spawn(move || {
                                let verifier = AlwaysTrueVerifier;
                                let base = t * items_per_thread;
                                for i in 0..items_per_thread {
                                    let idx = (base + i) % num_items;
                                    let key = make_key(idx);
                                    black_box(ht.lookup(black_box(&key), &verifier));
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mixed", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let handles: Vec<_> = (0..num_threads)
                        .map(|t| {
                            let ht = Arc::clone(&ht);
                            thread::spawn(move || {
                                let verifier = AlwaysTrueVerifier;
                                let base = t * items_per_thread;
                                for i in 0..items_per_thread {
                                    let idx = (base + i) % num_items;
                                    let key = make_key(idx);
                                    if i % 5 == 0 {
                                        // 20% writes
                                        let location = Location::new(idx as u64);
                                        let _ = black_box(ht.insert(
                                            black_box(&key),
                                            location,
                                            &verifier,
                                        ));
                                    } else {
                                        // 80% reads
                                        black_box(ht.lookup(black_box(&key), &verifier));
                                    }
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark different N-choice configurations.
fn bench_choices(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashtable/choices");

    let power = 16u8;
    let num_buckets = 1usize << power;
    let num_items = num_buckets * 4;

    for num_choices in [1, 2, 3, 4] {
        // Pre-populate
        let ht = MultiChoiceHashtable::with_choices(power, num_choices);
        let mut verifier = MapVerifier::new();

        for i in 0..num_items {
            let key = make_key(i);
            let location = Location::new(i as u64);
            verifier.add(&key, location);
            let _ = ht.insert(&key, location, &verifier);
        }

        let keys: Vec<Vec<u8>> = (0..num_items).map(make_key).collect();

        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::new("lookup", format!("{}-choice", num_choices)),
            &num_choices,
            |b, _| {
                let mut idx = 0usize;
                b.iter(|| {
                    let key = &keys[idx];
                    black_box(ht.lookup(black_box(key), &verifier));
                    idx = (idx + 1) % num_items;
                });
            },
        );
    }

    group.finish();
}

/// Benchmark insert_if_absent (ADD semantics).
fn bench_insert_if_absent(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashtable/insert_if_absent");

    let power = 16u8;
    let num_buckets = 1usize << power;
    let capacity = num_buckets * 8;

    group.throughput(Throughput::Elements(1));

    group.bench_function("new_key", |b| {
        let ht = MultiChoiceHashtable::new(power);
        let verifier = AlwaysTrueVerifier;
        let mut idx = 0usize;

        b.iter(|| {
            let key = make_key(idx);
            let location = Location::new(idx as u64);
            let _ = black_box(ht.insert_if_absent(black_box(&key), location, &verifier));
            idx = idx.wrapping_add(1) % capacity;
        });
    });

    group.bench_function("existing_key", |b| {
        let ht = MultiChoiceHashtable::new(power);
        let mut verifier = MapVerifier::new();

        // Insert one key
        let key = make_key(0);
        let location = Location::new(0);
        verifier.add(&key, location);
        let _ = ht.insert(&key, location, &verifier);

        b.iter(|| {
            let new_location = Location::new(1);
            let result = black_box(ht.insert_if_absent(black_box(&key), new_location, &verifier));
            debug_assert!(result.is_err());
        });
    });

    group.finish();
}

/// Benchmark CAS operations.
fn bench_cas_location(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashtable/cas_location");

    let power = 16u8;
    let num_buckets = 1usize << power;
    let num_items = num_buckets * 4;

    // Pre-populate
    let ht = MultiChoiceHashtable::new(power);
    let mut verifier = MapVerifier::new();

    for i in 0..num_items {
        let key = make_key(i);
        let location = Location::new(i as u64);
        verifier.add(&key, location);
        let _ = ht.insert(&key, location, &verifier);
    }

    group.throughput(Throughput::Elements(1));

    group.bench_function("success", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let key = make_key(idx);
            let old_loc = Location::new(idx as u64);
            let new_loc = Location::new((idx + num_items) as u64);

            // CAS old -> new
            let _ = black_box(ht.cas_location(black_box(&key), old_loc, new_loc, true));
            // CAS new -> old (restore)
            let _ = black_box(ht.cas_location(black_box(&key), new_loc, old_loc, true));

            idx = (idx + 1) % num_items;
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_insert,
    bench_lookup_hit,
    bench_lookup_miss,
    bench_contains,
    bench_remove,
    bench_mixed_workload,
    bench_concurrent,
    bench_choices,
    bench_insert_if_absent,
    bench_cas_location,
);

criterion_main!(benches);
