# cache-rs Comparison Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add pelikan-io/cache-rs as a `cache-bench` backend so engine+policy pairs can be ranked against each other at a fixed memory budget, by hit ratio and by write-tail latency.

**Architecture:** cache-rs's `Segcache` is wrapped in an adapter implementing crucible's `Cache` trait, behind an optional `cache-rs` cargo feature. Every existing piece of the replay rig — trace reader, driver, `envelope_verdict`, latency histograms — then applies to both arms unchanged, so the harness is not itself a difference between them.

**Tech Stack:** Rust; `segcache` crate from crates.io renamed to `cache_rs` (crucible already has a crate called `segcache`), patched to git `main`; SystemsLab for execution.

**Design:** `docs/superpowers/specs/2026-09-19-cache-rs-comparison-design.md`

---

## File Structure

| File | Responsibility |
|---|---|
| `cache-bench/Cargo.toml` | Optional renamed dependency + `cache-rs` feature |
| `Cargo.toml` (workspace) | `[patch.crates-io]` pinning cache-rs to git main |
| `cache-bench/src/cachers.rs` | **New.** The adapter: `Cache` impl over `cache_rs::Segcache`, plus policy mapping and the `hash_power` conversion |
| `cache-bench/src/config.rs` | `CacheBackend::CacheRs` variant; `cachers_policy` field |
| `cache-bench/src/main.rs` | `create_cachers()` constructor, wired into `run()` |
| `.github/workflows/*` | Clippy rung for the new feature |

The adapter is its own file rather than living in `main.rs`: it is the only place cache-rs types appear, and keeping that boundary in one file means the feature-gating is one `#[cfg]` on one `mod` line.

---

### Task 1: The `hash_power` conversion

The single most dangerous line in this work. crucible's `hashtable_power` counts **buckets** (`num_buckets = 1 << power`); cache-rs's `hash_power` counts **slots** (`bucket_power = power - 3`, 8 slots per bucket). Copying the number gives cache-rs a table 8x too small, and table pressure alone moved miss ratio 0.2810 → 0.2958 on this trace — indistinguishable from a policy result.

This task is pure arithmetic with no cache-rs dependency, so it lands first and independently.

**Files:**
- Create: `cache-bench/src/cachers.rs`
- Modify: `cache-bench/src/main.rs` (add `mod cachers;`)

- [ ] **Step 1: Write the failing test**

Create `cache-bench/src/cachers.rs`:

```rust
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
pub fn cachers_hash_power(crucible_power: u8) -> u8 {
    todo!("cachers_hash_power")
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
```

Add to `cache-bench/src/main.rs`, after the `mod config;` line:

```rust
mod cachers;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cache-bench cachers::`
Expected: FAIL, both tests panic with `not yet implemented: cachers_hash_power`

- [ ] **Step 3: Write minimal implementation**

Replace the `todo!` body:

```rust
pub fn cachers_hash_power(crucible_power: u8) -> u8 {
    crucible_power + 3
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cache-bench cachers::`
Expected: PASS, 2 passed

- [ ] **Step 5: Commit**

```bash
git add cache-bench/src/cachers.rs cache-bench/src/main.rs
git commit -m "feat(cache-bench): convert hashtable power between crucible and cache-rs

crucible's power counts buckets, cache-rs's counts slots, and the parameter
has the same name in both. Unconverted, cache-rs gets a table 8x smaller,
and table pressure alone is worth ~5% miss ratio -- indistinguishable from
a policy result."
```

---

### Task 2: The dependency and feature

**Files:**
- Modify: `cache-bench/Cargo.toml`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Add the renamed optional dependency**

In `cache-bench/Cargo.toml`, after the `zstd = "0.13"` line:

```toml
# Renamed: crucible already has a crate called `segcache`. Optional so the
# default build does not pay for a second cache engine.
cache_rs = { package = "segcache", version = "0.4", optional = true }
```

And add a `[features]` section before `[dev-dependencies]`:

```toml
[features]
default = []
cache-rs = ["dep:cache_rs"]
```

- [ ] **Step 2: Pin cache-rs to git main**

In the workspace root `Cargo.toml`, at the end of the file:

```toml
# The comparison is crucible main against cache-rs main. crates.io is at
# 0.4.4; main carries work that is not released yet. Pinned rather than
# floating so a result can name the code it measured.
[patch.crates-io]
segcache = { git = "https://github.com/pelikan-io/cache-rs", branch = "main" }
```

- [ ] **Step 3: Verify both build configurations resolve**

Run: `cargo check -p cache-bench && cargo check -p cache-bench --features cache-rs`
Expected: both succeed. The second downloads and compiles cache-rs from git.

If the patch fails with "patch for `segcache` in `https://github.com/rust-lang/crates.io-index` did not resolve to any crates", the package is not at the repo root — add `path` disambiguation is not needed, cargo scans the workspace, but confirm the crate name is exactly `segcache` in `crates/segcache/Cargo.toml`.

- [ ] **Step 4: Commit**

```bash
git add cache-bench/Cargo.toml Cargo.toml Cargo.lock
git commit -m "build(cache-bench): optional cache-rs dependency, pinned to main

Renamed because crucible has its own segcache crate. Behind a feature so
the default build does not carry a second cache engine."
```

---

### Task 3: Policy mapping

**Files:**
- Modify: `cache-bench/src/cachers.rs`

- [ ] **Step 1: Write the failing test**

Add to `cache-bench/src/cachers.rs`, above the `#[cfg(test)]` module:

```rust
/// Which cache-rs policy answers a given crucible policy.
///
/// Returns `None` where crucible has no counterpart, so an unmatched pair is
/// rejected at config time rather than silently compared against a default.
#[cfg(feature = "cache-rs")]
pub fn cachers_policy(policy: crate::config::EvictionPolicy) -> Option<cache_rs::Policy> {
    todo!("cachers_policy")
}
```

Add these tests inside the existing `mod tests`:

```rust
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cache-bench --features cache-rs cachers::`
Expected: FAIL with `not yet implemented: cachers_policy`

- [ ] **Step 3: Write minimal implementation**

```rust
#[cfg(feature = "cache-rs")]
pub fn cachers_policy(policy: crate::config::EvictionPolicy) -> Option<cache_rs::Policy> {
    use crate::config::EvictionPolicy as P;
    Some(match policy {
        P::Fifo => cache_rs::Policy::Fifo,
        P::Cte => cache_rs::Policy::Cte,
        P::Random => cache_rs::Policy::Random,
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cache-bench --features cache-rs cachers::`
Expected: PASS, 4 passed

- [ ] **Step 5: Commit**

```bash
git add cache-bench/src/cachers.rs
git commit -m "feat(cache-bench): map crucible eviction policies to cache-rs

Unmatched policies return None so the pair is rejected at config time
rather than silently compared against a default, which would put two
different algorithms under one label."
```

---

### Task 4: The adapter's read and write paths

**Files:**
- Modify: `cache-bench/src/cachers.rs`

- [ ] **Step 1: Write the failing test**

Add above the test module:

```rust
#[cfg(feature = "cache-rs")]
mod adapter {
    use cache_core::{Cache, CacheError, OwnedGuard, ValueRef};
    use std::time::Duration;

    /// cache-rs's `Segcache` behind crucible's `Cache` trait.
    pub struct CacheRs {
        pub(super) inner: cache_rs::Segcache,
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
            todo!("CacheRs::with_value")
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
            todo!("CacheRs::set")
        }

        fn delete(&self, key: &[u8]) -> bool {
            self.inner.delete(key)
        }

        fn contains(&self, key: &[u8]) -> bool {
            self.inner.get(key).is_some()
        }

        fn flush(&self) {
            // cache-rs exposes no flush; the replay driver never calls it.
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
}

#[cfg(feature = "cache-rs")]
pub use adapter::CacheRs;
```

Add these tests inside `mod tests`:

```rust
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cache-bench --features cache-rs cachers::`
Expected: FAIL with `not yet implemented: CacheRs::with_value` and `CacheRs::set`

- [ ] **Step 3: Write minimal implementation**

Replace the two `todo!` bodies:

```rust
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
```

```rust
        fn set(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
            // crucible's `None` TTL means no expiry; cache-rs takes a
            // Duration, and zero is its no-expiry encoding.
            let ttl = ttl.unwrap_or(Duration::ZERO);
            self.inner
                .insert(key, value, None, ttl)
                .map_err(|_| CacheError::OutOfMemory)
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cache-bench --features cache-rs cachers::`
Expected: PASS, 7 passed

- [ ] **Step 5: Commit**

```bash
git add cache-bench/src/cachers.rs
git commit -m "feat(cache-bench): adapt cache-rs Segcache to the Cache trait

get_value_ref returns None and says why at the site: cache-rs has no
refcount-pinned equivalent, and a caller reading None as a miss would
report a catastrophic hit ratio that looks like a result rather than a
wiring error. The replay driver uses with_value and never calls it."
```

---

### Task 5: Report cache-rs evictions through `internal_stats`

The design names this the most likely way to get a plausible wrong answer. `envelope_verdict` rejects a run whose main layer never evicted, and it reads crucible's `CacheInternalStats`. The default `internal_stats()` returns `None`, so without this the cache-rs arm skips the check entirely and a saturated run is reported as valid.

**Files:**
- Modify: `cache-bench/src/cachers.rs`

- [ ] **Step 1: Write the failing test**

Add inside `mod tests`:

```rust
    #[cfg(feature = "cache-rs")]
    #[test]
    fn evictions_are_reported_so_the_envelope_check_can_see_them() {
        use cache_core::Cache;
        // A cache far smaller than what is written to it must evict, and
        // must say so. Without this, `envelope_verdict` reads an all-zero
        // snapshot and passes a saturated run as valid -- the exact failure
        // the check exists to prevent.
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
        assert!(
            stats.evictions > 0,
            "no evictions reported after writing ~32 MB into an 8 MB cache; \
             envelope_verdict would pass this run"
        );
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cache-bench --features cache-rs evictions_are_reported`
Expected: FAIL with `cache-rs must report stats` (the default returns `None`)

- [ ] **Step 3: Write minimal implementation**

Add to the `impl Cache for CacheRs` block:

```rust
        fn internal_stats(&self) -> Option<cache_core::CacheInternalStats> {
            // cache-rs's `mod metrics` is private, so `SEGMENT_EVICT` cannot
            // be named by path. It is registered with metriken's global
            // registry by the `#[metric]` attribute, so it is read by name
            // instead -- the same pattern crucible's admin endpoint uses at
            // `server/src/admin/mod.rs:179`.
            //
            // Process-wide by construction: correct for a replay, which
            // builds one cache and runs one arm per process, and wrong for
            // any caller that built two cache-rs instances at once.
            let evictions = metriken::metrics()
                .iter()
                .find(|m| m.name() == "segment_evict")
                .and_then(|m| m.value())
                .and_then(|v| match v {
                    metriken::Value::Counter(c) => Some(c),
                    _ => None,
                })?;

            Some(cache_core::CacheInternalStats {
                evictions,
                demotions: 0,
                demotion_failures: 0,
                ..Default::default()
            })
        }
```

`metriken` must be a direct dependency of `cache-bench` for this; it already
is (`metriken = { workspace = true }`).

Returning `None` when the counter is absent is deliberate: cache-rs gates its
metrics behind a default-on `metrics` feature, and if that feature is ever
off the honest answer is "no stats", which `envelope_verdict` treats as
un-checkable. Substituting a hard-coded non-zero value would defeat the check
this exists to feed.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cache-bench --features cache-rs cachers::`
Expected: PASS, 8 passed

- [ ] **Step 5: Commit**

```bash
git add cache-bench/src/cachers.rs
git commit -m "feat(cache-bench): report cache-rs evictions through internal_stats

envelope_verdict rejects a run whose main layer never evicted, and reads
crucible's CacheInternalStats. The default returns None, so without this
the cache-rs arm skips the check and a saturated run reports as valid --
the single most likely way to get a plausible wrong answer here."
```

---

### Task 6: Config and constructor wiring

**Files:**
- Modify: `cache-bench/src/config.rs`
- Modify: `cache-bench/src/main.rs`

- [ ] **Step 1: Write the failing test**

Add to `cache-bench/src/config.rs` inside `mod tests`:

```rust
    #[test]
    fn a_cache_rs_backend_parses() {
        let toml = TRACE_TOML.replace("backend = \"segment\"", "backend = \"cachers\"");
        let config = match Config::from_toml(&toml) {
            Ok(c) => c,
            Err(e) => panic!("{e}"),
        };
        assert_eq!(config.cache.backend, CacheBackend::CacheRs);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cache-bench config::tests::a_cache_rs_backend_parses`
Expected: FAIL to compile, `no variant named CacheRs`

- [ ] **Step 3: Write minimal implementation**

In `cache-bench/src/config.rs`, extend the enum:

```rust
pub enum CacheBackend {
    Segment,
    Slab,
    Heap,
    /// pelikan-io/cache-rs, for engine-vs-engine ranking.
    #[serde(rename = "cachers")]
    CacheRs,
}
```

and its `Display`:

```rust
            CacheBackend::CacheRs => write!(f, "cachers"),
```

In `cache-bench/src/main.rs`, add the arm to `run()`:

```rust
        CacheBackend::CacheRs => {
            let cache = create_cachers(&config)?;
            run_with_cache(config, Arc::new(cache))
        }
```

and the constructor, beside `create_segment`:

```rust
#[cfg(feature = "cache-rs")]
fn create_cachers(config: &Config) -> Result<impl Cache, Box<dyn std::error::Error>> {
    let policy = cachers::cachers_policy(config.cache.policy).ok_or_else(|| {
        format!(
            "policy '{}' has no cache-rs counterpart",
            config.cache.policy
        )
    })?;

    let inner = cache_rs::Segcache::builder()
        .heap_size(config.cache.heap_size)
        .segment_size(config.cache.segment_size as i32)
        // Converted, not copied. See `cachers_hash_power`.
        .hash_power(cachers::cachers_hash_power(config.cache.hashtable_power))
        .eviction(policy)
        .build()?;

    Ok(cachers::CacheRs { inner })
}

#[cfg(not(feature = "cache-rs"))]
fn create_cachers(_config: &Config) -> Result<impl Cache, Box<dyn std::error::Error>> {
    // Rejected rather than ignored: a config naming a backend the binary
    // cannot provide must fail, not silently run a different engine.
    Err::<crate::cachers::Unavailable, _>(
        "this binary was built without the `cache-rs` feature; \
         rebuild with --features cache-rs"
            .into(),
    )
}
```

Add to `cache-bench/src/cachers.rs` so the `not(feature)` arm has a type:

```rust
/// Placeholder so the non-feature build has a concrete `impl Cache` type to
/// name in its error return. Never constructed.
#[cfg(not(feature = "cache-rs"))]
pub enum Unavailable {}

#[cfg(not(feature = "cache-rs"))]
impl cache_core::Cache for Unavailable {
    fn get(&self, _: &[u8]) -> Option<cache_core::OwnedGuard> {
        match *self {}
    }
    fn with_value<F, R>(&self, _: &[u8], _: F) -> Option<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        match *self {}
    }
    fn get_value_ref(&self, _: &[u8]) -> Option<cache_core::ValueRef> {
        match *self {}
    }
    fn set(
        &self,
        _: &[u8],
        _: &[u8],
        _: Option<std::time::Duration>,
    ) -> Result<(), cache_core::CacheError> {
        match *self {}
    }
    fn delete(&self, _: &[u8]) -> bool {
        match *self {}
    }
    fn contains(&self, _: &[u8]) -> bool {
        match *self {}
    }
    fn flush(&self) {
        match *self {}
    }
}
```

`CacheBackend` needs `PartialEq` for the test; add it to the derive if absent:

```rust
#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p cache-bench && cargo test -p cache-bench --features cache-rs`
Expected: both PASS. The non-feature build must still compile.

- [ ] **Step 5: Commit**

```bash
git add cache-bench/src/config.rs cache-bench/src/main.rs cache-bench/src/cachers.rs
git commit -m "feat(cache-bench): cachers backend, selectable from config

A config naming cachers in a binary built without the feature is rejected
rather than silently running a different engine."
```

---

### Task 7: CI rung for the new feature

Per the Rust conventions here, a feature nothing lints is a feature nothing compiles.

**Files:**
- Modify: the clippy job in `.github/workflows/` (locate with `grep -rl clippy .github/workflows/`)

- [ ] **Step 1: Add the feature to the clippy job**

After the existing `cargo clippy --all-targets -- -D warnings` step, add:

```yaml
      - name: Clippy (cache-rs feature)
        run: cargo clippy -p cache-bench --features cache-rs --all-targets -- -D warnings
```

- [ ] **Step 2: Verify locally**

Run: `cargo clippy -p cache-bench --features cache-rs --all-targets -- -D warnings`
Expected: no warnings, exit 0

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/
git commit -m "ci: lint the cache-rs feature

A feature combination nothing lints is one nothing compiles."
```

---

> **Note.** The SystemsLab specs this task refers to
> (`engine-leaderboard.toml` and the others) live in the private
> `crucible-experiments` repository, along with the analysis scripts and the
> trace-mount instructions. They name lab hosts and the trace corpus by
> service name, which is why they are not here. The measurements they
> produced are in `docs/superpowers/specs/`.

### Task 8: The leaderboard sweep

**Files:**
- Create: `docs/superpowers/plans/engine-leaderboard.toml` (SystemsLab spec; move to `brayniac/experiments` once it produces a result worth keeping)

- [ ] **Step 1: Write the spec**

Model it on the validated `merge-chain-latency.toml` from 2026-09-18. Reuse verbatim: the preflight mount check, the sha-manifest build step with `.cargo/env` sourcing and the `command -v cargo || FATAL` guard, the `set +e` around the arm so a rejected run is recorded rather than killing the step, and the `RESULT\t`-prefixed stdout rows.

Changes from that spec:

- Build with `cargo build --release -p cache-bench --features cache-rs`
- Inner loop is over `ENGINE` in `segment cachers` and `POLICY` in `s3fifo merge fifo cte random`, then `HEAP` in `32MB 48MB 64MB`
- Emit `engine` and `policy` as TSV columns alongside the existing ones
- Add a resident-items column, parsed from the run's own output

- [ ] **Step 2: Validate rendering without spending a host**

Run: `systemslab evaluate engine-leaderboard.toml`
Expected: JSON with every `{param}` substituted and no literal `{{`.

- [ ] **Step 3: Smoke it with two arms before the full sweep**

Run: `systemslab submit engine-leaderboard.toml -p host=<x86-host> -p engines="segment cachers" -p policies="merge" -p heaps="48MB" -p reps=1`
Expected: two `RESULT` rows, both `ok`, both with non-zero evictions.

**Do not trust a `success` state.** Read the log and confirm both rows are present with non-zero evictions — a job whose payload never ran exits 0.

- [ ] **Step 4: Run the full first cut**

Run: `systemslab submit engine-leaderboard.toml -p host=<x86-host> -p reps=4`
Expected: 5 policies x 2 engines x 3 heaps x 4 reps = 120 rows.

- [ ] **Step 5: Commit the spec and the results**

```bash
git add docs/superpowers/plans/engine-leaderboard.toml docs/superpowers/specs/2026-09-19-cache-rs-comparison-design.md
git commit -m "feat(experiments): engine+policy leaderboard sweep"
```

---

### Task 9: Report the leaderboard both ways

**Files:**
- Modify: `docs/superpowers/specs/2026-09-19-cache-rs-comparison-design.md`

- [ ] **Step 1: Add a results section**

Present two tables over the same rows — sorted by miss ratio, and sorted by write p99.9 — with columns: engine, policy, miss ratio, resident items, write p50/p99.9/max, evictions.

State a single order **only if both sorts agree**. If they disagree, say so explicitly; that disagreement is the most useful output, and yesterday's `min_segments` result showed it is the likely one.

Attach these caveats, which are already established and must not be dropped:

- A row is "this engine, this policy, at this budget, on this trace" — not "this algorithm". Resident items is the column that lets the algorithm-versus-header question be asked.
- Any point with `set_errors > 0` or zero evictions was rejected, not averaged in.
- The noise floor is zero for crucible across three machines; confirm it for cache-rs by checking that reps agree before quoting any gap.

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/specs/2026-09-19-cache-rs-comparison-design.md
git commit -m "docs: engine+policy leaderboard results"
```

---

## Self-review notes

**Spec coverage.** Every section of the design maps to a task: backend-not-separate-binary → Task 4; main-on-both → Task 2; `hash_power` conversion → Task 1; table pressure assertion → already in `envelope_verdict`, exercised in Task 8 step 3; resident items → Tasks 8 and 9; policy-matched pairs → Task 3; `get_value_ref` hazard → Task 4; `envelope_verdict` hazard → Task 5; first cut on the high-overwrite trace only → Task 8.

**Not covered, deliberately.** The crucible-ref parameter from the design is dead weight until a release contains the rig, and adding it now would be an unused knob. It goes in when the first such release exists.

**Risk in Task 5, checked and resolved before handing over.** `cache_rs::metrics::SEGMENT_EVICT` is *not* reachable — cache-rs declares `mod metrics;` privately. The counter is registered with metriken's global registry by its `#[metric]` attribute, so Task 5 reads it by name, mirroring `server/src/admin/mod.rs:179`. Had this not been caught, Task 5 would have failed to compile and the tempting fix would have been to stub the value, which defeats the check.

**Remaining risk.** cache-rs gates metrics behind a default-on `metrics` feature. If a future build turns it off, `internal_stats` returns `None` and `envelope_verdict` cannot check that arm. Task 8 step 3 catches this by requiring non-zero evictions on the smoke run before the full sweep.
