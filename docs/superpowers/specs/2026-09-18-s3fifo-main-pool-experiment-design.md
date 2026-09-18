# Experiment design: S3-FIFO main-pool eviction, merge vs CLOCK

**Date:** 2026-09-18
**Decides:** crucible#148 (deferred second-chance admission), and whether Layer 1's
merge eviction is the right main-pool algorithm under S3-FIFO admission
**Compares against:** pelikan-io/cache-rs `Policy::S3Fifo`

## Problem

Crucible and cache-rs both call their policy "S3-FIFO" and they do not run the
same algorithm below the admission queue.

- **cache-rs** main pool: CLOCK second chance. Claim the oldest main segment,
  copy every item with `freq > 0` into a fresh segment, drop the rest, no
  compaction of dead bytes
  (`crates/segcache/src/segments/segments.rs:2249`).
- **crucible** Layer 1: NSDI adaptive merge. Build a 256-bucket frequency
  histogram over a chain of segments, compute a cutoff that retains
  `target_ratio` of items, copy survivors into a compacted target
  (`cache/core/src/eviction/simple.rs:calculate_merge_threshold`, wired at
  `cache/segcache/src/lib.rs:664`).

These differ in what they optimize. CLOCK is O(one segment) per eviction and
reclaims only what the frequency test drops. Merge touches N segments, costs
more per eviction, and additionally reclaims dead bytes left by overwrites and
deletes. Which wins is a function of the workload's overwrite rate and its
frequency distribution, and nobody here has measured it.

The same replay answers #148, because a deferred-vs-immediate admission arm is
one more variable on the same rig.

## Why libCacheSim cannot answer this

libCacheSim models eviction at *item* granularity. Every mechanism under test
here is segment-granularity: the histogram cutoff spans a segment chain, dead-byte
compaction is a property of copying a segment, and TTL-bucket membership decides
which segments are even candidates. There is no libCacheSim configuration that
represents merge.

libCacheSim earns exactly one job in this design: a reference curve. The
oracleGeneral format carries a next-access-virtual-time field, so it yields exact
Belady miss ratio, plus item-level LRU and item-level S3-FIFO. Those bound how
much of the gap between the two segment designs is worth chasing. They are not
arms in the comparison.

## The confound problem

A crucible-S3FIFO vs cache-rs-S3FIFO run does not vary the main-pool algorithm.
At minimum it varies all of:

1. **Main-pool algorithm** — merge vs CLOCK. The variable we want.
2. **Admission routing** — cache-rs routes a ghost hit straight to main;
   crucible always writes Layer 0 and promotes one sweep later (#148).
3. **Effective capacity at a nominal heap size.** Item headers differ:
   cache-rs `RawItem` is 5 B; crucible is `TtlHeader` 10 B in Layer 0 and
   `BasicHeader` 6 B in Layer 1. On a trace of small objects that is a
   materially different item count behind the same "64 MB". Two arms counting
   a different denominator is not a comparison.
4. **Pool fungibility** — cache-rs draws both pools from one free queue;
   crucible's Layer 0 is a separate mmap that main can never borrow.
5. **Hash placement** — both seed `RandomState::new()` per process in release
   builds (`cache/core/src/hashtable_impl.rs:70`), so bucket occupancy and
   therefore ghost eviction differ run to run in *both*.
6. **TTL handling** on the promotion path — cache-rs links the target into the
   source's own TTL bucket; crucible recomputes per-item remaining TTL and
   re-inserts.

Running the two binaries against one trace and reading off two miss ratios
produces a precise number about the sum of six differences.

**Therefore the primary comparison is within crucible**, where five of the six
can be held fixed. That requires implementing `EvictionStrategy::Clock` in
`TtlLayer` — crucible has no second-chance-copy strategy today (the strategy set
is `ExpireFirst | Merge | Fifo | Random | Cte`, `cache/core/src/config.rs:178`).
That is the main build cost of this experiment and it is not optional; without it
there is no single-variable arm.

cache-rs stays in as a cross-check arm. If within-crucible merge-vs-CLOCK and
cross-codebase crucible-vs-cache-rs disagree in sign, something in the other five
variables is larger than the one under test, and that is itself the finding.

## Arms

Trace and cache size fixed within a sweep point; one variable per pair.

| Arm | Layer 0 | Layer 1 | Varies vs baseline |
|---|---|---|---|
| `A-merge` (baseline) | FIFO admission, defer-promote | Merge (default config) | — |
| `A-clock` | FIFO admission, defer-promote | **CLOCK** | main-pool algorithm |
| `A-merge-route` | FIFO admission, **promote-on-insert** | Merge | #148 routing |
| `A-clock-route` | FIFO admission, promote-on-insert | CLOCK | both (interaction check) |
| `X-cachers` | cache-rs `Policy::S3Fifo { admission_ratio: 0.10 }` | | cross-check, confounded |
| `R-belady` | libCacheSim Belady | | reference floor |
| `R-lru`, `R-s3fifo-item` | libCacheSim item-level | | reference |

`A-merge-route` is the #148 arm. Implement it as option 2 from that issue —
route on the frequency the ghost takeover restored — not as a port of cache-rs's
hash ghost queue.

## Traces

Two formats are supported by `cache-bench/src/trace.rs`, and they are not
interchangeable for this question.

- **Twitter cluster native** (20-byte records: timestamp, key id, key/value
  lengths, op code, 24-bit TTL). Carries the real GET/SET/DELETE mix and real
  TTLs. **This is the primary format.** Merge's advantage over CLOCK is dead-byte
  reclamation, and dead bytes come from overwrites and deletes. TTL bucketing
  decides eviction candidacy.
- **oracleGeneral** (24-byte: timestamp, object id, size, next-access virtual
  time). GET-with-insert-on-miss, no TTL, no overwrites. Use it **only** for the
  Belady and item-level reference curves.

> **The existing runs in `s3-replay/cluster*.log` measured nothing.** They ran
> oracleGeneral mode — no writes, no TTL — which disables both mechanisms under
> test, at 64 MB against a working set small enough to report 99.99% hit and
> `promoted: 0` for the first 240M operations. Do not use them as a baseline or
> as evidence that the rig works.

Traces live on delta's NFS export, mounted read-only:

    sudo mount -t nfs -o ro,resvport delta:/mnt/cachetrace /Volumes/cachetrace

`twoday/` and `bin/` and `nsdi/` hold `.sbin.zst` — the native 20-byte layout
with op codes and TTLs. `oracle/` holds the oracleGeneral form of the same
clusters, for the reference curves only.

## Measurement envelope

Establish this before producing any comparison number.

**Characterize each trace first**, and write the numbers down: unique keys,
total and unique byte footprint, key and value size distributions, GET/SET/DELETE
split, overwrite rate per key, TTL distribution. A trace with a negligible
overwrite rate cannot distinguish merge from CLOCK on the mechanism that
separates them, and should be excluded rather than run and reported as "no
difference".

**Then sweep cache size against the compulsory miss floor, not against an
absolute hit ratio.** An earlier draft of this document said to target the
80-95% hit band. That heuristic is wrong and the first real sweep showed why.

Sweep upward until the miss-ratio curve flattens. The value it flattens to is
the **compulsory miss floor** for that window: every object's first access
misses no matter how large the cache, so no policy can do better. On
`timelines_real_time_aggregates` over a 10M-record window the floor is 0.1834,
which caps hit ratio at 81.66% — a cache sized for "80% hit" is sitting at
1.04x the floor with almost no eviction pressure, and would have been read as
a healthy operating point.

Express the target as a multiple of the floor. The discriminating region is
roughly **1.3x to 2x**; below that the cache is barely evicting, above it the
cache is thrashing and the compulsory misses stop dominating. Measured:

    heap     miss ratio   x floor   evictions   verdict
      8 MB     1.0000      5.45x        0       rejected (see cliff below)
     16 MB     1.0000      5.45x        0       rejected
     24 MB     1.0000      5.45x        0       rejected
     32 MB     0.3061      1.67x      196       ok
     48 MB     0.2810      1.53x      188       ok
     64 MB     0.2653      1.45x      182       ok
     96 MB     0.2116      1.15x       82       marginal
    128 MB     0.1906      1.04x       40       marginal
    256 MB     0.1834      1.00x        0       rejected (saturated)

**A minimum cache size is imposed by a separate bug.** S3-FIFO wedges when
layer 0 holds fewer than 3 segments — 100% miss at one segment, 99.46% at two.
At `small_queue_percent = 10` and 1 MB segments that is any heap below ~30 MB,
which is why the three smallest points above return a 1.0 miss ratio rather
than a large one. Until that is fixed, the bottom of every sweep is unusable
and the envelope check is what stops it being reported.

**Reject any sweep point whose main layer never evicted.** `envelope_verdict`
enforces this. Note it tests `evictions`, not `evictions || demotions`: layer
0 keeps promoting into layer 1 above the working set, so the weaker test
accepts a saturated point where no main-pool eviction decision was ever made —
which is the whole quantity this experiment reads. That false negative was
live until the 256 MB point above exposed it.

## Trace selection

Screened the 18 largest `twoday` traces over a 600k-record prefix. Prefix
length understates both reuse and overwrite rate, so these rank traces rather
than characterise them.

Selected, on write mix and overwrite rate — merge's advantage over CLOCK is
dead-byte reclamation, and dead bytes come from overwrites and deletes:

| trace | writes | overwrites | reuse | mean value | TTLs |
|---|---|---|---|---|---|
| `timelines_real_time_aggregates` | 10.7% | **69.8%** | 9.8 | 359 B | 2 |
| `simclusters_v2_entity_cluster_scores` | **49.7%** | 32.9% | 3.0 | 2087 B | 2 |
| `botmaker_2` | 42.2% | 16.7% | 2.8 | 89 B | 16 |
| `pinkfloyd` | 12.5% | 24.1% | 3.6 | 575 B | 6 |

`timelines_real_time_aggregates` is the primary: the highest overwrite rate in
the corpus with enough reuse to have a working set, and a small enough mean
value to fit a laptop-scale sweep. `simclusters` is the opposite shape — half
writes with 2 KB values — and needs a multi-GB sweep to reach its band.
`botmaker_2` is the only candidate with real TTL diversity.

**Negative control: `wtf_req_cache`** — 4.9% writes and a **0.0%** overwrite
rate. Merge and CLOCK should be indistinguishable there, because the mechanism
that separates them never fires. If an arm pair differs on this trace, the rig
is wrong rather than the policies.

Excluded for no overwrite rate, and so unable to discriminate:
`timelines_content_features` (0.1% writes), `conversation_timeline_metadata`
(1.0%), `pushservice_core_svcs` (1.2%), `content_recommender` (2.5%
overwrites), `onboarding_task_service` (1.2% overwrites, and a 1-byte mean
value). Also excluded: `ibis_cache`, which is 99.8% `add` with zero-length
values.

## Steady state

The existing logs report cumulative hit ratio from a cold cache, which rises
monotonically and converges to something that occurs nowhere in the run.

- Split each trace into a **warmup prefix** and a **measured window**. Warm until
  the cache is full *and* the eviction rate has flattened, not merely until the
  cache is full — merge's steady state includes a compaction rhythm that takes
  longer to settle than first-fill.
- Report the miss ratio **of the measured window only**, computed from counter
  deltas across the window.
- Confirm flatness: emit per-interval miss ratio across the measured window and
  check the last third has no trend. If it is still moving, the window was short.
  A drifting window biases the sweep's *shape*, and the drift is worst at the
  small-cache end where the policy question lives.

## Determinism and the noise floor

**Measured, not assumed: the noise floor is zero at correct hashtable sizing.**

Neither engine seeds its hashtable deterministically in release builds, so the
concern was real. `MultiChoiceHashtable::with_seeds` was added to size it.
Result on `timelines_real_time_aggregates`, 48 MB, 15M-record window:

    hash_power   set errors   seeds 1-4 miss ratio              spread
       10          381,722    0.4151 0.4252 0.4183 0.4165       0.0101
       12           57,271    0.2948 0.2959 0.2954 0.2961       0.0013
       14+               0    0.2810 0.2810 0.2810 0.2810       0.0000

Hash placement matters only while the table is oversubscribed enough to refuse
insertions. Once `set_errors` reaches zero the single-threaded replay is
**exactly reproducible**, and ten seeds agree to four decimal places.

Two consequences:

1. **No seed averaging is needed for the policy arms.** At a correctly sized
   table any measured merge-vs-CLOCK difference is real by construction. This
   is a large simplification and it is why the knob was worth building even
   though the answer turned out to be zero — the floor cannot be known to be
   zero without the ability to vary the seed.
2. **`set_errors > 0` is a contamination mode, now a hard reject.** At
   `hash_power` 12 table pressure alone moved the miss ratio 0.2810 -> 0.2958.
   That is the same magnitude as the effects being looked for and would have
   been read as a policy difference.

Scope of the claim: single-threaded replay, one trace. Determinism does not
survive multi-threading, which reorders how evictions interleave with reads —
another reason the hit-ratio arms stay single-threaded. On a new trace, verify
`set_errors == 0` before assuming reproducibility rather than inheriting it.

## Comparability of the denominator

Because header sizes differ per arm, "64 MB" is not one operating point.

- Report miss ratio against **achieved resident item count** and **achieved
  resident bytes** as well as configured heap size.
- Record per-arm metadata overhead directly: bytes of header per resident item,
  from the counters, not from the struct definitions.
- If arms differ by more than a few percent in items-resident at the same heap
  size, add a capacity-matched sweep point so at least one comparison holds item
  count fixed instead of bytes.

This matters most for `X-cachers` (5 B vs 6/10 B headers). The within-crucible
arms share Layer 0/Layer 1 formats and should be nearly matched — verify that
rather than assuming it.

## Falsification checks

Decide before the run which line proves the work happened, then grep for it.

- `A-clock` must report a nonzero CLOCK-copy counter. A CLOCK arm that silently
  fell back to merge, or to `Random`, produces a clean curve and a wrong
  conclusion.
- `A-merge-route` must report a nonzero promote-on-insert counter. If the ghost
  takeover never restores a frequency ≥ threshold on this trace, the arm is
  inert and its agreement with baseline is not evidence about #148.
- Every arm must report the eviction and demotion counters nonzero in the
  measured window (see envelope).
- The trace reader must report records-consumed matching the file's record count.
  A truncated `.zst` read fails by succeeding.
- Cross-check one sweep point's miss ratio derived two ways — from counter deltas
  and from a hit/miss tally in the replay loop. Two of our own numbers
  disagreeing is a bug to find, not a rounding note.

## Decision rules

Write these down before seeing results.

- **#148 closes as wontfix** if `A-merge-route` minus `A-merge` is inside the
  noise floor across the working band on every trace with a meaningful
  resurrection rate. It closes as a fix if the routing arm wins by more than the
  floor at two or more sweep points.
- **Merge stays** unless `A-clock` beats `A-merge` by more than the floor across
  the band on traces that have a real overwrite rate. If CLOCK wins only on
  low-overwrite traces, that is a config recommendation, not a default change.
- **Convergence is not equivalence.** If two arms land within a few percent,
  name the constraint both were against and say whether either had headroom. At
  cache sizes near the working set every policy converges by construction, and
  that agreement carries no information.
- A cross-codebase disagreement in sign between `X-cachers` and the within-crucible
  arms is a finding to investigate, not a tiebreak to average.

## Build order

1. Characterize traces with the existing `s3-replay` summary; record the numbers.
   Drop traces with no overwrite rate. *(no code)*
2. Trace source in `cache-bench` — lift the reader from `s3-replay/src/main.rs`,
   both formats, behind a `[workload] source = "trace"` config arm.
   `cache-bench` already links segcache, slab, and heap.
3. Seed knobs (hashtable + eviction RNG) and the falsification counters.
4. Noise floor run: baseline arm, one sweep point, ≥10 seeds.
5. `EvictionStrategy::Clock` in `TtlLayer`.
6. `A-merge-route` (crucible#148 option 2).
7. cache-rs as a `cache-bench` arm — its API is `&self` and `Arc`-shareable, so
   it drops in as another backend.
8. libCacheSim reference curves from the oracleGeneral traces.

Steps 1–4 are worth doing even if the rest is deferred: they are what turns the
existing replay rig into one whose numbers can be quoted.

## Open questions

- Does `MergeConfig::default()` need tuning before it is a fair baseline? A
  default-configured merge losing to CLOCK may be a statement about the defaults
  rather than about merge. Consider a small `target_ratio` sweep on the winning
  trace before concluding.
- Should the disk tier be in scope? It changes what "evicted from main" means
  (demote rather than discard) and plausibly changes the merge-vs-CLOCK answer.
  Recommend: out of scope for round one, explicitly named as untested.
