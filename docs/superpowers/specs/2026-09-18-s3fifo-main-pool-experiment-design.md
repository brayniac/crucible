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

Two formats exist in `s3-replay/src/main.rs`, and they are not
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

Traces live on `/Volumes/cachetrace/oracle/`, currently unmounted. Confirm mount
and checksum before any run.

## Measurement envelope

Establish this before producing any comparison number.

**Characterize each trace first**, and write the numbers down: unique keys,
total and unique byte footprint, key and value size distributions, GET/SET/DELETE
split, overwrite rate per key, TTL distribution. `s3-replay`'s
`print_summary` already computes most of these. A trace with a negligible
overwrite rate cannot distinguish merge from CLOCK on the mechanism that
separates them, and should be excluded rather than run and reported as "no
difference".

**Then sweep cache size to find the working band.** The comparison is only
meaningful where the policy is actually deciding, which is roughly the 80–95%
hit-ratio band. Sweep in powers of two until the band is bracketed, then take
at least four points inside it. Report the whole curve, not a single size —
a policy can win at one capacity and lose at another, and a single point hides
that.

**Reject any sweep point above the working set.** If an arm reports
`demotions == 0` or `evictions == 0` for the measured window, that point
measured allocation, not eviction. Fail the point loudly rather than averaging
it in.

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

Neither implementation is deterministic across processes: both build their
hashtable with `RandomState::new()` in release builds, and `Random` eviction
draws from an unseeded RNG.

1. **Add a seed knob.** `MultiChoiceHashtable::with_seeds(power, choices, seeds)`,
   plumbed to a CLI flag. Same treatment for the eviction RNG. Fixed seed makes a
   run reproducible.
2. **Measure the floor before quoting any delta.** Run the baseline arm at a
   fixed sweep point across at least 10 seeds. The spread of that set is the
   noise floor. A merge-vs-CLOCK difference smaller than it is not a result.
3. **Replay single-threaded for all hit-ratio arms.** Concurrency changes
   eviction interleaving, which changes which items are resident, which changes
   the number we are trying to read. Throughput is a separate experiment with a
   separate rig; do not read hit ratio off a multi-threaded run.

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
