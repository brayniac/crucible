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
can be held fixed. This was expected to cost a separate `EvictionStrategy::Clock`
implementation. It did not: CLOCK is `Merge` with the chain pinned to one segment
and the threshold pinned, reached through `EvictionStrategy::merge_params`. Both
arms then execute the *same* claim protocol, item scan, copy and relink, differing
in two constants — a stronger single-variable comparison than two independently
written code paths could be.

**The prune threshold is not zero, and this is the subtlety that nearly voided
the arm.** A fresh insert is packed with frequency 1 and nothing ever lowers it:
`apply_frequency_decay` is unit-tested and called from no production path, and
`LayerConfig::frequency_decay` is stored and never read (crucible#150). Zero is
therefore unreachable for a live item, so a `freq > 0` rule retains everything
still indexed and reclaims only dead bytes — compaction, not second chance.

cache-rs has the identical property for the same two reasons:
`Hashbucket::pack(tag, 1, location)` on insert, and no decay function anywhere
despite `docs/s3fifo.md` asserting that "the hash table's frequency smoothing
handles decay over time". So its `s3fifo_evict_main` sweep also keeps every live
item.

CLOCK therefore prunes at `freq > 1` — touched since admission, not merely
present — via `MergeConfig::initial_threshold`. That is the faithful translation
of the published rule into a convention whose baseline is 1 rather than 0.
Merge keeps `initial_threshold: 0` because its threshold climbs past the
baseline on its own, which is the only reason it prunes live items at all.

cache-rs stays in as a cross-check arm. If within-crucible merge-vs-CLOCK and
cross-codebase crucible-vs-cache-rs disagree in sign, something in the other five
variables is larger than the one under test, and that is itself the finding.

## Arms

Trace and cache size fixed within a sweep point; one variable per pair.

| Arm | Layer 0 | Layer 1 | Varies vs baseline |
|---|---|---|---|
| `A-merge` (baseline) | FIFO admission, defer-promote | Merge (default config) | — |
| `A-clock` | FIFO admission, defer-promote | **CLOCK** (`min_segments: 1`, threshold pinned at 1) | main-pool algorithm |
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

Traces live on an NFS export mounted read-only on the measurement
hosts. Mount instructions and the spec definitions live in the
private `crucible-experiments` repository.

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
the high-overwrite trace over a 10M-record window the floor is 0.1834,
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
| high-overwrite | 10.7% | **69.8%** | 9.8 | 359 B | 2 |
| a mid-overwrite trace_v2_entity_cluster_scores` | **49.7%** | 32.9% | 3.0 | 2087 B | 2 |
| mid-overwrite 2 | 42.2% | 16.7% | 2.8 | 89 B | 16 |
| `pinkfloyd` | 12.5% | 24.1% | 3.6 | 575 B | 6 |

the high-overwrite trace is the primary: the highest overwrite rate in
the corpus with enough reuse to have a working set, and a small enough mean
value to fit a laptop-scale sweep. a mid-overwrite trace is the opposite shape — half
writes with 2 KB values — and needs a multi-GB sweep to reach its band.
a second mid-overwrite trace is the only candidate with real TTL diversity.

**Negative control: the low-overwrite control_cache`** — 4.9% writes and a **0.0%** overwrite
rate. Merge and CLOCK should be indistinguishable there, because the mechanism
that separates them never fires. If an arm pair differs on this trace, the rig
is wrong rather than the policies.

Excluded for no overwrite rate, and so unable to discriminate:
the high-overwrite trace_content_features` (0.1% writes), `conversation_timeline_metadata`
(1.0%), `pushservice_core_svcs` (1.2%), `content_recommender` (2.5%
overwrites), `onboarding_task_service` (1.2% overwrites, and a 1-byte mean
value). Also excluded: `ibis_cache`, which is 99.8% `add` with zero-length
values.


## First results (2026-09-18)

Single-threaded replay, 15M-record window (5M warmup / 10M measured), hash
seed pinned, `set_errors == 0`, noise floor 0.0000. Every point below passed
`envelope_verdict`; saturated points are marked where they were rejected.

### Merge beats CLOCK, and the margin tracks the overwrite rate

    the high-overwrite trace (69.8% overwrite)
      heap     merge     clock     gap
      32 MB    0.3061    0.3861    0.0800
      48 MB    0.2810    0.3662    0.0852
      64 MB    0.2653    0.3534    0.0881

    the low-overwrite control_cache (0.0% overwrite, negative control)
      64 MB    0.8126    0.8158    0.0032
     128 MB    rejected (saturated, 0 evictions)
     256 MB    rejected (saturated, 0 evictions)

The gap collapses 26x when there are no dead bytes to reclaim, which is the
mechanism the design predicted rather than merely the effect. The control
earned its place: without it this would read as "merge is better", full stop.

### But the cause is not the one the comparison was built around

Merge and CLOCK differ in two things, not one: the prune threshold *and* the
chain length (`min_segments` 4 vs 1). A third arm holds the chain at one
segment and varies only the threshold:

    the high-overwrite trace, 48 MB
      arm            chain   threshold    miss ratio   evictions
      merge            4     adaptive       0.2810        188
      merge-single     1     adaptive       0.3662        585
      clock            1     pinned at 1    0.3662        585

`merge-single` and `clock` are identical to four decimals with identical
eviction counts. **The whole 0.0852 is chain length. The pruning rule
contributes nothing here.**

The threshold is not an inert knob -- on a low-reuse trace, where items
actually sit at frequency 1, it moves the result:

    the low-overwrite control_cache (reuse 1.7), 64 MB
      merge-single   0.8146
      clock          0.8164

0.0018, against 0.0852 for chain length. Roughly a 47x difference in
leverage.

### What this means

The headline is not "merge's adaptive threshold beats CLOCK's second
chance". It is **"compacting four segments per pass beats compacting one"** —
a four-segment chain folds four partly-dead segments into roughly one, while
a one-segment chain can only reclaim the dead bytes inside a single segment.
cache-rs's main pool is disadvantaged by reclaiming one segment at a time,
not by its frequency rule.

That makes `min_segments` the lever worth tuning, and it is already a
configuration knob rather than a code change. It also reopens the design's
own question about `MergeConfig::default()`: if chain length carries the
effect, `min_segments: 4` is the parameter that should be swept before
either policy is called better.

Not yet measured: whether the chain-length advantage keeps scaling past 4,
where it costs more in eviction latency than it returns in hit ratio, and
whether any of this survives on a mid-overwrite trace (49.7% writes, 2 KB values)
or a second mid-overwrite trace (the only candidate with real TTL diversity).


### The chain-length sweep

`min_segments` is the variable, so it was swept. the high-overwrite trace:

    heap    chain=1   2        4        8        16       32
    48 MB   0.3662   0.2831   0.2810   0.2788   0.2798   0.3008
    64 MB   0.3534   0.2661   0.2653   0.2636   0.2608   0.2738
    96 MB       --       --       --   0.2104   0.2095   0.2168

Three things fall out.

**Almost all of the benefit is the first step.** 1 -> 2 is 0.083 of the
0.085 total gap at 48 MB. Everything from 2 to 16 is worth another 0.004.
The lever is not "longer chains are better", it is "do not use a chain of
one" -- which is exactly where cache-rs's main pool sits.

**Total scan work is flat.** Segments scanned per run (evictions x chain)
stays in 704-756 at 48 MB across chains 2-32, while eviction passes fall
from 378 to 22. A longer chain does not do more work; it batches the same
work into fewer, larger passes. The cost is per-pass latency, not
throughput, which this rig does not measure.

**There is an optimum and it degrades past it** -- 8 at 48 MB, 16 at 64 MB
and 96 MB, worse at 32 everywhere. The default of 4 is within 1-2% of the
best on both sizes.

Two mechanisms are candidates for the right-hand degradation and they have
not been separated:

1. *Candidate starvation.* `try_merge_eviction` falls back to whole-segment
   eviction when a bucket holds fewer candidates than `min_segments`. This
   trace has two distinct TTLs, so its segments concentrate in ~2 buckets and
   long chains outrun them. Predicted that raising the heap to 96 MB (~43
   segments per bucket) would rescue chain 32; it did not (0.2168 against
   0.2095 at 16), so this is at most a partial explanation.
2. *TTL truncation, and this one is a defect.* The compacted target takes
   `min(candidate.expire_at)` (`ttl_layer.rs:1107`). Segments in one bucket
   share a TTL but differ in creation time, so merging N of them truncates
   every item's remaining lifetime to that of the oldest candidate. The
   longer the chain, the wider the creation spread thrown away. Filed
   separately; it is a correctness-adjacent issue in its own right, not an
   artifact of this experiment.

Verified by reading for (2); not isolated experimentally, because both
mechanisms strengthen with chain length and the rig has no counter that
separates an expiry from an eviction.


### Replication across the trace set

The chain sweep was repeated on the other two selected traces. Neither
saturates within a practical heap, so the comparison sits in the
eviction-active region rather than at a fixed multiple of a compulsory floor;
zero-eviction points were rejected by `envelope_verdict`.

    trace / heap                chain=1  2       4       8       16      clock
    the high-overwrite trace_rta   / 48 MB     0.3662  0.2831  0.2810  0.2788  0.2798  0.3662
    the high-overwrite trace_rta   / 64 MB     0.3534  0.2661  0.2653  0.2636  0.2608  0.3534
    a second mid-overwrite trace      / 64 MB     0.6665  0.6228  0.5966  0.5883  0.6067  0.6696
    a second mid-overwrite trace      /128 MB     0.5634  0.5485  0.5460  0.5383  0.5384  0.5635
    a mid-overwrite trace     /512 MB     0.6732  0.6038  0.5713  0.5652  0.5614  0.6734
    a mid-overwrite trace     /  1 GB     0.4504  0.4354  0.4334  0.4327  0.4327  0.4506

**CLOCK equals a chain of one, on every trace.** 0.3662/0.3662,
0.6665/0.6696, 0.6732/0.6734, 0.5634/0.5635, 0.4504/0.4506. Three traces
spanning 16.7% to 69.8% overwrite and 89 B to 2 KB values, and the prune
threshold never separates from chain length. The decomposition result was
not a property of one trace.

**The chain-of-one penalty is large and universal**: 0.088, 0.078 and 0.112
absolute on the three real traces, against 0.0032 on the zero-overwrite
control. It is the single biggest effect this experiment found.

**Chain 8 beat chain 4 at every measured point**, by 0.0007 to 0.0083:

    the high-overwrite trace 48 MB   0.2810 -> 0.2788
    the high-overwrite trace 64 MB   0.2653 -> 0.2636
    mid-overwrite  64 MB   0.5966 -> 0.5883
    mid-overwrite 128 MB   0.5460 -> 0.5383
    a mid-overwrite trace 512MB 0.5713 -> 0.5652
    a mid-overwrite trace  1 GB 0.4334 -> 0.4327

Six of six, noise floor 0.0000. That is a case for changing
`MergeConfig::default().min_segments` from 4 to 8, subject to the per-pass
latency this rig does not measure: a chain of 8 does the same total scan
work in half as many passes, so each pass is twice as long.

**The degradation past the optimum is not universal.** It appears at
high-overwrite/32, mid-overwrite/64 MB/16 — and not at all on a mid-overwrite trace up to
chain 16, where layer 1 holds ~230 segments per TTL bucket. That is
consistent with both candidate mechanisms in crucible#151 (starvation and
TTL truncation) scaling with chain length *relative to bucket depth*, and it
still does not separate them. Chain 8 never degraded anywhere, which is the
practical reason to prefer it over 16.


### Eviction latency: the axis the rig could not see

Everything above is hit ratio. Merge eviction runs *inline* in the write path
-- `TieredCache::set` calls `ensure_space` as its first statement, on five
write entry points -- so a reclamation pass is a stall on whichever `set`
triggered it, and no arm had ever measured that.

Measured on board A (Raspberry Pi 4B, Cortex-A72, 8 GB, Debian 13, bare metal via
SystemsLab), the high-overwrite trace at 48 MB, 15M-record window,
4 reps per chain **interleaved on one host** -- not a matrix, because Pi
thermal drift is the dominant noise term and only same-host interleaving
controls for it.

    chain  evicts  p50    p99    p99.9  | p99.99 mean  | max mean  max min  max max
      1      585   1.62   2.80   8.70   |     2195     |   6742      3211     8978
      2      378   1.62   2.77   8.82   |     2761     |   6562      4456     9896
      4      188   1.62   2.85   9.12   |     2224     |   6595      5603     8520
      8       92   1.62   2.80   8.72   |    10519*    |  10519      8651    14680
     16       45   1.62   2.80   8.90   |     1585     |  13861     11338    16908

(microseconds; * see the p99.99 warning below)

**The common path is untouched.** p50 is 1.62 us at every chain length, p99
within 2.77-2.85, p99.9 within 8.70-9.12. Chain length costs nothing until
four nines out, which is exactly why a hit-ratio-only rig never saw it.

**No thermal drift to correct for.** p50 held at 1.62 us across all 20
interleaved runs over nine minutes. The control was still necessary to know
that.

**Stalls are milliseconds.** 3.2 to 16.9 ms against a 1.62 us median -- four
orders of magnitude, inline in `set()`.

> **Correction.** This section first read "chains 1, 2 and 4 are
> indistinguishable at ~6.6 ms with heavily overlapping ranges". That was a
> single-host artifact and it is wrong. board A's chain-1 cell is the outlier in
> the whole set, and board A is the only host of three where the series is not
> monotonic in chain length. Two independent replications put chain 1 at
> ~0.60x of chain 4. See the cross-host table below; the claim survived
> exactly as long as it took to run a second host.

**p99.99 is a trap here and must not be quoted alone.** It reads
2195 / 2761 / 2224 / 1622 / 1585 -- non-monotonic, with chain 2 worst and the
long chains apparently best, the opposite of the max story. The measured
window holds ~1.078M writes, so p99.99 sits at roughly the 108th largest
sample. Once the eviction count falls below that (chain 8 = 92, chain 16 = 45)
the percentile falls off the stall population entirely and is measuring
ordinary writes. Chains 1/2/4 have 585/378/188 stalls and therefore sample
*different percentiles within their own stall distributions*, which is why
they cross. **`max` tracks stall duration; p99.99 tracks stall frequency
against the percentile's sample budget.** Neither alone describes the system.

**Determinism held across architectures.** Every miss ratio is identical
across all four reps and identical to the x86 figures earlier in this
document, to four decimals, on aarch64 with NEON rather than AVX2 and a
different compiler profile. The zero noise floor is structural, not an
artifact of one machine -- which also means any host-to-host difference in the
latency columns cannot be hiding a workload difference.


### Replication: a second board and an x86 host

The same sweep, same interleaved design, on a second Pi and on an x86 host
(bare-metal Zen 4, x86-64) -- the closest thing in this lab to a deployment
target. Mean of the per-rep `max`, in microseconds:

    chain | board A board B   x86 | as a multiple of that host's chain 4
      1   | 6742   3790   3408  | 1.02   0.60   0.61
      2   | 6562   6270   4272  | 1.00   1.00   0.76
      4   | 6595   6291   5628  | 1.00   1.00   1.00
      8   |10519   9683   7250  | 1.60   1.54   1.29
     16   |13861  13517  11321  | 2.10   2.15   2.01

    monotonic in chain length?   board A NO  board B yes   x86 yes

(board B's chain-1 and chain-2 cells are n=3 rather than n=4; the three chain-1
values span 3637-3932, so the mean is not carrying the disagreement.)

**What replicates:** the chain-8 and chain-16 penalties, at 1.60/1.54/1.29 and
2.10/2.15/2.01 across three machines and two architectures. Longer chains cost
worst-case stall, reliably.

**What did not:** board A's flat chain-1/2/4 region. Both other hosts make chain 1
about 40% better than chain 4 on worst-case stall, and both are monotonic.

**The cliff is relatively worse on better hardware, which is the opposite of
reassuring.** the x86 host's common path is 5x faster than a Pi's (p50 0.30 us
against 1.62), but its worst-case stall improved only 2x. Expressed against
each host's own median write:

    board A  chain-4 max =  4,071 x p50
    board B  chain-4 max =  3,860 x p50
    x86      chain-4 max = 18,760 x p50

On server-class hardware a single `set()` blocks for 5.6 ms while the median
write costs 300 ns. That is the shipping default, not a tuned extreme, and
nothing in the cache measures or bounds it. Filed as crucible#152.

**Revised conclusion on `min_segments`.** 4 still stands, but not for the
reason the single-board data gave. It is not free: chain 4 costs roughly 65% more
worst-case stall than chain 1, and buys 23% fewer misses (0.3662 -> 0.2810)
for it. That is a defensible trade. It is a trade.

### What this settles

**`min_segments: 4` is the knee and should stay** -- see the revised wording
above. It captures essentially all the hit-ratio win (0.3662 -> 0.2810), at a
cost of roughly 65% more worst-case stall than a chain of one. Moving to 8 buys 0.8% hit ratio
(0.2810 -> 0.2788) for a 60% worse worst-case stall (6.6 -> 10.5 ms). On a
cache whose stated premise is latency predictability that is the wrong trade.

This reverses the recommendation made earlier in this document from the
hit-ratio sweep alone, where chain 8 beat chain 4 at all six measured points.
That result was not wrong; it was measured on the only axis the rig had. The
lesson is the one the design opened with -- a comparison that changes one
variable can still be read on the wrong axis, and "six of six" is not a
defence against that.

Caveats: Pi 4B, so absolute milliseconds do not transfer even though the shape
does; one trace, one cache size; and `max` is a single-sample statistic, which
is why min and max across four reps are reported rather than a mean with a
confident sign.

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
Result on the high-overwrite trace, 48 MB, 15M-record window:

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
