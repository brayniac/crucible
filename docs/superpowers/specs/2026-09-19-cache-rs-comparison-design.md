# Design: comparing hit ratio against cache-rs

**Date:** 2026-09-19
**Builds on:** `2026-09-18-s3fifo-main-pool-experiment-design.md`, which specified
this as the `X-cachers` cross-check arm and never ran it
**Subject:** crucible `main` vs pelikan-io/cache-rs `main`

## The question: rank engine+policy pairs at a fixed budget

The deliverable is a leaderboard. Each row is an **engine + policy pair** --
`cache-rs/merge`, `crucible/merge`, `cache-rs/s3fifo`, `crucible/s3fifo` and
so on -- measured at a fixed memory budget on a fixed workload, sortable by
hit ratio and, separately, by write-tail latency.

The expected shape of the answer: *at 48 MB on this trace, sorted by hit rate
the order is A, B, C; sorted by p99.9 write latency it is C, A, B.* Today's
chain-length work already demonstrated those two orders can disagree --
`min_segments: 4` lost to 8 on hit ratio and beat it on worst-case stall --
so a single ranking would hide the thing most worth knowing.

### Why the "confounds" are mostly not confounds here

An earlier draft of this document declared "which engine is better" out of
scope, on the grounds that the two engines differ in six ways beyond the
policy. That was over-cautious, and it would have produced a result nobody
could act on.

The distinction that matters is between two kinds of difference:

**Differences the engine owns.** Item header size (cache-rs `RawItem` is 5 B;
crucible is 6 B or 10 B), admission routing, TTL handling on promotion, pool
fungibility. An operator cannot separate these from the engine -- deploying
cache-rs in 48 MB *means* getting its header overhead and the extra items
that buys. These are properties being ranked, not noise to remove. Holding
them constant would measure an abstraction that nobody can deploy.

**Differences the harness would introduce.** `hash_power` meaning slots on
one side and buckets on the other, unequal warmup, different traces or
windows. These stay rigorously controlled, because they are configuration
mistakes on our side rather than facts about either engine.

So: fix the memory budget and the workload, let each engine be itself, and
rank the pairs.

### What a row still cannot claim

A row is "this engine, this policy, at this budget, on this trace". It is not
"this algorithm". If `cache-rs/merge` beats `crucible/merge`, that does not
establish that cache-rs's merge *implementation* is better -- it may be
winning on header overhead. The resident-item count is reported alongside
precisely so that question can be asked afterwards, rather than assumed
either way.

## Shape: cache-rs as a backend inside cache-bench

`cache-bench` gains a `CacheBackend::CacheRs`, implementing crucible's `Cache`
trait over cache-rs's `Segcache`.

Rejected: a separate replay binary linking cache-rs. It avoids a cross-org
dependency, but it makes *the harness itself* a difference between the arms —
in a comparison that already varies six things — and it would duplicate the
falsification checks, which is where most of the rig's value sits.

Mechanics:

- Dependency is renamed, because crucible already has a crate called
  `segcache`: `cache_rs = { package = "segcache", version = "0.4", optional = true }`.
- Behind a feature so the default build does not pay for it. Per the Rust
  conventions here, a new feature needs its own clippy rung in CI.
- Pinned to an exact upstream rev via `[patch.crates-io]`, not to a branch:
  `pelikan-io/cache-rs` at `2af1aff1020ba94cb3e58a2722e41b1403e390d1`. A
  branch ref drifts if anyone pushes mid-sweep, and a result that cannot name
  the code it measured is not reproducible. Bump deliberately and re-run,
  rather than mixing arms across revs.

**Which tree the design was read against.** The cache-rs findings in this
document and in the S3-FIFO design — the `freq == 0` prune test, the
unconditional fixed hash seed, the CLOCK-vs-merge reading — were taken from a
local checkout sitting on the feature branch `insert-waits-instead-of-burning`
(`b73f0fe`), one commit off upstream. The benchmark measures upstream
`2af1aff`. That branch changes `segcache.rs`'s insert-after-rollback path and
`builder.rs`, neither of which carries the findings above, so they are not
expected to move — but the two trees are not identical and the results should
say which one they measured.

## Versions: main on both, and why there is no version axis yet

Both repos at `main`. A version sweep was considered and dropped, because the
two axes are not symmetric:

- **cache-rs would be cheap.** It is an external dependency behind an adapter
  we control, so crates.io `0.4.4` ↔ git `main` is a `[patch.crates-io]` entry.
- **crucible is not.** The replay rig landed on `main` on 2026-09-18; tag
  `v0.4.1` has no `trace.rs` or `replay.rs`. And main's rig cannot be pointed
  at v0.4.1's cache crates, because `cache_trait.rs` and `config.rs` moved 284
  lines between them, including breaking additions (`CacheInternalStats::
  eviction_latency`, `EvictionStrategy::Clock`, `MergeConfig::initial_threshold`).

The spec still takes a crucible ref as a parameter, defaulting to `main`, so
the axis exists at no cost and becomes usable from the next release onward —
every future tag will contain the rig.

**Recorded escape hatch:** if version-over-time comparison becomes wanted for
releases that predate the rig, the answer is to lift the harness into an
external crate that depends on both engines by version, rather than to
backport the rig. Noted here so it is a known option rather than rediscovered.

## Held constant

Two of these need active work; the rest are trivial.

| Variable | Treatment |
|---|---|
| `hash_power` | **Converted, not copied.** crucible: `num_buckets = 1 << power`. cache-rs: `bucket_power = power - 3`, i.e. power counts *slots*. Equal slot counts need `cachers_power = crucible_power + 3`. Copying the number gives cache-rs an 8x smaller table. |
| table pressure | Assert `set_errors == 0` on **both** arms. Table pressure alone moved miss ratio 0.2810 → 0.2958 on this trace, which is the same magnitude as the effects being compared. |
| heap size | Same nominal — this is the budget being held, and the point of the exercise. Achieved resident items are reported **as an explanatory column, not a correction**: cache-rs's 5 B `RawItem` against crucible's 6/10 B means the same megabytes hold different item counts, and that difference is part of what is being ranked. It is reported so a later question ("did it win on the algorithm or on the header?") can be asked rather than assumed. |
| segment size | Same (1 MiB). |
| trace, window, warmup | Same. |
| latency measurement | The replay driver times `apply_record`, which is engine-agnostic, so the read/write histograms cover both arms with no adapter involvement. The in-cache `eviction_latency` histogram is crucible-only and has no cache-rs counterpart, so the leaderboard's latency column is the **replay-side** figure — identical machinery on both sides. |
| hash seed | Different constants per engine, and that is fine: the noise floor is zero once the table is not oversubscribed, verified across three machines. cache-rs seeds with fixed constants unconditionally; crucible needs `hashtable_seed` set explicitly. |

## Varied

Policy-matched pairs:

    crucible s3fifo  <->  cache-rs S3Fifo
    crucible merge   <->  cache-rs Merge
    crucible fifo    <->  cache-rs Fifo
    crucible cte     <->  cache-rs Cte
    crucible random  <->  cache-rs Random

    cache-rs only, no crucible counterpart: Util, RandomFifo, None

Sizes: the in-band points for the trace, from the S3-FIFO design's
compulsory-floor method — for the high-overwrite trace, 32/48/64 MB.

**First cut:** the high-overwrite trace only. 5 pairs x 2 engines x 3
sizes = 30 arms, roughly 15 minutes on the x86 host. It is the highest-overwrite
trace in the corpus and the one where policies separated most, so pairs that
do not move there will not move elsewhere. Widen to a second mid-overwrite trace,
a mid-overwrite trace and the the low-overwrite control control only if they do.

## Implementation notes

**The `Cache` trait has methods cache-rs cannot honour.** `get_value_ref`
returns crucible's `ValueRef`, which pins segment memory by refcount; cache-rs
has no equivalent to hand back. The adapter returns `None`.

That is a hazard worth naming: a caller that treats `None` as "miss" would
read every lookup as a miss. The replay driver uses `with_value` and never
calls `get_value_ref`, so the path is cold — but the adapter must carry a
comment saying so, because the failure would be silent and would look like a
catastrophic hit-ratio result rather than a wiring error.

Similarly `begin_segment_set` and the zero-copy reservation path: not
implemented, not reachable from the replay driver.

**Envelope checks apply unchanged.** `envelope_verdict` already rejects a run
whose main layer never evicted or whose hashtable refused writes. Both apply
to the cache-rs arm — though "main layer never evicted" is expressed through
crucible's `CacheInternalStats`, so the adapter needs to report cache-rs's own
eviction counter through that struct or the check silently passes on an
all-zero snapshot. **This is the most likely way to get a plausible wrong
answer from this work**, and it gets an explicit test.

## Acceptance

- A cache-rs arm and a crucible arm at matched policy and converted
  `hash_power` both report `set_errors == 0` and a non-zero eviction count,
  or the run is rejected.
- Miss ratios are reproducible across reps on both arms, as they are today for
  crucible on three machines.
- Results report achieved resident items alongside miss ratio, so the header
  difference is visible and attributable rather than assumed either way.
- The output is a leaderboard of engine+policy pairs, presented sorted both
  ways when the two orders differ, and stated as a single order only when
  they agree.

---

# Results

**Run:** an x86_64 host (Zen 4), 2026-09-19, experiment
`01a0bb4b-851b-71e1-1614-6bf03fa98a60`, branch `feat/cache-rs-backend` at
`5498e73`. Trace the high-overwrite trace, 5M warmup + 10M measured
records, 1 MiB segments, `hashtable_power = 22`, 4 reps.

**120 arms, 120 accepted, 0 rejected.** Every arm reported `set_errors == 0`
and a non-zero eviction count, so no point is excluded and none is averaged
in against the envelope rule.

## The headline: the two orders disagree at every size

They disagree at 32, 48 and 64 MB, so there is no single ranking to state.
At 48 MB:

| sorted by miss ratio | | sorted by write p99.9 | |
|---|---|---|---|
| 1 | cachers/merge 0.2128 | 1 | segment/random 4.35 us |
| 2 | segment/s3fifo 0.2810 | 2 | segment/cte 4.65 us |
| 3 | segment/merge 0.2972 | 3 | cachers/random 4.65 us |
| 4 | cachers/fifo 0.3040 | 4 | cachers/cte 4.95 us |
| 5 | cachers/cte 0.3043 | 5 | segment/merge 5.08 us |
| 6 | segment/fifo 0.3085 | 6 | segment/fifo 5.80 us |
| 7 | segment/cte 0.3085 | 7 | segment/s3fifo 5.85 us |
| 8 | segment/random 0.3085 | 8 | cachers/merge 5.92 us |
| 9 | cachers/random 0.3412 | 9 | cachers/fifo 6.00 us |
| 10 | cachers/s3fifo 0.5001 | 10 | cachers/s3fifo 6.40 us |

`cachers/merge` is first on hit ratio and eighth of ten on write tail. This
is the same shape as the `min_segments` result from 2026-09-18: the pair that
retains the most also stalls the most on the write path.

## Full table, 48 MB (means of 4 reps)

| engine/policy | miss | resident | w_p50 | w_p99.9 | w_max | evictions |
|---|---|---|---|---|---|---|
| cachers/merge | 0.2128 | 84,176 | 0.32 | 5.92 | 11149.3 | 20 |
| segment/s3fifo | 0.2810 | 39,517 | 0.28 | 5.85 | 5832.7 | 188 |
| segment/merge | 0.2972 | 31,533 | 0.25 | 5.08 | 4726.8 | 199 |
| cachers/fifo | 0.3040 | 28,443 | 0.28 | 6.00 | 1667.1 | 595 |
| cachers/cte | 0.3043 | 28,136 | 0.22 | 4.95 | 1538.0 | 595 |
| segment/fifo | 0.3085 | 27,539 | 0.28 | 5.80 | 1562.6 | 600 |
| segment/cte | 0.3085 | 27,539 | 0.28 | 4.65 | 1257.5 | 600 |
| segment/random | 0.3085 | 27,539 | 0.28 | 4.35 | 1257.5 | 600 |
| cachers/random | 0.3412 | 25,330 | 0.25 | 4.65 | 1597.4 | 595 |
| cachers/s3fifo | 0.5001 | 9,534 | 0.30 | 6.40 | 3219.4 | 639 |

Miss ratio tracks resident items almost monotonically down this table. That
is the point of carrying the column, and it is why no row here is a claim
about an algorithm.

## What the residency column changes

`cachers/merge` wins hit ratio at every size, and the margin is far outside
the noise (32% relative at 48 MB against a 0.94% rep spread). But it holds
**2.1x to 2.7x more items in the same budget**:

| size | cachers/merge | segment/merge | ratio |
|---|---|---|---|
| 32 MB | 52,741 | 25,242 | 2.09x |
| 48 MB | 84,176 | 31,533 | 2.67x |
| 64 MB | 108,742 | 39,617 | 2.74x |

A 5 B versus 6/10 B header does not explain a 2.7x gap. Comparing every
policy pair like-for-like localises it, and the answer is **not** a general
crucible packing deficiency:

| policy | crucible | cache-rs | ratio | |
|---|---|---|---|---|
| fifo | 27,539 | 28,443 | 1.03x | equivalent |
| cte | 27,539 | 28,136 | 1.02x | equivalent |
| random | 27,539 | 25,330 | 0.92x | crucible ahead |
| merge | 31,533 | 84,176 | **2.67x** | cache-rs ahead |
| s3fifo | 39,517 | 9,534 | **0.24x** | crucible ahead |

(48 MB shown; 32 and 64 MB give the same picture -- fifo/cte within 3% at
every size, merge 2.09-2.74x to cache-rs, s3fifo 3.7-5.5x to crucible.)

On the policies where both engines do the simple thing, they pack within a
few percent -- the header difference showing up as roughly the couple of
percent it should. The whole gap is concentrated in the two policies that
make retention decisions, and it points in **opposite directions**: cache-rs's
merge retains far more than crucible's, and crucible's S3-FIFO retains far
more than cache-rs's. Neither engine is broadly better at holding items.

**This is still not an algorithm result**, and the ranking must not be read
as one -- retaining more is not the same as retaining better. But it does
narrow the open question from "why does cache-rs fit more" to "why does
crucible's merge consolidate less", which is a specific and testable thing.

A hypothesis worth one sweep: crucible's merge walks a chain of
`min_segments` (default 4), while cache-rs's is `Merge { max: 8, merge: 4,
compact: 2 }` -- examining up to 8 segments and consolidating down to 2. The
2026-09-18 work established chain length as the dominant variable in
crucible's merge, and that its hit-ratio gain is paid for in write-tail
latency. cache-rs's merge sitting first on hit ratio and eighth of ten on
write p99.9 is the same trade, further along the same curve. Sweeping
crucible's `main_merge_segments` upward would test this directly.

Isolating the algorithm from residency still needs a second sweep matched on
resident items rather than megabytes; that is a separate experiment with its
own design, not a reinterpretation of these rows.

The same column flips the reading of the worst row. `cachers/s3fifo` looks
catastrophic at 0.5001 -- but it holds 9,534 items against crucible's 39,517,
a 4.1x deficit. Its S3-FIFO admission queue is evidently sized or packed very
differently. Again: a residency fact, not an algorithm verdict.

## Reproducibility differs by engine, and this is a real finding

| engine | cells bit-identical across 4 reps |
|---|---|
| crucible | **15 of 15** |
| cache-rs | 3 of 15 (only `s3fifo`) |

crucible's zero noise floor now replicates on a fourth machine. cache-rs is
bit-identical under `s3fifo` and varies under `merge`, `cte`, `fifo` and
`random` -- worst spread 3.66% (`cachers/merge` at 32 MB).

Every gap quoted above is larger than the spread of the cells it separates,
so the orderings hold. But cache-rs arms **require replication**; a
single-rep cache-rs number is not a measurement. Crucible arms do not.

## A crucible bug this comparison found

`segment/fifo`, `segment/cte` and `segment/random` return **byte-identical**
miss ratios, resident counts and eviction counts at all three sizes. They are
the same code path:

    // cache/core/src/layer/ttl_layer.rs:1441
    match self.config.eviction_strategy.merge_params() {
        Some(merge_config) => self.try_merge_eviction(&merge_config, hashtable),
        None => self.evict_randomfifo(hashtable),
    }

`merge_params()` returns `None` for `Fifo`, `Random`, `Cte` and
`ExpireFirst` (`cache/core/src/config.rs:245-248`), so all four fall into
`evict_randomfifo`. Three documented policies are accepted as distinct
configuration and execute identically; `cte` claims "closest to expiration"
and does not consider expiration at all.

cache-rs distinguishes them (0.3040 / 0.3043 / 0.3412 at 48 MB), which is how
the collapse became visible. Filed separately.

## Caveats that travel with these numbers

- A row is "this engine, this policy, at this budget, on this trace". It is
  not "this algorithm". The residency column is what lets that question be
  asked; on this data it is not yet answered.
- One trace, one host, one session. the high-overwrite trace is the
  highest-overwrite trace in the corpus and the one where policies separate
  most. Pairs that do not move here will not move elsewhere; pairs that do
  move here still need a second mid-overwrite trace and a mid-overwrite trace before they generalise.
- **`cachers/merge` at 64 MB ran only 5 eviction passes** across the measured
  window (against 194 for `segment/merge`). The envelope check requires
  `evictions > 0`, which that clears, but 5 passes is thin evidence of a
  saturated cache -- the cell sits close to the "measures allocation rather
  than eviction" regime the check exists to reject. The `>0` threshold is
  weaker than it should be, and this cell in particular should be re-run at a
  smaller budget before its ranking is relied on.
- Write latency is the replay-side figure, identical machinery on both arms.
  crucible's in-cache `eviction_latency` histogram has no cache-rs
  counterpart and is not used here.

## Why crucible's merge retains less: the mechanism

Traced through both implementations. Two independent causes, both specific
to merge, which is why the non-merge policies measure equal.

### 1. Overwrites never trigger compaction

crucible reclaims dead space only on explicit `delete`:

    // cache/core/src/cache.rs:1096, inside delete()
    self.mark_deleted_at_with_compact(location);   // the ONLY compacting caller

    // cache/core/src/cache.rs:580, inside set(), on overwriting a live key
    self.mark_deleted_at(old_location);            // no compaction

`try_compact_segment` exists (`ttl_layer.rs:597`) but its only caller is
`mark_deleted_and_compact`. An overwrite marks the superseded item dead and
leaves the bytes stranded.

cache-rs fires its **compaction merge** on occupancy instead of on cause:
it triggers when a segment falls below `1/compact` live bytes (`compact: 2`
-> below 50% occupancy), and that catches dead space from overwrites and
deletes alike.

the high-overwrite trace is the highest-overwrite trace in the
corpus, so this is close to a worst case for crucible and close to a best
case for cache-rs.

### 2. Every reclamation prunes live items, by design

cache-rs has two merge sub-modes; crucible has one.

- cache-rs **compaction merge** copies a fragmented segment *without
  pruning*: it recovers dead space and discards nothing live.
- crucible's merge always prunes. The threshold escalates inside each pass
  (`ttl_layer.rs`, adaptive block):

      threshold = initial_threshold            // 0
      for each candidate segment in the chain:
          retain items with freq > threshold
          if total_retained / total_items > target_ratio:   // 0.5
              threshold += 1

  A fresh insert carries frequency 1 and nothing lowers it -- crucible's
  `apply_frequency_decay` is dead code (#150). So the first segment of a
  chain retains everything at threshold 0, retention reads ~1.0 > 0.5, the
  threshold rises to 1, and every later segment in that chain keeps only
  items read more than once. With `min_segments: 4`, roughly one segment in
  four is retained whole and three are stripped to multi-read items.

crucible therefore cannot recover fragmentation without also throwing away
live data. That is what the counters show at 48 MB: 199 eviction passes to
cache-rs's 20, and 31,533 resident items to 84,176. It evicts ten times as
often *because* each pass fails to recover the dead space that overwrites
keep creating, and each pass destroys live items on the way.

### Two candidate fixes, both testable

1. **Trigger compaction on occupancy, not on delete.** Give the overwrite
   path in `set` the same treatment `delete` gets, or better, check live-byte
   occupancy during the merge pass and take a copy-without-pruning branch
   when a segment is merely fragmented. This is the structural fix and it
   matches what cache-rs does.
2. **Raise `target_ratio`.** It is already a knob (`MergeConfig`, default
   0.5, documented "higher = more retention"). No code change; worth a sweep
   before the structural fix, if only to size how much of the gap is the
   retention target versus the missing compaction mode.

Both need measuring against write-tail latency, not just hit ratio: the
2026-09-18 chain-length work showed retention gains on this engine are paid
for in write-path stalls, and cache-rs's merge -- first on hit ratio, eighth
of ten on write p99.9 -- is sitting further along that same curve.

### The retention target is coupled to the chain length, and nothing enforces it

A merge pass takes `min_segments` candidates and reserves exactly **one**
spare to copy into (`ttl_layer.rs:1094`, `reserve_spare()` -- singular).
Retained bytes therefore cannot exceed one segment: **1/N of the chain's
capacity**. When the spare fills, the copy loop does not fall back on
frequency:

    } else {
        // Spare is full -- discard remaining items

Items are dropped **by position**, so an overflowing pass silently stops
being frequency-based partway through and discards the tail of the chain
arbitrarily.

The safe bound is `1/(N*L)` where `L` is the live fraction of the candidate
segments. With the defaults -- `min_segments: 4`, `target_ratio: 0.5` -- the
pass aims to retain two segments' worth into one, which fits only while
segments are at most half live. `target_ratio` is also counted in **items**
while the constraint is in **bytes**, so variable-size items break the
correspondence even when the ratio looks right.

`target_ratio` and `min_segments` are independent knobs carrying a joint
constraint that is neither validated nor documented.

**This likely contaminated the 2026-09-18 `min_segments` sweep.** That sweep
varied chain length from 1 to 8 with `target_ratio` fixed at 0.5. At N=8 the
capacity-safe target roughly halves, so chain-8 passes would overflow and
discard the back half of each chain by position -- paying to scan eight
segments and then throwing away half of them arbitrarily. That is a live
candidate explanation for why 4->8 bought 0.8% hit ratio for 60% worse max
stall, and it means the recommendation to stay at 4 was reached for possibly
the wrong reason even though it was the right call. Re-running with the two
knobs coupled would settle it.

### The fix is smaller than "add compaction"

crucible's merge already compacts -- it copies live items into a spare. What
it cannot do is compact *without* pruning, because the threshold escalates
whenever retention exceeds target.

Both behaviours are already expressible. Per `MergeConfig::CLOCK`'s own
documentation, `target_ratio: 1.0` pins the threshold permanently, since
retention can never exceed 1.0. So:

    // prune-and-reclaim, today's default
    MergeConfig { min_segments: 4, target_ratio: 0.5, initial_threshold: 0 }

    // pure compaction: keep every live item, reclaim only dead bytes
    MergeConfig { min_segments: 2, target_ratio: 1.0, initial_threshold: 0 }

The second satisfies the 1/N bound exactly when occupancy is under half --
which is precisely cache-rs's `compact: 2`: fire when a segment falls below
`1/compact` live bytes, merge that many into one, prune nothing.

So the missing piece is not an algorithm. It is an occupancy check that
selects between two parameter sets the config can already express.

---

## Re-validation under the stronger envelope check (2026-09-20)

The sweep above ran under an envelope check that accepted any run with a
non-zero eviction count. crucible#158 replaced that proxy with a direct
fill test, which is a stronger bar, so the published ranking was re-run
against it: experiment `01a0bf72-5be0-7185-990d-8e87124289d4`, same host,
same trace, same parameters.

**The ranking holds. 120 arms, 120 accepted, 0 rejected.**

| 48 MB, sorted by miss ratio | miss | resident | w_p99.9 | evicts |
|---|---|---|---|---|
| cachers/merge | 0.2138 | 80,801 | 4.00 | 22 |
| segment/s3fifo | 0.2810 | 39,517 | 3.98 | 188 |
| segment/merge | 0.2972 | 31,533 | 3.45 | 199 |
| cachers/fifo | 0.3043 | 28,276 | 4.12 | 595 |
| cachers/cte | 0.3044 | 28,224 | 4.10 | 595 |
| segment/fifo | 0.3085 | 27,539 | 4.12 | 600 |
| segment/cte | 0.3085 | 27,539 | 4.05 | 600 |
| segment/random | 0.3085 | 27,539 | 4.03 | 600 |
| cachers/random | 0.3430 | 25,296 | 4.35 | 595 |
| cachers/s3fifo | 0.5001 | 9,534 | 4.28 | 639 |

The two sort orders still disagree at all three sizes, and every conclusion
above survives.

### The top row was the one in doubt, and it stands

`cachers/merge` at 64 MB led the hit-ratio ranking on **5 eviction passes**
against `segment/merge`'s 194, which looked thin enough to be an artefact.
The fill check settles it: **3 of 64 segments free, 4.7%**. The cache was
genuinely saturated; the pass count was simply a bad proxy, because a merge
pass reclaims a variable number of segments. The ranking earned its place.

### A false rejection the new check introduced, and what it cost

The first re-run rejected 6 of 120 arms, every one at 32 MB with exactly 4
free segments. Free segment counts turn out to be **absolute constants set
by each policy's reserve, not a fraction of the heap**: segment/s3fifo holds
4 free at 32, 48 and 64 total; segment/fifo holds 2 at every size;
cachers/fifo holds 0. A percentage-only bar therefore tightens as the heap
shrinks and eventually fires on the reserve alone -- 4 of 32 is 12.5% while
the same reserve at 4 of 64 is 6.2%.

Fixed by requiring both an absolute and a relative bound (more than 8 free
segments **and** more than 25%). On the measured data reserves reach 5
segments and 15.6%, while a cache that never filled sits at 34-68% and
hundreds of segments.

Worth recording because the check is the thing that protects every number
here, and it was wrong for a day in a direction that silently deletes valid
data rather than admitting invalid data.

### Determinism replicates across whole experiments

Comparing the two independent runs cell by cell:

| | cells |
|---|---|
| bit-identical across both runs | **18 of 30** |
| differing | 12 of 30 |

The 18 are all 15 crucible cells plus cachers/s3fifo. The 12 that moved are
exactly cache-rs's non-s3fifo cells. This is the same split the within-run
rep analysis found, now replicated across separate experiments a day apart --
so it is a property of the engines, not of a single run's scheduling.

---

## crucible#154 measured: choosing the threshold before copying

**Experiment** `01a0c088-6212-71f8-3abd-d8164f98f49d`, board A, 6 reps, both arms
built from the same tree and interleaved within each rep.
`942c8bf` (before) vs `0af6f1d` (after). 48 MB, 1 MiB segments, merge policy,
the high-overwrite trace.

| | before | after | change | against its own null floor |
|---|---|---|---|---|
| miss ratio | 0.2972 | 0.2560 | **-13.8%** | arms bit-identical in the null |
| evict pass mean | 4022 us | 2843 us | **-29.3% (faster)** | **89x** the 0.33% floor |
| resident items | 31,533 | 29,499 | -6.4% | |
| write p99.9 | 7.08 us | 7.73 us | **+9.2%** | 9.4x its 1.0% floor |
| write max | 5953 us | 5625 us | -5.5% | inside its 6.8% floor -- not resolvable |

### The predicted cost is a saving

The design estimated under 10% overhead and allowed it might be net negative.
It is **29% faster**, and the arms do not overlap: the slowest before-run
(3974 us) beats the fastest after-run (2866 us). Choosing the threshold from a
histogram avoids the probe-then-discard work the reactive rule paid for on
every overflowing pass -- and on this trace most passes overflowed.

### It retains fewer items and misses less

13.8% better miss ratio while holding 6.4% **fewer** items. The pass is not
keeping more, it is keeping better ones, which is the signature of removing a
position-based discard rather than of relaxing a threshold.

### One real regression

Write p99.9 rose 9.2% (+0.65 us on ~7 us), 9.4x its own null floor, so it is
genuine rather than noise. The likely cause is the per-pass scratch
allocation, named as the one genuine cost when the design was chosen; a
reusable per-layer buffer would remove it at the price of synchronising
concurrent passes. Write **max** moved -5.5%, inside its own 6.8% floor, so
the worst-case stall is unchanged.

### Why three different verdicts were possible

Each metric was scaled against a null run of the same harness with identical
binaries in both arms (experiment `01a0bfaf-d976-7100-a5e6-111270bc6937`),
which gives a per-metric floor: 0.33% for eviction mean, 1.0% for write
p99.9, 6.8% for write max. Without that, "29% faster", "9% slower" and "no
change" could not have been told apart from each other or from drift. The
laptop could not have done this job at all -- five identical runs there
spanned 35% on eviction mean.

### Side effect on #152

The multi-millisecond merge stall is now 2.8 ms rather than 4.0 ms on a Pi 4B.
Still a stall on the write path and still worth moving off it, but a third
smaller than when that issue was filed.

---

## The merge retention knee, across four traces (2026-09-21)

**Experiment** `01a0c47f-39c6-7153-0d99-4d66b2001585`, board A, 4 traces x 6
ratios x 3 reps = 72 arms, all accepted. Same binary throughout (`191dbd5`);
only `main_target_ratio` changes between arms, which is what exposing that
knob to config bought.

| trace | miss @0.5 | miss @1.0 | change | total evict @0.5 | @1.0 | knee |
|---|---|---|---|---|---|---|
| high-overwrite | 0.2511 | 0.2374 | **-5.5%** | 566 ms | 1010 ms | **0.8** |
| mid-overwrite | 0.7577 | 0.7518 | -0.8% | 9250 ms | 12772 ms | 0.6 |
| mid-overwrite 2 | 0.6429 | 0.6420 | -0.1% | 2945 ms | 2799 ms | 0.6 |
| low-overwrite (control) | 0.8428 | 0.8414 | -0.2% | 114 ms | 112 ms | 0.6 |

### The ratio matters on one trace of the four

Only the high-overwrite trace shows a retention effect worth the name. On the other three
the whole 0.5-to-1.0 range moves miss ratio by 0.8% or less, at or below what
replication can resolve. a mid-overwrite trace nonetheless pays **+38%** in total
eviction time for its 0.8%, which is the worst trade in the table.

On the high-overwrite trace the knee is **0.8**: it reaches 0.2373, and 0.9 and 1.0 return
0.2374 -- indistinguishable. So `target_ratio: 1.0`, the endpoint this work
has been treating as "the expensive option", is **dominated**: 0.8 reaches the
same hit ratio at the same cost. The real choice is 0.5 (0.2511 at 566 ms)
against 0.8 (0.2373 at 1004 ms) -- 5.5% better miss ratio for 77% more
eviction time.

### The adaptive chain's null result holds, and the control supports the mechanism

Eviction pass counts, 0.5 -> 1.0:

- the high-overwrite trace 137 -> 198: the chain extends, but **only at low ratio**
- a mid-overwrite trace 5072 -> 5076, a second mid-overwrite trace 736 -> 750, the low-overwrite control 62 -> 63:
  flat, no extension at any ratio

This is the predicted shape. The chain extends only into sparsity, sparsity
comes from overwrites, and the high-overwrite trace is the highest-overwrite trace in the
corpus. the low-overwrite control was included as the low-overwrite control precisely because
it could have falsified that -- it did not. And on the high-overwrite trace the extension
disappears once the ratio stops manufacturing the fragmentation it feeds on.

The adaptive chain is therefore not carrying weight on any trace measured.

### What this table cannot support

Three of the four traces sit at **64%, 75% and 84% miss** at 48 MB. That is
far below their working sets, where capacity misses swamp any retention
decision -- so their flatness may mean "the ratio does not matter" or may
mean "nothing matters at this size". These points are valid eviction-regime
measurements (the cache filled, it evicted, the envelope accepted them) but
they are not in-band for a policy comparison.

The size was held at 48 MB across all four for comparability with the earlier
work. Reading the three flat rows as evidence about retention would require
re-running them at per-trace in-band sizes, chosen by the compulsory-floor
method the S3-FIFO design specifies. Until then the only trace this sweep
speaks for is the high-overwrite trace.
