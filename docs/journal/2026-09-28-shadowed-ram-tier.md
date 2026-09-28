---
status: open
opened: 2026-09-28
updated: 2026-09-28
---

# A RAM tier that shadows the disk tier

## Goal

Restructure the segment backend's RAM tier for deployments with a disk tier:

1. **An admission region:** an S3-FIFO-style small queue that makes the
   admission and ghost decisions.
2. **A shadow region:** RAM segments whose items shadow copies stored in
   disk segments. Merge eviction runs on these segments, segcache style,
   to keep the hottest items in RAM.

The aim is to make RAM eviction cheap, make promotion from disk useful, keep
disk writes under the admission decision's control, and allow a warm
restart. Counters (incr/decr) may need their own storage. That question is
part of this entry.

## Decision Criteria

GO on building the shadow region needs all of:

- **The case, without the write tail.** This criterion originally asked
  whether merge's inline stalls were worth removing. They are (see
  Evidence), but the shadow region does not remove them: a clean eviction
  skips the *demotion write*, and merge still copies retained items within
  RAM. The case for this design therefore rests on disk writes,
  useful promotion and warm restart. The tail is a separate lever
  (Deferred).
- **The prototype against today's merge demotion (#11)**, on the same
  A/B grid:
  - bytes written to disk no higher;
  - write p99.9 and max no worse;
  - miss ratio no worse;
  - no hits past TTL.

GO on specialized counters needs the incr/decr census (below) to show
counters carry a meaningful share of ops, or dominate some clusters.

## Scope

In: segment backend with a disk tier; RAM layout, eviction and
promotion; write-back of mutated items; counter storage; what disk recovery
must not do.

Out: slab and heap backends; the cache-rs comparison; wire protocol changes.

## Evidence

Measured in the lab, single seed unless stated, with cache-bench trace
replay and its miss oracle:

- **Merge ignored the disk tier until #11.** Configuring one switched RAM
  eviction to whole-segment random-FIFO. #11 makes merge demote what it
  prunes.
- **Disk expiry ran on the wrong clock until #12.** Under trace replay the
  disk layers stamped deadlines in wall time, and one TTL-bearing trace
  served 31k-50k hits past TTL per run. Production was unaffected; the fix
  keeps Unix time.
- **Merge demotion against whole-segment eviction, 1MB segments.** Merge
  writes 15-30% fewer bytes and serves 5-10x fewer reads from disk. It keeps
  the hot set in RAM.
  - cluster14 (no TTLs, disk fills): merge wins, 0.2229 against 0.2569.
  - cluster4: level.
  - cluster6: merge loses, 0.2106 against 0.1993.
- **Early expiry explains the cluster6 loss.** Items merge keeps are copied
  into a segment carrying the chain's earliest deadline. At 64KB segments,
  merge's early misses at cluster6 fall 7x and it leads (0.1334 against
  0.1370).
- **Segment size dominates any disk setting.** Without a disk, 1MB to 64KB
  cuts cluster6's miss ratio 38% (0.2208 to 0.1369). At 64KB a disk tier
  adds almost nothing there.
- **The cost of small segments.** At cluster4, 64KB ran about 20x the
  eviction passes of 1MB (13,494 against 678). Merge runs inline in `set`.
  Tail latency is being measured now.
- **`demotion_threshold` stays 0 under merge.** Merge has already filtered
  what it prunes, and a second filter only costs hits.
- **Small segments move merge's stall into p99.** Merge, no disk, three
  reps on one pinned host. A pass costs in proportion to segment size, and
  total stall time barely changes. From 1MB to 64KB:
  - write p99.99 falls about 10x, from 3-5ms to 0.3-0.6ms;
  - write p99.9 rises 7x, and 256KB is its worst size (514-840us);
  - write p99 rises 7-41x at 64KB, to 42-172us;
  - reads do not move.
  cluster14 (no TTLs) gains nothing in miss ratio and pays the worst p99.
- **Frequency counts reads (#9).** Inserts start at 0, so a threshold means
  reads.
- **Disk-tier counters (#10):** `disk_hits` and `demoted_bytes`.

## Design and Implementation

Nothing is implemented. The current design, from discussion:

### Regions

- **Admission:** a small FIFO. An item read before it reaches the tail
  moves on to the shadow region; an unread one becomes a ghost. Ghosts
  keep their read count, so an item that returns is admitted on its next
  write.
- **Shadow:** RAM segments under merge eviction, plus disk segments.
  Items in the shadow region's RAM segments are **clean** or **dirty**.

### Clean and dirty (write-back)

- **Clean:** the item matches a disk copy, and its RAM header records that
  copy's location (about 8 bytes an item).
- **Dirty:** no valid disk copy. That covers newly admitted items and any
  item mutated since it was last written down.
- **Evicting a clean item is free:** a CAS moves the hashtable from the RAM
  location to the disk location. If a concurrent mutation moved the
  hashtable first, the CAS fails, which is correct.
- **Evicting a dirty item writes it down,** as demotion does today. One
  write coalesces every mutation since the item was last clean.
- **Promotion** from disk copies the item into a RAM segment and marks it
  clean. That makes the segment backend's `promotion_threshold`, unread
  today, meaningful.

This is the "keep the disk copy on promotion" variant. Short-lived items never
touch disk, and hot items stop generating writes once they have a disk copy.
Full write-through at admission is the other end of the same knob, and both
can be measured.

### Rules to design in

- **One deadline for both copies.** A shadow copy carries its disk copy's
  deadline, never a later one. A TTL is a ceiling.
- **The disk copy can die first.** Disk eviction or expiry can reclaim the
  segment a clean item points at. Eviction checks the recorded location's
  incarnation; if the copy is gone, the item is treated as dirty.
- **A mutation never repoints to a disk copy.** The new version is dirty and
  has no disk location, and the old disk copy is dead bytes.
- **Recovery must not resurrect superseded copies.** A mutated item's old
  version is still on disk. A restart that indexes whatever disk segments
  hold would serve a stale value, such as a counter from before its last
  acknowledged incr. The candidate fix: a small tombstone written to disk
  when an item goes from clean to dirty. That is once per stretch of heat,
  not once per mutation. The hazard exists in today's design too (an
  overwrite of a disk-resident item leaves the old copy), and recovery is
  not yet wired up.

### Counters

Today `increment` (`cache/core/src/cache.rs`) reads, parses the ASCII,
formats the result, then CAS-appends a new item. A hot counter leaves one
dead copy per incr, moves the hashtable per incr, and retries its CAS more
under cross-core contention. In place is ruled out for ordinary items because
a zero-copy GET may be sending their bytes.

A specialized counter can update in place: its wire value is ASCII and its
storage a binary `u64`, so a read always formats into a buffer, and nothing
ever holds a reference into counter storage.

- **A separate slot region, not a flag on segment items.** Segment items
  move under merge and compaction, so an in-place `fetch_add` could land on
  a copy already taken, which is a lost update. Slots never move.
- **A slot holds** the key, an `AtomicU64` and a deadline. The hashtable
  points to it through its own pool id. Frequency stays in the hashtable.
- **Conversion:** the first incr on a string value parses it into a slot. A
  SET, append or prepend turns it back into an ordinary item. That covers
  memcache and Redis.
- **Eviction:** CLOCK over slots. An evicted counter is written down as an
  ordinary ASCII item. An incr on a disk-resident counter promotes it into a
  slot.
- **Interaction with the shadow region:** hot counters are always in RAM,
  in a slot, and never dirty the segment region.

## Outcome

Open, paused by choice.

1. **The segment-size latency run: done** (Evidence). Predicted: per-pass
   stalls and the max fall with segment size, p99.9 rises at 64KB, p99 does
   not move. The first two held; p99 rose 7-41x at 64KB, which refutes the
   third. The run did not gate this design as written: merge copies within
   RAM whatever the tier layout (Decision Criteria).
2. **The incr/decr census** across the trace corpus: share of ops and
   concentration on few keys, per cluster. Not started.

Restart condition: the census, and a decision to prototype.

## Derived Documents

None yet. A design spec under `docs/superpowers/specs/` if this reaches GO.

## Deferred or Reopen Items

- **Take eviction off the write path.** Keeping free segments ahead of
  demand in the background would remove merge's stall from `set` at any
  segment size. At 64KB that stall now sits in p99. This is orthogonal to
  the tier layout and may matter more than it.
- **No single segment size fits every workload.** It trades early
  expiry against the write tail, and a trace without TTLs gets only the
  cost. A per-deployment setting, or buckets sized by TTL, rather than one
  new default.
- **incr/decr on a counter only on disk returns `SegmentNotAccessible`.**
  It used to spin forever. That was reproduced, with a second trigger: an
  expired counter, which the hashtable still indexes until its segment is
  reclaimed. It is now treated as absent. The error for the disk case is a
  stopgap; the design above promotes the counter back into RAM (the async
  path GET already takes), which would let incr succeed.
- **ADD on an expired key returns `KeyExists`** for the same reason: the
  hashtable's `contains` does not check expiry. Not yet fixed.
- **Slab and heap** expire on the system clock (`cache/slab/src/item.rs`,
  `cache/heap/src/entry.rs`). Slab also subtracts a 2024 base epoch that
  replayed timestamps saturate. Their TTL results under trace replay are
  invalid.
- **Disk recovery** (`warm_from_pool`) has no caller. When wired, it must
  pass `crate::clock::now_unix_secs()` and honour the resurrection rule
  above.
- **The segment backend's `promotion_threshold`** is accepted and never
  read. This design would give it a meaning; until then it is a dead knob.
- **Under whole-segment eviction,** `demotion_threshold` 1 beat 0 at
  cluster6 where the disk never evicts. Unexplained, and moot while merge
  demotion ships.
