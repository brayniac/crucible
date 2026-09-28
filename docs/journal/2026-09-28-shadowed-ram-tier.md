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
**Met for a minority** (Evidence): 2 of 54 traces, at 18% and 31% of ops.
Build it as an opt-in hot-counter table, sized to zero by default.

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
- **Counters are rare, and dominate where present.** The census covers 54
  traces and 5.1B records (each read whole or capped at its first 100M):
  - incr/decr are 0.96% of all ops, and 50 traces have none;
  - **cluster23:** 31.0% of ops are incr, on 4 keys (7.7M each);
  - **cluster22:** 17.9% incr, over 10.1M keys, the top 100 taking under 6%;
  - two more traces have a few hundred at most;
  - decr is almost unused (389 in the corpus);
  - no incr/decr record carries a TTL. The field is probably recorded on
    writes only, so this says nothing about rate-limiter windows.
- **How many counters are hot at once** (60s windows of trace time, first
  100M records):
  - cluster23: all 4 counters, in every window, about 39k incrs a minute
    each;
  - cluster22: about 27k counters touched a minute, but at most 124 take 10
    or more increments in a minute and at most 5 take 100 or more;
  - counter key lengths: mean 21-54 bytes, longest 95.
  256 slots cover every trace with 2x headroom; cluster22's long tail stays
  on the append path under frequency admission, as it should.
- **The replay under-models incr.** cache-bench replays incr/decr as
  lookups. In crucible each incr appends a copy, so no measurement so far
  includes that cost.
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
under cross-core contention.

**The payoff is proportional to increments per key, not to the number of
counters.** cluster23 sends 31M increments to 4 keys: 31M appended copies,
31M dead ones for merge to reclaim, to hold 4 counters. cluster22 spreads
18M over 10M keys, under 2 each, where the churn is negligible anyway. So
the target is hot counters, not all counters.

**In-place updates inside segments: rejected.** Weighed and turned down,
because segment immutability is load-bearing:

- Demotion reads sealed segments while readers hold them, and merge,
  compaction and the disk write buffers copy sealed items unlocked. All of
  them assume a sealed segment never changes.
- Relocation loses updates. An incr on the old copy between merge's copy
  and its hashtable swap is lost, and the incr cannot tell whether it was.
  Fixing it needs a freeze protocol on every relocating path (merge,
  compaction, demotion, promotion).
- Readers would need an atomic, aligned binary `u64` to avoid tearing, so a
  counter becomes a different item type anyway.
- A CAS token (location + segment generation) is unchanged by an in-place
  incr, so a CAS after an incr would silently overwrite it. Memcache
  requires incr to change the CAS value.

**Chosen: a small table of in-place slots for hot counters.**

- **Admission:** a counter enters when its frequency (already in the
  hashtable) beats the coldest slot's. Others keep today's append path, so
  under pressure the table degrades to current behaviour, never below.
- **A slot** holds the key (the hashtable verifies every match with a full
  key compare), an `AtomicU64` value, a version word for CAS, and a
  deadline. Slots are fixed-size and **provisioned for the worst-case key**
  (250 bytes, about 280 bytes a slot). That costs almost nothing because the
  table is small. The census puts it at **256 slots, about 72KB**, and the
  longest counter key in the corpus is 95 bytes. Slot size classes are not
  needed.
- **Nothing in the table moves**, so there is no lost update in normal
  operation.
- **Leaving the table** is the one relocation. A CLOCK bit per slot, set on
  each incr, finds the victim. Its value word is frozen (a "moved" bit),
  copied out, and written back as an ordinary item, and the hashtable is
  swapped. An incr that finds the word frozen retries through the hashtable
  onto the ordinary item. If there is no segment space for the write-back,
  the counter is evicted, which a cache may always do. This is the only
  place the freeze protocol lives, small enough to model-check with loom
  and shuttle.
- **Expiry** frees a slot; the key is then absent, like any expired item.
- **Conversion:** the first qualifying incr parses a string value into a
  slot. A SET, append or prepend turns it back into an ordinary item. Reads
  format the `u64` as ASCII into a buffer; values this small are copied
  anyway.
- **Budget:** a fixed carve-out from the heap, configurable, zero where there
  are no counters (50 of 54 traces).
- **With the shadow region:** a hot counter lives in its slot and never
  dirties a segment. Its write-back on leaving is its one write.

**Space is not the argument.** A slot costs about what an ordinary item
does (header + key + ASCII value): tens of bytes with a typical key. It does
not save memory; it removes churn. Storing a 64-bit key hash instead of the
key would shrink slots to about 20 bytes. Rejected: a collision would
silently return another key's counter.

## Outcome

Open, paused by choice.

1. **The segment-size latency run: done** (Evidence). Predicted: per-pass
   stalls and the max fall with segment size, p99.9 rises at 64KB, p99 does
   not move. The first two held; p99 rose 7-41x at 64KB, which refutes the
   third. The run did not gate this design as written: merge copies within
   RAM whatever the tier layout (Decision Criteria).
2. **The incr/decr census: done** (Evidence). It meets the counter gate for
   a minority of workloads.
3. **Census, second pass: done.** It sets the table at about 256 slots with
   worst-case key provisioning (about 72KB).

Restart condition: a decision to prototype. A
replay that performs real increments, not lookups, would give cluster23's
current cost as the baseline the table must beat.

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
- **Every operation that decides on presence now checks expiry.** The
  hashtable indexes an expired item until its segment is reclaimed. ADD,
  REPLACE and CAS used to trust that, and now retire the expired entry
  first, as incr and decr do: ADD stores, REPLACE and CAS answer not found.
  Keep this in mind for any new operation that asks the hashtable whether
  a key exists.
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
