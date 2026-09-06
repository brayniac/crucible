# Incarnation-tagged locations

**Date:** 2026-09-06
**Ports:** pelikan-io/cache-rs#78 (`segcache: generation-tagged locations`)
**Addresses:** the stale-location residual recorded in crucible#88

## Problem

An `ItemLocation` names `(pool_id, segment_id, offset)` and carries no incarnation
identity. A segment can be drained, recycled, re-reserved and refilled while a
thread still holds a location naming it, so a stale location can silently resolve
to a *different item at the same address*.

`CacheKeyVerifier::verify` (`cache/core/src/cache.rs:1631`) hands a
hashtable-supplied offset straight to `verify_key_at_offset` with no check that
the segment is still the incarnation the location was issued against. Two
consequences:

1. **False positive.** Segments are append-only from a fixed start, so with
   uniform item sizes the n-th item of every incarnation lands at the same
   offset. If the recycled segment holds the same key at that offset, `verify`
   returns true for an item the caller never asked for.
2. **Data race.** `append_with_header` publishes an offset via `reserve_space`'s
   CAS and *then* lays the header down with a plain `copy_nonoverlapping`. A
   stale-location reader aims at an offset being written right now.

crucible already has `SliceSegment.generation: AtomicU16`, but it is consumed
only by `CasToken` — *beside* the location, not inside it — which is precisely
why the read path cannot see it.

## Why this is not a straight port

cache-rs#78 had slack: it repartitioned `[24 seg][20 off]` into
`[18 seg][6 tag][20 off]` by shrinking an oversized segment id field.

Crucible's 44 bits are fully spent — `[2 pool][18 seg][24 off/8]` — and the
offset field is genuinely used. `server/tests/large_value_io_tests.rs` runs
32 MB segments in CI (22 offset bits) and 64/128 MB under `#[ignore]`
(23–24 bits). There is nothing free to take.

## Design

### 1. Per-pool layout, fixed 6-bit tag

```
Location (44 bits)
  [43..42]  pool_id       2 bits, FIXED POSITION
  [41..  ]  segment_id    42 - 6 - offset_bits
  [  ..  ]  incarnation   6 bits
  [  ..0 ]  offset/align  ceil(log2(segment_size / align_bytes))
```

`pool_id` stays at a fixed position so a decoder reads it *first* and then
selects that pool's layout. This is what lets four pools with different segment
sizes and different alignment factors share one 44-bit word.

```rust
#[derive(Clone, Copy)]
pub struct LocationLayout {
    align_shift: u8,   // log2(align_bytes)
    offset_bits: u8,
    seg_shift: u8,
    seg_mask: u64,
    tag_shift: u8,
    offset_mask: u64,
}
```

Built once at pool construction and stored on the pool. `CacheKeyVerifier` holds
`[Option<LocationLayout>; 4]` beside its existing `pools` array; layers hold
their own pool's layout for `ItemLocation::new`.

### 2. Capacity is invariant under `segment_size`

With a fixed tag width, offset bits gained are segment bits lost, exactly:

```
capacity/pool = 2^(42 - tag_bits) x align_bytes = 2^36 x align_bytes
```

`segment_size` does not appear. **The alignment factor is the only capacity
lever.**

| pool   | align | offset bits (8 MB segs) | seg bits | capacity/pool |
|--------|-------|-------------------------|----------|---------------|
| memory | 8 B   | 20                      | 16       | 512 GiB       |
| disk   | 512 B | 14                      | 22       | 32 TiB        |

Memory keeps 8-byte alignment: a minimal item is `BasicHeader(6) + 1 + 1 = 8`
bytes, so 16-byte alignment would double the smallest items, and 512 GiB x 4
pools is already beyond any single host's RAM.

Disk coarsens to 512 B, and the justification is the read path rather than
capacity. `IoUringPool::item_disk_range` already rounds reads down to a
4096-byte block and returns an intra-block offset
(`cache/core/src/disk/io_uring_pool.rs`), so a disk item straddling a block
boundary costs an extra block read today. Sector alignment cuts that. Average
waste is 256 B, landing on the tier built for larger cold values.

`align_bytes` is a per-pool config field: a power of two, at least 8, and no
larger than `segment_size`. Disk pools additionally cap it at `block_size`,
since aligning coarser than the I/O block buys nothing on the read path.
Defaults are 8 for memory and 512 for disk.

### 3. `incarnation` lives in `Metadata`, not in `generation`

`Metadata` packs `[8 unused][8 state][24 prev][24 next]`. The 6-bit tag goes in
that unused byte.

This is a correctness requirement, not tidiness. **The bump must be atomic with
the state transition.** Storing the tag in the existing `AtomicU16 generation`
would leave a window where the segment is already `Free` but still carries the
old counter; a thread reserving inside that window refills under the old tag,
reintroducing exactly the aliasing being removed.

`generation: AtomicU16` is left untouched — still bumped in `try_reserve`, still
feeding `CasToken`. No CAS-token semantics change. The two counters answer
different questions and this must be documented on both fields.

### 4. Bump sites

> **Corrected 2026-09-06** after the Task 2 review traced the real recycle path.
> The original table said `Locked -> Free` bumps. **That transition does not
> occur on crucible's main eviction path**, so implementing the table literally
> would have left the tag at 0 forever and made the feature inert while every
> test passed. The actual path is:
>
> ```text
> Sealed -> Draining -> Locked -> Reserved     (cas_metadata, in the layers)
>                                    |
>                                    v
>                        pool.release() -> try_release  =>  Reserved -> Free
> ```
>
> `fifo_layer.rs:227,324,425,517`, `ttl_layer.rs:283,486,734,841`,
> `disk_layer.rs:270`. The `Reserved -> Free` that follows is precisely the
> transition the original table forbade bumping.

`incarnation` bumps when a **used** incarnation ends. Stated as a property of
the segment rather than as a list of state pairs, because the pair list is what
went wrong:

| transition                   | bumps | why                                        |
|------------------------------|-------|--------------------------------------------|
| **leaving `Locked`**, to any state | yes   | drained and cleared; covers both `Locked -> Reserved` (the real recycle path) and `Locked -> Free` |
| `AwaitingRelease -> Free`    | yes   | condemned, last reader released            |
| `force_free` (`* -> Free`)   | yes   | flush recycles every segment at once       |
| `Free -> Reserved`           | NO    | start of an incarnation, not the end       |
| `Reserved\|Linking -> Free`  | NO    | reserved but never used                    |

Keying on *leaving `Locked`* rather than on a destination state is deliberate:
`Locked` is reached only after a drain, so any exit from it ends a used
incarnation regardless of which path took it. Enumerating destination pairs is
what produced the original error.

The exclusion is the substance of cache-rs#78's prerequisite. `memory_pool.rs:175`,
`memory_pool.rs:270`, `file_pool.rs:322` and `io_uring_pool.rs:222` all release
never-used segments; under crucible's current `try_reserve` bump site each of
those burns a generation for free, at a rate decoupled from segment lifecycles.
A 6-bit tag cannot afford that.

This bump site carries the tag's collision hardness. A future change moving it
would look locally harmless while silently draining that hardness, so it is
pinned by a full transition-table test.

### 5. The check

In `CacheKeyVerifier::verify`, after resolving the segment and **before any item
bytes are touched**:

```rust
if segment.incarnation() != item_loc.incarnation() {
    return false;
}
```

`metadata` is the first field of a 64-byte-aligned `SliceSegment`, and `verify`
already reads `capacity` and `pool_flags` from that line, so this should cost a
compare rather than a miss. The same check goes in `SinglePoolVerifier`,
`MultiPoolVerifier` (`cache/core/src/item_location.rs`) and the disk verifier.

### 6. GHOST stays unaliasable

`Location::GHOST` is all 44 bits set. Construction rejects `num_segments >
2^seg_bits - 1` so the top segment id is never issued and GHOST cannot be
aliased by construction.

### 7. Construction validates instead of aliasing

Pool construction computes the layout and fails with an error naming the limit
and pointing at `segment_size` and `align_bytes` as the levers. Current configs
top out at 100 GB (disk) and 4 GB (heap), so nothing in the tree breaks.

## Scope

Memory and disk pools change together — `MemoryPool`, `FilePool`, `IoUringPool`,
and all `ItemLocation::new` sites in `fifo_layer`, `ttl_layer` and `disk_layer`.

A half-tagged location space is worse than an untagged one: a verifier holding a
location cannot tell whether a zero tag means "untagged pool" or "incarnation 0",
so the check would have to skip, and could decay into a no-op with no test
noticing.

## Evidence

Every item below must be proven red by neutering the thing it tests.

1. **Deterministic recycle test.** Drive N drain/recycle/refill cycles against a
   held stale location; assert it is rejected every time, *and* that the
   rejection count is non-zero so the test cannot pass vacuously.
2. **Loom model.** Extend `loom_oracle` to model incarnations. It currently
   records that the oracle owns all 44 bits with no generation riding in them —
   which is exactly what blocked the cache-rs model port. Races a reader holding
   a stale location against a recycle.
3. **Bump-site transition table.** Assert `incarnation` advances on exactly
   `Locked -> Free` and `AwaitingRelease -> Free`, and not on
   `Reserved|Linking -> Free`.
4. **Layout property test.** Roundtrip every `(segment_size, align_bytes,
   heap_size)` combination through pack/unpack; assert construction rejects
   overflowing configs rather than aliasing them.

## Residuals

- **This does not fully close the crucible#88 data race.** It removes the false
  positive and shrinks the window, but a reader can pass the tag check and then
  be preempted before reading item bytes. Closing that requires the read to
  happen under a ref guard. Separate work; to be filed, not claimed here.
- **The tag is probabilistic.** Aliasing requires a thread to stall across 64
  lifecycles of one specific segment id. "Same offset" is not an independent
  coincidence — segments are append-only from a fixed start, so with uniform
  item sizes the n-th item of every incarnation lands at the same offset. A real,
  bounded residual: tracked, not hidden.
- **Read-path cost is unmeasured.** cache-rs#78 measured its tag check at
  +0.94 +/- 0.26 ns, though net versus its parent was indistinguishable from zero.
  Crucible's arithmetic differs (the shifts become layout-dependent loads rather
  than constants), so that figure does not transfer and must be measured here.
- **Layout-dependent shifts are a new hot-path load.** Today's accessors are
  constant shifts an optimiser can fold. They become loads from the layout
  descriptor. Keeping the descriptor in the same cache line as the pool pointer
  the verifier already reads is the mitigation; whether it is sufficient is an
  open measurement.
