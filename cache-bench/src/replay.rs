//! Trace-driven replay: record-paced rather than clock-paced.
//!
//! The synthetic workload in [`crate::worker`] runs for a duration and reports
//! an average over it. A replay runs until the trace ends, and the number that
//! matters — miss ratio — is read off a *window* of the trace rather than the
//! whole of it, because a run measured from a cold cache reports a value that
//! occurs nowhere in it.
//!
//! Replay is deliberately single-threaded. Concurrency changes the order
//! evictions interleave with reads, which changes which items are resident,
//! which changes the quantity being measured. Throughput belongs to a
//! different rig.
//!
//! See `docs/superpowers/specs/2026-09-18-s3fifo-main-pool-experiment-design.md`.

use crate::trace::{Op, TraceRecord};
use cache_core::Cache;
use std::time::Duration;

/// Tally of what a replay did. Reset at the warmup boundary so the measured
/// window is counted from zero rather than differenced from a cold prefix.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplayStats {
    /// GETs that found the key.
    pub hits: u64,
    /// GETs that did not.
    pub misses: u64,
    /// Writes issued, including inserts on miss.
    pub sets: u64,
    /// Writes the cache rejected.
    pub set_errors: u64,
    /// Breakdown of `set_errors` by `CacheError` cause, so a rejected replay
    /// can name why rather than guessing from the total alone.
    pub set_error_causes: SetErrorCauses,
    /// Deletes issued.
    pub deletes: u64,
    /// Records whose value exceeded the replay's value buffer.
    pub oversized: u64,
    /// Value bytes behind the GETs counted in `hits`.
    ///
    /// Taken from the trace record, not from the cache, so a hit and a miss
    /// on the same key contribute the same number and the figure is
    /// identical across engines. That is what makes it comparable; reading
    /// the length back from each cache would measure two implementations of
    /// the measurement alongside the two caches.
    pub hit_bytes: u64,
    /// Value bytes behind every GET counted here, hit or miss.
    pub get_bytes: u64,
}

/// Cause counts behind [`ReplayStats::set_errors`].
///
/// A hashtable-full rejection and a segment-exhaustion rejection trip the
/// same `set_errors` counter but call for opposite fixes -- one wants a
/// bigger table, the other wants fewer/smaller segments or a bigger heap.
/// Kept as counts rather than a single "last cause seen" so a dominant cause
/// can be told from noise from other variants.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SetErrorCauses {
    /// `CacheError::HashTableFull`: the hashtable is undersized for this
    /// trace and cache size.
    pub hashtable_full: u64,
    /// `CacheError::OutOfMemory`: eviction ran and still found no space --
    /// too few or too small segments for this heap size.
    pub out_of_memory: u64,
    /// `CacheError::ValueTooLong`: a value did not fit in one segment.
    pub value_too_long: u64,
    /// Any other `CacheError` variant. `store` only ever issues an upsert,
    /// so this should stay at zero in practice; it exists so a future
    /// variant is counted rather than silently dropped.
    pub other: u64,
}

impl ReplayStats {
    /// Miss ratio by value bytes, or `None` if no GET carried any.
    ///
    /// The companion to [`miss_ratio`](Self::miss_ratio), and the two answer
    /// different questions. Ranking retention by frequency alone favours
    /// large hot items; ranking by frequency-over-size favours small ones.
    /// Both raise one of these ratios at the other's expense, so reporting
    /// only the request-weighted figure scores a size-aware policy on the
    /// axis it optimises and a size-blind one on the axis it does not.
    ///
    /// `None` rather than 0.0 for the same reason `miss_ratio` uses it: a
    /// window with nothing in the denominator has no ratio, and rendering
    /// that as a perfect hit rate is how an empty window reads as a good
    /// result. Note that GETs for zero-length values count in
    /// [`miss_ratio`] but contribute nothing here, so the two denominators
    /// are deliberately not the same population.
    pub fn byte_miss_ratio(&self) -> Option<f64> {
        if self.get_bytes == 0 {
            return None;
        }
        Some((self.get_bytes - self.hit_bytes) as f64 / self.get_bytes as f64)
    }

    /// Miss ratio over the GETs counted here, or `None` if there were none.
    ///
    /// `None` rather than 0.0: a window with no GETs has no miss ratio, and
    /// rendering that as a perfect hit rate is how an empty window reads as a
    /// good result.
    pub fn miss_ratio(&self) -> Option<f64> {
        let gets = self.hits + self.misses;
        if gets == 0 {
            return None;
        }
        Some(self.misses as f64 / gets as f64)
    }
}

/// Format a trace key id into `buf` as zero-padded decimal.
///
/// Matches the convention in rpc-perf and s3-replay so a key drawn from the
/// same trace has the same length here as there; item size depends on it, and
/// a different convention would silently change effective cache capacity.
pub fn write_key(buf: &mut Vec<u8>, key_id: u64, key_len: u16) {
    use std::io::Write;
    buf.clear();
    // `width` is a minimum, so an id too wide for the trace's key length keeps
    // all its digits rather than colliding with a different object.
    write!(buf, "{:0width$}", key_id, width = key_len as usize).expect("writing to a Vec");
}

/// Apply one trace record to the cache.
///
/// `insert_on_miss` is set for `oracleGeneral` traces, which carry no writes:
/// every record is a GET and the cache is populated by the misses. Twitter
/// traces carry their own op codes and must not have inserts synthesized for
/// them, or the write mix under test becomes a property of the harness.
pub fn apply_record<C: Cache>(
    cache: &C,
    record: &TraceRecord,
    key_buf: &mut Vec<u8>,
    value_pool: &[u8],
    insert_on_miss: bool,
    stats: &mut ReplayStats,
) {
    write_key(key_buf, record.key_id, record.key_len);

    let value_len = record.value_len as usize;
    let needs_value = !matches!(
        record.op,
        Op::Get | Op::Gets | Op::Delete | Op::Incr | Op::Decr
    );
    if (needs_value || insert_on_miss) && value_len > value_pool.len() {
        // Storing a short value would understate the trace's memory footprint
        // and therefore overstate how many items fit — the denominator of
        // everything this rig reports.
        stats.oversized += 1;
        return;
    }

    match record.op {
        Op::Get | Op::Gets => {
            stats.get_bytes += record.value_len as u64;
            if cache.with_value(key_buf, |_| ()).is_some() {
                stats.hits += 1;
                stats.hit_bytes += record.value_len as u64;
            } else {
                stats.misses += 1;
                if insert_on_miss {
                    store(cache, key_buf, &value_pool[..value_len], record, stats);
                }
            }
        }
        Op::Delete => {
            cache.delete(key_buf);
            stats.deletes += 1;
        }
        Op::Incr | Op::Decr => {
            // A counter operation reads, modifies and writes back in place.
            // It is replayed as a lookup for three reasons: on a miss the
            // real operation fails rather than creating the key, so writing
            // here would invent residency the workload never had; the item's
            // size does not change, so it moves nothing between segments;
            // and the trace records a value size but not the increment
            // amount, so the arithmetic cannot be reproduced anyway.
            //
            // It goes through the same read path as `Get` rather than
            // `contains` so that it bumps the frequency counter -- an access
            // is an access, and eviction policy depends on that.
            if cache.with_value(key_buf, |_| ()).is_some() {
                stats.hits += 1;
            } else {
                stats.misses += 1;
            }
        }
        Op::Set | Op::Add | Op::Cas | Op::Replace | Op::Append | Op::Prepend => {
            store(cache, key_buf, &value_pool[..value_len], record, stats);
        }
    }
}

/// Issue a write and tally its outcome.
fn store<C: Cache>(
    cache: &C,
    key: &[u8],
    value: &[u8],
    record: &TraceRecord,
    stats: &mut ReplayStats,
) {
    match cache.set(key, value, record_ttl(record)) {
        Ok(()) => stats.sets += 1,
        Err(e) => record_set_error(stats, e),
    }
}

/// Tally a rejected write against its `set_errors` total and its cause.
fn record_set_error(stats: &mut ReplayStats, err: cache_core::CacheError) {
    use cache_core::CacheError;

    stats.set_errors += 1;
    match err {
        CacheError::HashTableFull => stats.set_error_causes.hashtable_full += 1,
        CacheError::OutOfMemory => stats.set_error_causes.out_of_memory += 1,
        CacheError::ValueTooLong => stats.set_error_causes.value_too_long += 1,
        _ => stats.set_error_causes.other += 1,
    }
}

/// How to pace and window a replay.
pub struct ReplayOptions {
    /// Records to apply before the measured window opens.
    pub warmup_records: u64,
    /// Cap on records applied after warmup; `None` replays to end of trace.
    pub max_records: Option<u64>,
    /// Records per reported interval inside the measured window.
    pub report_interval_records: u64,
    /// Largest value the replay will store.
    pub max_value_bytes: usize,
    /// Synthesize an insert on every GET miss (`oracleGeneral` traces).
    pub insert_on_miss: bool,
    /// Record which value sizes survived, by probing every key written.
    ///
    /// Off by default: it holds a key-to-size map for the whole run and
    /// probes the cache once per distinct key afterwards, which is memory
    /// and time a normal measurement should not pay.
    ///
    /// Measured from here rather than inside either engine on purpose. The
    /// replay already knows every key's size, and `contains` is on the
    /// `Cache` trait, so one piece of code measures both engines and the
    /// results are comparable by construction rather than by reconciling
    /// two engines' internal gauges -- which is how the last three
    /// comparisons went wrong.
    pub retained_sizes: bool,
}

/// Retention by value size: how many distinct keys of each size were
/// written, and how many were still resident at the end.
///
/// Buckets are powers of two on the value length. The question it answers
/// is whether two engines holding different item counts are keeping
/// different size distributions, which a total count cannot show.
#[derive(Debug, Default, Clone)]
pub struct RetainedSizes {
    /// `(bucket_low_bytes, written, retained)`, ascending.
    pub buckets: Vec<(u32, u64, u64)>,
}

impl RetainedSizes {
    fn bucket_of(value_len: u32) -> u32 {
        if value_len == 0 {
            0
        } else {
            1u32 << (31 - value_len.leading_zeros())
        }
    }
}

/// What a replay produced.
#[derive(Debug)]
pub struct ReplayOutcome {
    /// Tally over the warmup prefix, kept only to prove warmup did work.
    pub warmup: ReplayStats,
    /// Tally over the measured window. This is the reported result.
    pub measured: ReplayStats,
    /// Records consumed from the trace, warmup included.
    pub records_read: u64,
    /// Per-operation latency over the measured window, reads and writes
    /// separately.
    ///
    /// Split because merge eviction runs *inline* in the write path --
    /// `TieredCache::set` calls `ensure_space` as its first statement -- so a
    /// reclamation pass is a stall on whichever `set` happened to trigger it.
    /// Pooling reads and writes would bury that: with a 10% write mix, the
    /// write p99 lands around the pooled p99.9 and the shape is lost.
    pub read_latency: Option<metriken::histogram::Histogram>,
    /// See [`Self::read_latency`].
    pub write_latency: Option<metriken::histogram::Histogram>,
    /// Miss ratio per interval across the measured window.
    ///
    /// Reported so the tail can be checked for flatness: a window still
    /// trending at its end was too short, and its average is a value that
    /// occurs nowhere in the run.
    pub intervals: Vec<f64>,
    /// Retention by value size, when `ReplayOptions::retained_sizes` asked
    /// for it.
    pub retained_sizes: Option<RetainedSizes>,
}

/// Replay a trace against a cache, splitting warmup from the measured window.
pub fn run_replay<C: Cache>(
    cache: &C,
    reader: &mut crate::trace::TraceReader,
    opts: &ReplayOptions,
) -> std::io::Result<ReplayOutcome> {
    let mut warmup = ReplayStats::default();
    let mut measured = ReplayStats::default();
    let mut intervals = Vec::new();
    let mut interval = ReplayStats::default();

    let mut key_buf = Vec::with_capacity(64);
    // key_id -> value length, for the retention probe. Only distinct keys,
    // and only when asked.
    let mut written_sizes: std::collections::HashMap<u64, (u16, u32)> = if opts.retained_sizes {
        std::collections::HashMap::with_capacity(1 << 20)
    } else {
        std::collections::HashMap::new()
    };
    let value_pool = vec![0xA5u8; opts.max_value_bytes];
    let mut measured_records = 0u64;

    // 7/64 matches the histograms in `metrics`: ~1% relative error, and a
    // range that reaches seconds, which an inline multi-segment merge pass
    // can plausibly need on a slow core.
    let read_hist = metriken::AtomicHistogram::new(7, 64);
    let write_hist = metriken::AtomicHistogram::new(7, 64);

    while let Some(result) = reader.next_record() {
        // A read error ends the replay rather than truncating it silently: a
        // short trace produces a complete-looking run over a fraction of the
        // workload, which is the failure this whole rig exists to avoid.
        let record = result?;

        // Cache time follows trace time, one second for one second, so a TTL
        // expires after as many records as it did in production. Otherwise
        // the cache expires against the wall clock while this loop consumes
        // hours of recorded time in seconds, and a TTL shorter than the run
        // never fires at all.
        //
        // Set per record rather than per interval: expiry is checked on
        // every read, so a clock that lagged the record being applied would
        // expire items late by however far it lagged.
        // Both engines, or neither. A comparison that advanced one side's
        // clock and left the other on wall time would penalise the engine
        // that honours expiry for the hits it correctly discards, which is
        // the same bias as before with the sign flipped.
        #[cfg(feature = "virtual-clock")]
        if let Some(secs) = record.timestamp_secs {
            cache_core::clock::set_virtual_now(secs);
            #[cfg(feature = "cache-rs")]
            cache_rs::clock::set_virtual_now(secs);
        }

        let in_warmup = reader.records_read() <= opts.warmup_records;
        let stats = if in_warmup {
            &mut warmup
        } else {
            &mut measured
        };

        // Snapshot and diff rather than re-deriving what the record did: the
        // interval tally and the window tally then agree by construction, and
        // two of our own numbers disagreeing is the failure this rig exists
        // to surface rather than reproduce.
        let before = *stats;

        // Only the measured window is timed; `Instant::now` twice per record
        // through a 5M-record warmup is pure overhead that also perturbs the
        // thing being measured.
        let started = if in_warmup {
            None
        } else {
            Some(std::time::Instant::now())
        };

        apply_record(
            cache,
            &record,
            &mut key_buf,
            &value_pool,
            opts.insert_on_miss,
            stats,
        );

        // Record the size a key was stored at. Last write wins, which is
        // what the cache holds too.
        if opts.retained_sizes && !matches!(record.op, Op::Get | Op::Gets | Op::Delete) {
            written_sizes.insert(record.key_id, (record.key_len, record.value_len));
        }

        if let Some(started) = started {
            let elapsed = started.elapsed().as_nanos() as u64;
            // A GET that misses under `insert_on_miss` performs a write, so it
            // is classified by what it did rather than by its op code.
            let wrote = measured.sets + measured.set_errors > before.sets + before.set_errors;
            let hist = if wrote { &write_hist } else { &read_hist };
            let _ = hist.increment(elapsed);
        }

        if in_warmup {
            continue;
        }

        interval.hits += measured.hits - before.hits;
        interval.misses += measured.misses - before.misses;
        measured_records += 1;

        if opts.report_interval_records > 0
            && measured_records.is_multiple_of(opts.report_interval_records)
            && let Some(ratio) = interval.miss_ratio()
        {
            intervals.push(ratio);
            interval = ReplayStats::default();
        }

        if opts.max_records.is_some_and(|max| measured_records >= max) {
            break;
        }
    }

    // A partial trailing interval still carries information about the tail,
    // which is exactly where flatness is judged.
    if let Some(ratio) = interval.miss_ratio() {
        intervals.push(ratio);
    }

    // Probe after the window closes, so the probing itself cannot change
    // what is resident during measurement. `contains` is the non-bumping
    // lookup, so it does not disturb frequencies either.
    let retained_sizes = if opts.retained_sizes {
        let mut tally: std::collections::BTreeMap<u32, (u64, u64)> =
            std::collections::BTreeMap::new();
        for (&key_id, &(key_len, value_len)) in &written_sizes {
            write_key(&mut key_buf, key_id, key_len);
            let entry = tally
                .entry(RetainedSizes::bucket_of(value_len))
                .or_insert((0, 0));
            entry.0 += 1;
            if cache.contains(&key_buf) {
                entry.1 += 1;
            }
        }
        Some(RetainedSizes {
            buckets: tally.into_iter().map(|(b, (w, r))| (b, w, r)).collect(),
        })
    } else {
        None
    };

    Ok(ReplayOutcome {
        warmup,
        measured,
        records_read: reader.records_read(),
        read_latency: read_hist.load(),
        write_latency: write_hist.load(),
        intervals,
        retained_sizes,
    })
}

/// Whether a completed replay measured what it claims to.
///
/// A sweep point above the trace's working set never evicts, so every policy
/// scores identically there and the point carries no information about
/// eviction. Reporting it anyway is how a flat region of a sweep reads as
/// "the policies are equivalent". Rejected loudly rather than averaged in.
pub fn envelope_verdict(
    measured: &ReplayStats,
    internal: Option<cache_core::CacheInternalStats>,
) -> Result<(), String> {
    if measured.miss_ratio().is_none() {
        return Err("measured window contains no GETs; there is no miss ratio to report".into());
    }

    // A refused write is not an eviction-policy outcome: it changes what is
    // resident for a reason orthogonal to the policy under test, and the
    // shift is the same size as the effects being looked for. Sizing is also
    // what makes the replay deterministic: with no refusals, hash placement
    // and allocation stop mattering and repeated runs agree exactly.
    //
    // `set_errors` alone does not say *why* a write was refused, and an
    // undersized hashtable and too few/small segments trip the same counter
    // for opposite reasons -- see the issue this check was split for: 100%
    // of sets failed on `OutOfMemory` at hashtable_power 18 *and* 20, and
    // raising the power changed nothing because segments, not the table,
    // were the real constraint. Naming a cause the counts do not clearly
    // support would be exactly that mistake in different words, so a mixed
    // or unmapped mix reports the breakdown rather than guessing.
    if measured.set_errors > 0 {
        let causes = &measured.set_error_causes;
        let total = measured.sets + measured.set_errors;
        // "Dominant" means a strict majority of the errors, not merely the
        // largest bucket: a 450/450 split between two causes is not evidence
        // for either one.
        return Err(if causes.hashtable_full * 2 > measured.set_errors {
            format!(
                "the hashtable refused {} of {} writes: it is undersized for this \
                 trace and cache size, and table pressure will read as a policy \
                 effect (raise hashtable_power until set errors reach zero)",
                measured.set_errors, total
            )
        } else if causes.out_of_memory * 2 > measured.set_errors {
            format!(
                "{} of {} writes were refused for lack of memory: eviction ran \
                 and still found no space, which means too few or too small \
                 segments for this heap, not an undersized hashtable (use a \
                 smaller segment_size or a larger heap; raising hashtable_power \
                 will not help)",
                measured.set_errors, total
            )
        } else if causes.value_too_long * 2 > measured.set_errors {
            format!(
                "{} of {} writes were refused as too large for a segment: this \
                 workload's values do not fit at the configured segment_size \
                 (raise segment_size; raising hashtable_power will not help)",
                measured.set_errors, total
            )
        } else {
            format!(
                "{} of {} writes were refused with no single dominant cause \
                 ({} hashtable-full, {} out-of-memory, {} value-too-long, {} \
                 other): the mix must be understood before trusting this run's \
                 miss ratio",
                measured.set_errors,
                total,
                causes.hashtable_full,
                causes.out_of_memory,
                causes.value_too_long,
                causes.other
            )
        });
    }

    // `evictions` counts only a layer with no demotion target -- the main
    // cache in the two-layer RAM topology. Layer 0's promotions land in
    // `demotions` and keep happening above the working set, so requiring
    // "evicted OR demoted" accepts a point where the main pool never
    // reclaimed anything and no eviction policy decision was ever made. That
    // is exactly the saturated region of a sweep, where every policy scores
    // identically and the agreement carries no information.
    //
    // With a disk tier, layer 1 demotes rather than evicts and this would be
    // the wrong test; the experiment targets the two-layer configuration and
    // the check is scoped to it.

    // Eviction *pass* counts are not comparable across engines or policies:
    // a pass reclaims a variable number of segments (crucible's chain is 4,
    // cache-rs's merge consolidates up to 8), so "5 eviction passes" and
    // "194 eviction passes" are not an apples-to-apples shortfall and a
    // pass-count threshold would reject legitimate runs (crucible#158,
    // cachers/merge at 64MB: 5 passes across 10M records, genuinely
    // saturated). What the check actually needs to know is whether the
    // cache ever reached capacity -- test that directly instead of proxying
    // it through pass counts.
    //
    // The bar is deliberately BOTH absolute and relative, because a
    // saturated cache's leftover free segments are an absolute constant, not
    // a fraction of the heap. Measured on the x86 host (sweep 01a0bf67):
    // segment/s3fifo holds 4 free at 32, 48 and 64 total; segment/fifo holds
    // 2 at every size; cachers/fifo holds 0. These are spare reserves --
    // crucible's merge spare and layer rounding, and cache-rs's
    // `segment_free` gauge, whose own description says it "includes the
    // held-back spare reserve (not available to normal writes)".
    //
    // A percentage-only bar therefore tightens as the heap shrinks and
    // eventually fires on the reserve alone: 4 of 32 is 12.5% while the same
    // reserve at 4 of 64 is 6.2%, so one saturated cache passes and another
    // fails on segment count rather than on anything about the run. A
    // 10%-only bar did exactly that to 6 arms of a 120-arm sweep.
    //
    // Requiring both bounds separates the two regimes cleanly on the
    // measured data: reserves run to 4-5 segments and at most 12.5%, while a
    // cache that genuinely never filled sits at 34-68% and hundreds of
    // segments (a 128MB cache on this trace left 175 and 350 of 512 free).
    //
    // `total_segments == 0` means the engine did not report segment counts
    // at all (an unpopulated `CacheInternalStats::default()`); that is
    // un-checkable, not evidence of "never filled", so it is skipped here
    // rather than treated as a divide-by-zero or a false rejection.
    const FREE_SEGMENT_REJECT_PCT: u64 = 25;
    /// Free segments below this are a reserve at any heap size, never headroom.
    const FREE_SEGMENT_SLACK: u64 = 8;
    if let Some(stats) = &internal
        && stats.total_segments > 0
        && stats.free_segments > FREE_SEGMENT_SLACK
        && stats.free_segments * 100 > stats.total_segments * FREE_SEGMENT_REJECT_PCT
    {
        return Err(format!(
            "the cache never filled: {} of {} segments ({:.1}%) were still \
             free at the end of the measured window, over the {}% threshold: \
             this point largely measures allocation rather than the eviction \
             policy under test (shrink the cache or lengthen the trace)",
            stats.free_segments,
            stats.total_segments,
            stats.free_segments as f64 / stats.total_segments as f64 * 100.0,
            FREE_SEGMENT_REJECT_PCT,
        ));
    }

    // Kept alongside the fill check above rather than replaced by it: a
    // full-and-never-evicted cache is still suspect (something other than
    // normal capacity pressure kept the eviction path from ever running),
    // and the two checks answer different questions.
    match internal {
        Some(stats) if stats.evictions == 0 => Err(format!(
            "the main layer never evicted in the measured window \
             ({} demotions, {} sets, {} set errors): this cache size is at or \
             above the trace's working set, so the point measures allocation \
             rather than eviction",
            stats.demotions, measured.sets, measured.set_errors
        )),
        _ => Ok(()),
    }
}

/// Spread of the measured window's tail, or `None` when the window is too
/// short to judge.
///
/// A window still trending at its end was too short, and its average is a
/// value that occurs nowhere in the run. But the check needs enough intervals
/// in the tail to say anything: with three intervals the last third is one
/// interval, whose spread is zero by construction, and a printed 0.0000 reads
/// as "perfectly flat" while measuring nothing at all.
pub fn tail_spread(intervals: &[f64]) -> Option<f64> {
    /// Fewest intervals the tail must hold for its spread to mean anything.
    const MIN_TAIL: usize = 3;

    let tail_len = intervals.len() / 3;
    if tail_len < MIN_TAIL {
        return None;
    }

    let tail = &intervals[intervals.len() - tail_len..];
    let lo = tail.iter().copied().fold(f64::INFINITY, f64::min);
    let hi = tail.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    Some(hi - lo)
}

/// TTL to hand the cache for a record, `None` for "no expiry".
fn record_ttl(record: &TraceRecord) -> Option<Duration> {
    if record.ttl_secs == 0 {
        None
    } else {
        Some(Duration::from_secs(record.ttl_secs as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::ORACLE_GENERAL_KEY_LEN;

    const MB: usize = 1024 * 1024;

    fn small_cache() -> segcache::SegCache {
        segcache::SegCache::builder()
            .heap_size(8 * MB)
            .segment_size(MB)
            .hashtable_power(12)
            .build()
            .expect("failed to build test cache")
    }

    #[test]
    fn a_counter_operation_is_a_lookup_not_an_insert() {
        // incr/decr read-modify-write an existing counter. On a miss the
        // real operation fails rather than creating the key, so replaying it
        // as a write would invent residency the workload never had.
        let cache = small_cache();
        let mut stats = ReplayStats::default();
        let mut key = Vec::new();
        let pool = vec![0u8; 4096];

        apply_record(
            &cache,
            &record(1, Op::Incr, 8, 0),
            &mut key,
            &pool,
            false,
            &mut stats,
        );
        assert_eq!(stats.misses, 1, "a counter op on an absent key is a miss");
        assert_eq!(stats.sets, 0, "it must not insert on miss");

        apply_record(
            &cache,
            &record(2, Op::Set, 64, 0),
            &mut key,
            &pool,
            false,
            &mut stats,
        );
        apply_record(
            &cache,
            &record(2, Op::Incr, 8, 0),
            &mut key,
            &pool,
            false,
            &mut stats,
        );
        assert_eq!(stats.hits, 1, "a counter op on a present key is a hit");
        assert_eq!(stats.sets, 1, "the counter op must not have written again");
    }

    #[test]
    fn a_trace_carrying_a_counter_operation_replays_to_the_end() {
        // The regression this exists for: before incr was decoded, a trace
        // using counters aborted the run with "unknown op code 10" and
        // reported nothing at all.
        let mut bytes = twitter_bytes(1, 64, Op::Set).to_vec();
        bytes.extend_from_slice(&twitter_bytes(1, 8, Op::Incr));
        bytes.extend_from_slice(&twitter_bytes(1, 64, Op::Get));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("counters.bin");
        std::fs::write(&path, &bytes).unwrap();

        let mut reader =
            crate::trace::TraceReader::open(&path, crate::trace::TraceFormat::Twitter).unwrap();
        let mut n = 0;
        while let Some(r) = reader.next_record() {
            r.expect("a counter operation must not abort the replay");
            n += 1;
        }
        assert_eq!(n, 3, "all three records should decode");
    }

    fn record(key_id: u64, op: Op, value_len: u32, ttl_secs: u32) -> TraceRecord {
        TraceRecord {
            key_id,
            key_len: ORACLE_GENERAL_KEY_LEN,
            value_len,
            op,
            ttl_secs,
            timestamp_secs: None,
        }
    }

    fn twitter_bytes(key_id: u64, value_len: u32, op: Op) -> [u8; 20] {
        let mut out = [0u8; 20];
        out[4..12].copy_from_slice(&key_id.to_le_bytes());
        let kv_packed = ((ORACLE_GENERAL_KEY_LEN as u32) << 22) | (value_len & 0x003F_FFFF);
        out[12..16].copy_from_slice(&kv_packed.to_le_bytes());
        out[16..20].copy_from_slice(&((op as u32) << 24).to_le_bytes());
        out
    }

    /// Like `twitter_bytes`, but carrying the timestamp and TTL fields the
    /// clock work depends on. Kept separate so the existing callers keep
    /// stating only what they care about.
    fn twitter_bytes_at(
        key_id: u64,
        value_len: u32,
        op: Op,
        ttl_secs: u32,
        timestamp_secs: u32,
    ) -> [u8; 20] {
        let mut out = twitter_bytes(key_id, value_len, op);
        out[0..4].copy_from_slice(&timestamp_secs.to_le_bytes());
        out[16..20]
            .copy_from_slice(&(((op as u32) << 24) | (ttl_secs & 0x00FF_FFFF)).to_le_bytes());
        out
    }

    #[test]
    fn a_record_carries_its_timestamp_and_ttl() {
        let bytes = twitter_bytes_at(7, 64, Op::Set, 300, 1_700_000_000);
        let record = crate::trace::TraceRecord::from_twitter_bytes(&bytes).expect("decode");
        assert_eq!(record.timestamp_secs, Some(1_700_000_000), "timestamp");
        assert_eq!(record.ttl_secs, 300, "ttl");
        assert_eq!(record.key_id, 7, "key id");
        assert_eq!(record.op, Op::Set, "op");
    }

    /// The replay must move the cache's clock to the record it is applying.
    ///
    /// This is the whole point of carrying the timestamp: with the clock
    /// left on wall time, a 60s TTL in a trace spanning hours outlives the
    /// entire run, and expiry -- the mechanism TTL-bucketed segments exist
    /// to exploit -- is measured as if it never happened.
    #[test]
    #[cfg(feature = "virtual-clock")]
    fn replaying_advances_the_cache_clock_to_trace_time() {
        let mut bytes = Vec::new();
        for (i, ts) in [1_700_000_000u32, 1_700_000_060, 1_700_000_120]
            .iter()
            .enumerate()
        {
            bytes.extend_from_slice(&twitter_bytes_at(i as u64, 64, Op::Set, 300, *ts));
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("timestamps.bin");
        std::fs::write(&path, &bytes).unwrap();

        let mut reader =
            crate::trace::TraceReader::open(&path, crate::trace::TraceFormat::Twitter).unwrap();
        let cache = small_cache();
        run_replay(&cache, &mut reader, &opts(0, 1000)).expect("replay");

        assert_eq!(
            cache_core::clock::virtual_now(),
            Some(1_700_000_120),
            "clock should sit at the last record's timestamp"
        );
        cache_core::clock::clear_virtual_now();
    }

    /// Both engines' clocks must advance, not just crucible's.
    ///
    /// This is the fairness property the whole feature exists for. If only
    /// one side follows the trace, that side expires items and loses the
    /// hits it correctly discards while the other keeps serving them -- the
    /// same bias the wall clock produced, with the sign flipped. A silent
    /// regression here would look like a clean result.
    #[test]
    #[cfg(all(feature = "virtual-clock", feature = "cache-rs"))]
    fn replaying_advances_both_engines_clocks() {
        let mut bytes = Vec::new();
        for (i, ts) in [1_585_565_987u32, 1_585_566_047].iter().enumerate() {
            bytes.extend_from_slice(&twitter_bytes_at(i as u64, 64, Op::Set, 300, *ts));
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("both.bin");
        std::fs::write(&path, &bytes).unwrap();

        cache_core::clock::clear_virtual_now();
        cache_rs::clock::clear_virtual_now();

        let mut reader =
            crate::trace::TraceReader::open(&path, crate::trace::TraceFormat::Twitter).unwrap();
        run_replay(&small_cache(), &mut reader, &opts(0, 1000)).expect("replay");

        assert_eq!(
            cache_core::clock::virtual_now(),
            Some(1_585_566_047),
            "crucible's clock should sit at the last record"
        );
        // cache-rs anchors trace seconds onto an opaque monotonic instant,
        // so the readable assertion is that it moved, and moved by the same
        // 60 seconds the trace did.
        let rs = cache_rs::clock::virtual_now().expect("cache-rs clock should be driven too");
        cache_rs::clock::set_virtual_now(1_585_565_987);
        let rs_start = cache_rs::clock::virtual_now().expect("set");
        assert_eq!(
            rs.duration_since(rs_start).as_secs(),
            60,
            "cache-rs should have advanced by the trace's own 60 seconds"
        );

        cache_core::clock::clear_virtual_now();
        cache_rs::clock::clear_virtual_now();
    }

    /// The histogram must separate size classes and measure survival
    /// Byte miss ratio must weight by value size, not just count GETs.
    ///
    /// Built so the two ratios must disagree: the same number of GETs hit
    /// and miss, so the request miss ratio is exactly 0.5 whatever the
    /// sizes are, while every hit is a large value and every miss a small
    /// one. An implementation that counted GETs, or that added a constant
    /// per GET, would report 0.5 here too.
    #[test]
    fn byte_miss_ratio_weights_gets_by_value_size() {
        const BIG: u32 = 8192;
        const SMALL: u32 = 64;
        let mut bytes = Vec::new();
        // Only the large keys are ever stored, so every GET for a small key
        // is a guaranteed miss and every GET for a large key a guaranteed
        // hit -- no dependence on the cache's policy.
        for i in 0..50u64 {
            bytes.extend_from_slice(&twitter_bytes(i, BIG, Op::Set));
        }
        for i in 0..50u64 {
            bytes.extend_from_slice(&twitter_bytes(i, BIG, Op::Get));
            bytes.extend_from_slice(&twitter_bytes(5000 + i, SMALL, Op::Get));
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bytes.bin");
        std::fs::write(&path, &bytes).unwrap();

        let mut reader =
            crate::trace::TraceReader::open(&path, crate::trace::TraceFormat::Twitter).unwrap();
        let mut o = opts(0, 1000);
        o.insert_on_miss = false;
        o.max_value_bytes = 64 * 1024;
        let outcome = run_replay(&small_cache(), &mut reader, &o).expect("replay");
        let m = &outcome.measured;

        assert_eq!(m.hits, 50, "every large key was stored, so every GET hits");
        assert_eq!(m.misses, 50, "no small key was ever stored");
        assert_eq!(
            m.miss_ratio(),
            Some(0.5),
            "by request count the window is exactly half misses"
        );

        let byte_miss = m.byte_miss_ratio().expect("GETs carried bytes");
        let expected = (50.0 * SMALL as f64) / (50.0 * SMALL as f64 + 50.0 * BIG as f64);
        assert!(
            (byte_miss - expected).abs() < 1e-9,
            "byte miss ratio must be miss bytes over GET bytes: {byte_miss} against {expected}"
        );
        assert!(
            byte_miss < 0.5,
            "the misses are the small values, so by bytes the window must \
             look far better than by requests: {byte_miss} against 0.5"
        );
        assert_eq!(
            m.get_bytes,
            50 * (BIG as u64 + SMALL as u64),
            "every GET contributes its trace value length"
        );
    }

    /// within each.
    ///
    /// Asserts the instrument, not a policy outcome. An earlier version
    /// asserted that small values survive better than large ones; they do
    /// not, at least not here -- eviction is by whole segment and a segment
    /// holds a mix, so both classes survived at 6-7%. That is the kind of
    /// claim this instrument exists to test, so encoding it as a test would
    /// have been assuming the answer.
    #[test]
    fn retained_sizes_reports_survival_per_size_class() {
        // Interleaved, not one class then the other. Writing all the small
        // keys first and then 25 MiB of large ones into an 8 MiB cache
        // evicts the small ones by recency, and the result measures write
        // order rather than size -- which is what the first version of this
        // test did, and it read as small keys surviving *worse*.
        let mut bytes = Vec::new();
        for i in 0..100u64 {
            bytes.extend_from_slice(&twitter_bytes(i, 64, Op::Set));
            bytes.extend_from_slice(&twitter_bytes(1000 + i, 256 * 1024, Op::Set));
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sizes.bin");
        std::fs::write(&path, &bytes).unwrap();

        let mut reader =
            crate::trace::TraceReader::open(&path, crate::trace::TraceFormat::Twitter).unwrap();
        let mut o = opts(0, 1000);
        o.retained_sizes = true;
        o.max_value_bytes = 512 * 1024;
        let outcome = run_replay(&small_cache(), &mut reader, &o).expect("replay");

        let hist = outcome.retained_sizes.expect("asked for it");
        let small: u64 = hist
            .buckets
            .iter()
            .filter(|(b, _, _)| *b < 1024)
            .map(|(_, _, r)| r)
            .sum();
        let small_w: u64 = hist
            .buckets
            .iter()
            .filter(|(b, _, _)| *b < 1024)
            .map(|(_, w, _)| w)
            .sum();
        let large: u64 = hist
            .buckets
            .iter()
            .filter(|(b, _, _)| *b >= 1024)
            .map(|(_, _, r)| r)
            .sum();
        let large_w: u64 = hist
            .buckets
            .iter()
            .filter(|(b, _, _)| *b >= 1024)
            .map(|(_, w, _)| w)
            .sum();

        assert_eq!(small_w, 100, "100 small keys written, bucketed under 1 KiB");
        assert_eq!(
            large_w, 100,
            "100 large keys written, bucketed at or above 1 KiB"
        );
        assert!(
            hist.buckets.len() >= 2,
            "two size classes were written, so both must appear: {:?}",
            hist.buckets
        );
        // Eviction happened, and the probe saw it rather than echoing the
        // written counts back.
        assert!(
            small + large < small_w + large_w,
            "25 MiB into an 8 MiB cache must evict something: \
             {small}/{small_w} small, {large}/{large_w} large"
        );
        for &(bucket, written, retained) in &hist.buckets {
            assert!(
                retained <= written,
                "bucket {bucket}: retained {retained} exceeds written {written}"
            );
        }
    }

    fn opts(warmup: u64, interval: u64) -> ReplayOptions {
        ReplayOptions {
            warmup_records: warmup,
            max_records: None,
            report_interval_records: interval,
            max_value_bytes: 4096,
            retained_sizes: false,
            insert_on_miss: true,
        }
    }

    fn reader_over(bytes: Vec<u8>) -> crate::trace::TraceReader {
        crate::trace::TraceReader::new(
            Box::new(std::io::Cursor::new(bytes)),
            crate::trace::TraceFormat::Twitter,
        )
    }

    #[test]
    fn warmup_records_are_excluded_from_the_measured_window() {
        // Four GETs of one key: the first misses and inserts, the rest hit.
        // With two records of warmup the measured window must see only hits.
        let mut bytes = Vec::new();
        for _ in 0..4 {
            bytes.extend_from_slice(&twitter_bytes(1, 64, Op::Get));
        }
        let cache = small_cache();
        let mut reader = reader_over(bytes);

        let out = run_replay(&cache, &mut reader, &opts(2, 1)).unwrap();

        assert_eq!(out.warmup.misses, 1, "the cold miss belongs to warmup");
        assert_eq!(out.warmup.hits, 1);
        assert_eq!(out.measured.misses, 0);
        assert_eq!(out.measured.hits, 2);
        assert_eq!(out.measured.miss_ratio(), Some(0.0));
        assert_eq!(out.records_read, 4);
    }

    #[test]
    fn the_measured_window_reports_one_miss_ratio_per_interval() {
        let mut bytes = Vec::new();
        for _ in 0..6 {
            bytes.extend_from_slice(&twitter_bytes(1, 64, Op::Get));
        }
        let cache = small_cache();
        let mut reader = reader_over(bytes);

        let out = run_replay(&cache, &mut reader, &opts(0, 2)).unwrap();

        assert_eq!(out.intervals.len(), 3, "6 records at 2 per interval");
        assert_eq!(out.intervals[0], 0.5, "first interval: one miss, one hit");
        assert_eq!(out.intervals[2], 0.0);
    }

    #[test]
    fn max_records_stops_the_window_before_the_trace_ends() {
        let mut bytes = Vec::new();
        for id in 0..10u64 {
            bytes.extend_from_slice(&twitter_bytes(id, 64, Op::Get));
        }
        let cache = small_cache();
        let mut reader = reader_over(bytes);

        let mut o = opts(0, 100);
        o.max_records = Some(4);
        let out = run_replay(&cache, &mut reader, &o).unwrap();

        assert_eq!(out.measured.misses, 4);
        assert_eq!(out.records_read, 4);
    }

    #[test]
    fn a_truncated_trace_fails_the_replay_rather_than_shortening_it() {
        let mut bytes = twitter_bytes(1, 64, Op::Get).to_vec();
        bytes.extend_from_slice(&[0u8; 5]);
        let cache = small_cache();
        let mut reader = reader_over(bytes);

        let err = run_replay(&cache, &mut reader, &opts(0, 100))
            .expect_err("a truncated trace must not read as a completed run");

        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    fn stats_with_gets() -> ReplayStats {
        ReplayStats {
            hits: 90,
            misses: 10,
            ..Default::default()
        }
    }

    fn internal(evictions: u64) -> cache_core::CacheInternalStats {
        cache_core::CacheInternalStats {
            demotions: 0,
            evictions,
            demotion_failures: 0,
            ..Default::default()
        }
    }

    /// Stats with an explicit free/total segment split, for the fill check.
    fn internal_with_fill(
        evictions: u64,
        free_segments: u64,
        total_segments: u64,
    ) -> cache_core::CacheInternalStats {
        cache_core::CacheInternalStats {
            demotions: 0,
            evictions,
            demotion_failures: 0,
            free_segments,
            total_segments,
            ..Default::default()
        }
    }

    #[test]
    fn a_run_that_never_filled_is_rejected_for_under_filling_not_eviction_counts() {
        // 90 of 100 segments free: nowhere near capacity, even with a
        // non-zero eviction count (e.g. a transient early-trace burst).
        let internal = internal_with_fill(50, 90, 100);

        let msg = envelope_verdict(&stats_with_gets(), Some(internal))
            .expect_err("a cache sitting at 90% free never reached capacity");
        assert!(
            msg.contains("fill") || msg.contains("free"),
            "the rejection must name under-filling, not eviction counts: {msg}"
        );
        assert!(
            !msg.contains("evicted in the measured window"),
            "must not reuse the eviction-pass-count message for this case: {msg}"
        );
    }

    #[test]
    fn a_saturated_cache_with_a_small_spare_reserve_is_accepted_despite_a_low_eviction_pass_count()
    {
        // The cachers/merge case from crucible#158: a 64MB cache that
        // cleared only 5 eviction passes across 10M records (against 194 for
        // segment/merge) but genuinely filled -- 2 of 64 segments free is a
        // held-back spare reserve, not headroom. Pass counts are not
        // comparable across engines/policies (a pass reclaims a variable
        // number of segments), so this must pass on fill, not on pass count.
        let internal = internal_with_fill(5, 2, 64);

        assert_eq!(envelope_verdict(&stats_with_gets(), Some(internal)), Ok(()));
    }

    #[test]
    fn a_fixed_spare_reserve_is_not_mistaken_for_under_filling_at_a_small_segment_count() {
        // Measured on the x86 host, sweep 01a0bf67: free segment counts are small
        // ABSOLUTE constants set by each policy's reserve, not a fraction of
        // the heap. segment/s3fifo holds 4 free at 32, 48 and 64 total;
        // segment/fifo holds 2 at every size. A percentage-only bar therefore
        // misfires as the heap shrinks -- 4 of 32 is 12.5% and 4 of 64 is
        // 6.2%, so the same saturated cache passes at one size and fails at
        // another. That rejected 6 arms of a 120-arm sweep on a threshold
        // artefact rather than on anything about the run.
        let internal = internal_with_fill(196, 4, 32);

        assert_eq!(
            envelope_verdict(&stats_with_gets(), Some(internal)),
            Ok(()),
            "a 4-segment reserve at 32 total is a reserve, not headroom"
        );
    }

    #[test]
    fn a_reserve_larger_than_a_quarter_of_a_tiny_heap_is_still_a_reserve() {
        // Pins the absolute bound. A 4-segment reserve in a 12-segment cache
        // is 33% -- past the percentage bar -- but it is the same fixed
        // reserve that sits at 6% in a 64-segment cache, so rejecting here
        // would again be a verdict about heap size rather than about the run.
        let internal = internal_with_fill(50, 4, 12);

        assert_eq!(
            envelope_verdict(&stats_with_gets(), Some(internal)),
            Ok(()),
            "a percentage-only bar rejects a fixed reserve once the heap is small enough"
        );
    }

    #[test]
    fn a_handful_of_free_segments_in_a_large_heap_is_not_under_filling() {
        // Pins the relative bound. 10 free segments clears any small absolute
        // slack, but 10 of 512 is 2% -- a cache that plainly reached capacity.
        // An absolute-only bar would reject it for having a reserve that grew
        // with the heap.
        let internal = internal_with_fill(50, 10, 512);

        assert_eq!(
            envelope_verdict(&stats_with_gets(), Some(internal)),
            Ok(()),
            "an absolute-only bar rejects a saturated cache once the heap is large enough"
        );
    }

    #[test]
    fn a_saturated_cache_with_zero_evictions_is_still_rejected() {
        // Full-and-never-evicted is still suspect: the two checks (did it
        // fill? did it ever evict?) answer different questions, and this
        // case fails the second one even though it passes the first.
        let internal = internal_with_fill(0, 2, 64);

        let msg = envelope_verdict(&stats_with_gets(), Some(internal))
            .expect_err("a full cache that never evicted is still suspect");
        assert!(msg.contains("evict"), "{msg}");
    }

    #[test]
    fn a_run_with_no_reported_segment_totals_is_not_rejected_for_under_filling() {
        // total_segments == 0 means the engine didn't report segment counts
        // at all (the un-set `Default`). The fill check must treat this as
        // un-checkable rather than dividing by zero or reading it as "never
        // filled".
        let internal = internal(1234);
        assert_eq!(internal.total_segments, 0);

        assert_eq!(envelope_verdict(&stats_with_gets(), Some(internal)), Ok(()));
    }

    #[test]
    fn a_nonzero_free_count_with_no_total_is_still_treated_as_unchecked() {
        // A degenerate state (free_segments > 0 while total_segments == 0)
        // should never occur in practice, but the guard must be an explicit
        // `total_segments > 0`, not something that merely happens to work
        // out via the multiplication -- otherwise this exact case would slip
        // through as a false "never filled" rejection.
        let internal = internal_with_fill(1234, 5, 0);

        assert_eq!(envelope_verdict(&stats_with_gets(), Some(internal)), Ok(()));
    }

    #[test]
    fn a_run_that_never_evicted_is_rejected_rather_than_reported() {
        let verdict = envelope_verdict(&stats_with_gets(), Some(internal(0)));

        let msg = verdict.expect_err("a run with no evictions measured allocation, not eviction");
        assert!(msg.contains("evict"), "{msg}");
    }

    #[test]
    fn a_run_whose_main_layer_never_reclaimed_is_rejected_even_when_layer_zero_promoted() {
        // Above the working set, layer 0 still demotes into layer 1 while
        // layer 1 never evicts. The cache "works", but no main-pool eviction
        // decision was ever made -- which is the entire quantity a
        // merge-vs-CLOCK comparison is trying to read. Accepting the point is
        // how a saturated region of a sweep reads as "the policies agree".
        let internal = cache_core::CacheInternalStats {
            demotions: 376_122,
            evictions: 0,
            demotion_failures: 0,
            ..Default::default()
        };

        let msg = envelope_verdict(&stats_with_gets(), Some(internal))
            .expect_err("no main-layer eviction means no policy decision");
        assert!(msg.contains("evict"), "{msg}");
    }

    #[test]
    fn a_run_whose_hashtable_refused_writes_is_rejected() {
        // An undersized table refuses insertions, which changes what is
        // resident for a reason that has nothing to do with eviction policy.
        // Measured on the high-overwrite trace at 48 MB: hash_power 12
        // gave 57,271 set errors and a miss ratio of 0.2958 against 0.2810 at
        // power 22 -- a 5% shift indistinguishable from a policy effect.
        let stats = ReplayStats {
            hits: 90,
            misses: 10,
            set_errors: 57_271,
            sets: 1_021_283,
            set_error_causes: SetErrorCauses {
                hashtable_full: 57_271,
                ..Default::default()
            },
            ..Default::default()
        };

        let msg = envelope_verdict(&stats, Some(internal(176)))
            .expect_err("table pressure is not an eviction-policy result");
        assert!(msg.contains("hashtable"), "{msg}");
        assert!(
            msg.contains("hashtable_power"),
            "a hashtable-full majority must still get the hashtable_power advice: {msg}"
        );
    }

    /// Build a rejected-write stats value with a given cause breakdown. `sets`
    /// and `hits`/`misses` are fixed so only the cause mix varies between
    /// cases.
    fn stats_with_set_error_causes(causes: SetErrorCauses) -> ReplayStats {
        let set_errors =
            causes.hashtable_full + causes.out_of_memory + causes.value_too_long + causes.other;
        ReplayStats {
            hits: 90,
            misses: 10,
            sets: 1000,
            set_errors,
            set_error_causes: causes,
            ..Default::default()
        }
    }

    #[test]
    fn set_errors_from_too_few_segments_are_not_blamed_on_the_hashtable() {
        // Segment exhaustion (`OutOfMemory`) trips the exact same
        // `set_errors > 0` counter the hashtable does. Telling the operator
        // to raise `hashtable_power` here is confidently wrong -- see the
        // repro in the issue: 100% of sets failed at power 18 *and* 20
        // because too few segments, not table pressure, was the constraint.
        let stats = stats_with_set_error_causes(SetErrorCauses {
            out_of_memory: 900,
            ..Default::default()
        });

        let msg = envelope_verdict(&stats, Some(internal(176)))
            .expect_err("segment exhaustion is still not an eviction-policy result");
        assert!(
            !msg.contains("raise hashtable_power"),
            "an out-of-memory majority must not get told to raise hashtable_power: {msg}"
        );
        assert!(
            msg.contains("segment") || msg.contains("heap"),
            "an out-of-memory majority must point at segment/heap sizing: {msg}"
        );
    }

    #[test]
    fn set_errors_from_oversized_values_are_not_blamed_on_the_hashtable() {
        // `ValueTooLong` is a segment-sizing problem, not a table-sizing one:
        // raising `hashtable_power` changes nothing when a value simply does
        // not fit in one segment.
        let stats = stats_with_set_error_causes(SetErrorCauses {
            value_too_long: 900,
            ..Default::default()
        });

        let msg = envelope_verdict(&stats, Some(internal(176)))
            .expect_err("oversized values are still not an eviction-policy result");
        assert!(
            !msg.contains("raise hashtable_power"),
            "a value-too-long majority must not get told to raise hashtable_power: {msg}"
        );
        assert!(
            msg.contains("segment_size") || msg.contains("segment size"),
            "a value-too-long majority must point at segment sizing: {msg}"
        );
    }

    #[test]
    fn a_mixed_set_error_cause_does_not_pick_a_misleading_single_cause() {
        // No cause holds a majority: inventing one (by picking whichever
        // counter happens to be checked first, say) would give advice no
        // more trustworthy than a coin flip.
        let stats = stats_with_set_error_causes(SetErrorCauses {
            hashtable_full: 450,
            out_of_memory: 450,
            ..Default::default()
        });

        let msg = envelope_verdict(&stats, Some(internal(176)))
            .expect_err("mixed-cause set errors are still not an eviction-policy result");
        assert!(
            !msg.contains("raise hashtable_power"),
            "a mixed cause must not confidently advise raising hashtable_power: {msg}"
        );
        assert!(
            !msg.contains("raise segment_size"),
            "a mixed cause must not confidently advise resizing segments: {msg}"
        );
        assert!(
            msg.contains("450"),
            "a mixed cause must report the counts instead of inventing one: {msg}"
        );
    }

    #[test]
    fn a_run_with_evictions_and_gets_passes_the_envelope_check() {
        assert_eq!(
            envelope_verdict(&stats_with_gets(), Some(internal(1234))),
            Ok(())
        );
    }

    #[test]
    fn a_window_with_no_gets_is_rejected_even_when_eviction_happened() {
        let no_gets = ReplayStats {
            sets: 500,
            ..Default::default()
        };

        let msg = envelope_verdict(&no_gets, Some(internal(1234)))
            .expect_err("a window with no GETs has no miss ratio to report");
        assert!(msg.contains("GET"), "{msg}");
    }

    #[test]
    fn a_window_too_short_to_judge_has_no_tail_spread() {
        // Three intervals put one interval in the last third; its spread is
        // zero whatever the run did.
        assert_eq!(tail_spread(&[0.9, 0.5, 0.1]), None);
    }

    #[test]
    fn tail_spread_measures_only_the_last_third() {
        // A large swing early and a flat tail is a converged run.
        let intervals = vec![0.9, 0.8, 0.5, 0.30, 0.31, 0.30, 0.31, 0.30, 0.31];
        let spread = tail_spread(&intervals).expect("nine intervals is enough to judge");

        assert!(spread < 0.02, "flat tail reported spread {spread}");
    }

    #[test]
    fn a_still_trending_tail_reports_its_drift() {
        let intervals = vec![0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1];
        let spread = tail_spread(&intervals).expect("nine intervals is enough to judge");

        assert!(spread > 0.15, "drifting tail reported spread {spread}");
    }

    #[test]
    fn writes_and_reads_are_timed_into_separate_histograms() {
        // Two GETs of one key under insert_on_miss: the first misses and
        // writes, the second hits. They must land in different histograms, or
        // an inline eviction stall on a write is diluted by the read mix.
        let mut bytes = Vec::new();
        for _ in 0..2 {
            bytes.extend_from_slice(&twitter_bytes(1, 64, Op::Get));
        }
        let cache = small_cache();
        let mut reader = reader_over(bytes);

        let out = run_replay(&cache, &mut reader, &opts(0, 100)).unwrap();

        let reads = out.read_latency.expect("read samples");
        let writes = out.write_latency.expect("write samples");
        assert_eq!(total_samples(&writes), 1, "the miss performed the write");
        assert_eq!(total_samples(&reads), 1, "the hit performed no write");
    }

    #[test]
    fn warmup_records_are_not_timed() {
        let mut bytes = Vec::new();
        for _ in 0..4 {
            bytes.extend_from_slice(&twitter_bytes(1, 64, Op::Get));
        }
        let cache = small_cache();
        let mut reader = reader_over(bytes);

        let out = run_replay(&cache, &mut reader, &opts(3, 100)).unwrap();

        let reads = out.read_latency.map(|h| total_samples(&h)).unwrap_or(0);
        let writes = out.write_latency.map(|h| total_samples(&h)).unwrap_or(0);
        assert_eq!(
            reads + writes,
            1,
            "only the single measured record may be timed"
        );
    }

    fn total_samples(hist: &metriken::histogram::Histogram) -> u64 {
        hist.into_iter().map(|bucket| bucket.count()).sum()
    }

    #[test]
    fn a_key_is_zero_padded_to_the_traces_key_length() {
        let mut buf = Vec::new();
        write_key(&mut buf, 12345, 17);

        assert_eq!(buf, b"00000000000012345");
    }

    #[test]
    fn a_key_wider_than_its_length_is_not_truncated() {
        let mut buf = Vec::new();
        write_key(&mut buf, u64::MAX, 4);

        assert_eq!(
            buf,
            u64::MAX.to_string().as_bytes(),
            "truncating would collide distinct trace objects onto one key"
        );
    }

    #[test]
    fn an_oracle_general_miss_inserts_the_object_and_the_next_get_hits() {
        let cache = small_cache();
        let pool = vec![0xABu8; 4096];
        let mut key_buf = Vec::new();
        let mut stats = ReplayStats::default();

        let rec = record(42, Op::Get, 100, 0);
        apply_record(&cache, &rec, &mut key_buf, &pool, true, &mut stats);
        apply_record(&cache, &rec, &mut key_buf, &pool, true, &mut stats);

        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.sets, 1, "the miss must have inserted exactly once");
    }

    #[test]
    fn a_get_miss_without_insert_on_miss_leaves_the_cache_empty() {
        let cache = small_cache();
        let pool = vec![0xABu8; 4096];
        let mut key_buf = Vec::new();
        let mut stats = ReplayStats::default();

        let rec = record(42, Op::Get, 100, 0);
        apply_record(&cache, &rec, &mut key_buf, &pool, false, &mut stats);
        apply_record(&cache, &rec, &mut key_buf, &pool, false, &mut stats);

        assert_eq!(stats.misses, 2, "a twitter GET must not populate the cache");
        assert_eq!(stats.sets, 0);
    }

    #[test]
    fn a_set_record_stores_a_value_of_the_traces_length() {
        let cache = small_cache();
        let pool = vec![0xABu8; 4096];
        let mut key_buf = Vec::new();
        let mut stats = ReplayStats::default();

        apply_record(
            &cache,
            &record(7, Op::Set, 512, 0),
            &mut key_buf,
            &pool,
            false,
            &mut stats,
        );

        let stored_len = cache.get(&key_buf).map(|v| v.len());
        assert_eq!(stored_len, Some(512));
        assert_eq!(stats.sets, 1);
    }

    #[test]
    fn a_delete_record_removes_the_key() {
        let cache = small_cache();
        let pool = vec![0xABu8; 4096];
        let mut key_buf = Vec::new();
        let mut stats = ReplayStats::default();

        apply_record(
            &cache,
            &record(7, Op::Set, 64, 0),
            &mut key_buf,
            &pool,
            false,
            &mut stats,
        );
        apply_record(
            &cache,
            &record(7, Op::Delete, 0, 0),
            &mut key_buf,
            &pool,
            false,
            &mut stats,
        );
        apply_record(
            &cache,
            &record(7, Op::Get, 64, 0),
            &mut key_buf,
            &pool,
            false,
            &mut stats,
        );

        assert_eq!(stats.deletes, 1);
        assert_eq!(stats.misses, 1, "the GET after a DELETE must miss");
    }

    #[test]
    fn a_set_error_is_tallied_against_its_cache_error_cause() {
        // The total (`set_errors`) is read elsewhere and must keep moving on
        // every rejection; the per-cause counters must move on exactly the
        // matching variant and nothing else.
        let mut stats = ReplayStats::default();

        record_set_error(&mut stats, cache_core::CacheError::HashTableFull);
        assert_eq!(stats.set_errors, 1);
        assert_eq!(stats.set_error_causes.hashtable_full, 1);
        assert_eq!(stats.set_error_causes.out_of_memory, 0);
        assert_eq!(stats.set_error_causes.value_too_long, 0);
        assert_eq!(stats.set_error_causes.other, 0);

        record_set_error(&mut stats, cache_core::CacheError::OutOfMemory);
        assert_eq!(stats.set_errors, 2);
        assert_eq!(stats.set_error_causes.out_of_memory, 1);

        record_set_error(&mut stats, cache_core::CacheError::ValueTooLong);
        assert_eq!(stats.set_errors, 3);
        assert_eq!(stats.set_error_causes.value_too_long, 1);

        // A variant with no dedicated bucket (`store` only ever issues an
        // upsert, so this is defensive) must still be counted in the total,
        // and must land in `other` rather than being misattributed.
        record_set_error(&mut stats, cache_core::CacheError::KeyExists);
        assert_eq!(stats.set_errors, 4);
        assert_eq!(stats.set_error_causes.other, 1);
        assert_eq!(
            stats.set_error_causes.hashtable_full, 1,
            "unrelated to KeyExists"
        );
    }

    #[test]
    fn a_value_larger_than_the_pool_is_counted_rather_than_silently_shortened() {
        let cache = small_cache();
        let pool = vec![0xABu8; 256];
        let mut key_buf = Vec::new();
        let mut stats = ReplayStats::default();

        apply_record(
            &cache,
            &record(7, Op::Set, 4096, 0),
            &mut key_buf,
            &pool,
            false,
            &mut stats,
        );

        assert_eq!(stats.oversized, 1);
        assert_eq!(
            stats.sets, 0,
            "storing a short value would understate the trace's footprint"
        );
    }

    #[test]
    fn a_window_with_no_gets_has_no_miss_ratio() {
        let stats = ReplayStats {
            sets: 10,
            ..Default::default()
        };

        assert_eq!(stats.miss_ratio(), None);
    }

    #[test]
    fn miss_ratio_is_misses_over_gets() {
        let stats = ReplayStats {
            hits: 3,
            misses: 1,
            ..Default::default()
        };

        assert_eq!(stats.miss_ratio(), Some(0.25));
    }
}
