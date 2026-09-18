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
    /// Deletes issued.
    pub deletes: u64,
    /// Records whose value exceeded the replay's value buffer.
    pub oversized: u64,
}

impl ReplayStats {
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
    let needs_value = !matches!(record.op, Op::Get | Op::Gets | Op::Delete);
    if (needs_value || insert_on_miss) && value_len > value_pool.len() {
        // Storing a short value would understate the trace's memory footprint
        // and therefore overstate how many items fit — the denominator of
        // everything this rig reports.
        stats.oversized += 1;
        return;
    }

    match record.op {
        Op::Get | Op::Gets => {
            if cache.with_value(key_buf, |_| ()).is_some() {
                stats.hits += 1;
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
        Err(_) => stats.set_errors += 1,
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
    /// Miss ratio per interval across the measured window.
    ///
    /// Reported so the tail can be checked for flatness: a window still
    /// trending at its end was too short, and its average is a value that
    /// occurs nowhere in the run.
    pub intervals: Vec<f64>,
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
    let value_pool = vec![0xA5u8; opts.max_value_bytes];
    let mut measured_records = 0u64;

    while let Some(result) = reader.next_record() {
        // A read error ends the replay rather than truncating it silently: a
        // short trace produces a complete-looking run over a fraction of the
        // workload, which is the failure this whole rig exists to avoid.
        let record = result?;

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
        apply_record(
            cache,
            &record,
            &mut key_buf,
            &value_pool,
            opts.insert_on_miss,
            stats,
        );

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

    Ok(ReplayOutcome {
        warmup,
        measured,
        records_read: reader.records_read(),
        intervals,
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

    match internal {
        Some(stats) if stats.evictions == 0 && stats.demotions == 0 => Err(format!(
            "no segment was evicted or demoted in the measured window \
             ({} sets, {} set errors): this cache size is above the trace's \
             working set, so the point measures allocation rather than eviction",
            measured.sets, measured.set_errors
        )),
        _ => Ok(()),
    }
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

    fn record(key_id: u64, op: Op, value_len: u32, ttl_secs: u32) -> TraceRecord {
        TraceRecord {
            key_id,
            key_len: ORACLE_GENERAL_KEY_LEN,
            value_len,
            op,
            ttl_secs,
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

    fn opts(warmup: u64, interval: u64) -> ReplayOptions {
        ReplayOptions {
            warmup_records: warmup,
            max_records: None,
            report_interval_records: interval,
            max_value_bytes: 4096,
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
        }
    }

    #[test]
    fn a_run_that_never_evicted_is_rejected_rather_than_reported() {
        let verdict = envelope_verdict(&stats_with_gets(), Some(internal(0)));

        let msg = verdict.expect_err("a run with no evictions measured allocation, not eviction");
        assert!(msg.contains("evict"), "{msg}");
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
