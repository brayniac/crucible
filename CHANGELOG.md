# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Fixed

### Removed

## [0.3.2] - 2026-02-12

### Added
- Benchmark prefill timeout detection and diagnostics: configurable timeout (default 300s),
  periodic progress reporting, stall detection, and root-cause diagnostics
- Zero-copy benchmark SET sends via `send_parts()` + `ValuePoolGuard`: shared 1GB random value
  pool with scatter-gather sends eliminates the 16KB send slot size limit that caused deadlocks
  when `value_size >= 16384`

### Changed
- Updated I/O documentation in CLAUDE.md for factual accuracy (zero-copy data paths, EventHandler
  trait signatures, platform requirements)

### Fixed
- HTTP/2 DATA frames now clamped to negotiated `max_frame_size` per RFC 9113 Section 4.2
- Benchmark SET deadlock when `value_size >= 16384` (send pool slot overflow)
- Rustfmt formatting in benchmark crate

## [0.3.1] - 2026-02-12

### Changed
- Benchmark diagnostic and startup logging lowered from info to debug/trace

### Fixed
- Segment leak race in non-blocking eviction (cache-core): last reader could drop between
  ref_count check and CAS to AwaitingRelease, leaving segment stuck forever. Re-check
  ref_count after CAS and call `release_condemned()` if zero
- Event loop stall in client-only mode (kompio): tick timeout now armed to prevent indefinite
  wait when no I/O completions arrive
- Default max_connections lowered to fit typical RLIMIT_NOFILE on Linux
- Server force-exits after drain timeout instead of hanging on shutdown
- Heap cache generation overflow panic: slot generation counters used 12-bit mask but
  TypedLocation only encodes 9 bits, causing panic after 512+ slot reuses
- ConnectionTable double-release during shutdown: duplicate Close CQEs could push the same
  index onto the free list twice, causing active_count() underflow

## [0.3.0] - 2026-02-11

### Added
- New `kompio` I/O framework replacing `io-driver`: push-based io_uring event loop with
  `EventHandler` trait callbacks, scatter-gather `send_parts()` API, `InFlightSendSlab` for
  zero-copy SendMsgZc sends, and bundled acceptor/worker thread management
- Scatter-gather send for large value responses: header + value + trailer sent as a single
  atomic `SendMsgZc` operation instead of one 16KB chunk per event loop iteration, eliminating
  ~1.3ms of added latency for 4MB responses
- `RegionId::UNREGISTERED` sentinel for heap-backed zero-copy guards that skip io_uring
  fixed buffer validation
- `cache-bench`: standalone cache benchmarking tool for direct cache library performance testing

### Changed
- Server, benchmark, and proxy migrated from `io-driver` to `kompio` EventHandler trait
- Transport types (`TlsTransport`, `PlainTransport`) moved from `io-driver` to `http2` crate
- CI test matrix reduced to Linux-only (macOS removed; kompio requires io_uring / Linux 6.0+)

### Fixed
- Non-blocking segment eviction under reader pressure in cache-core
- All clippy -D warnings across workspace (collapsible_if, redundant_closure, implicit borrow
  patterns, io_other_error, etc.)
- Rustdoc invalid HTML tag warnings

### Removed
- `io-driver` crate (replaced by `kompio`)

## [0.2.18] - 2026-02-09

### Changed
- io_uring ring setup now uses COOP_TASKRUN and SINGLE_ISSUER flags for reduced context
  switches and kernel-side lock-free optimizations
- io_uring send path uses hybrid direct write: tries write() on the raw fd first, falls back
  to SQE-based send only when the socket buffer is full or sends are in flight
- io_uring smart poll loop re-waits when only SendReady completions arrive, reducing
  unnecessary event loop wakeups for the benchmark client

### Fixed
- io_uring high latency variance under sustained load caused by DEFER_TASKRUN batching CQEs
- Clippy 1.93 collapsible-if warning in io_uring poll loop

## [0.2.17] - 2026-02-08

### Added
- GET TTFB (time-to-first-byte) metric in benchmark: stamps when first response bytes arrive,
  enabling apples-to-apples comparison with tools like valkey-benchmark. Displayed in all
  output formats (clean, verbose, JSON)

### Changed
- Benchmark io_uring recv buffers increased from 16KB to 256KB, reducing CQE overhead 16x
  for large value responses (e.g., 4MB GET: ~256 CQEs → ~16 CQEs)

### Fixed
- Benchmark tail latency inflation from sequential completion processing: all connections
  in a poll batch now share a single receive timestamp instead of per-connection timestamps
  that were delayed by earlier connections' read processing

## [0.2.16] - 2026-02-08

### Changed
- Re-enabled io_uring as default I/O engine (was temporarily disabled in 0.2.15)
- io_uring poll() now drains partial send continuations in a tight loop, matching mio's
  write loop behavior and eliminating per-partial-send event loop round-trips

### Fixed
- Benchmark client latency measurement inflated by pipelining: now measures from when bytes
  are sent to kernel (`sent_at`) instead of when request is encoded into buffer (`queued_at`)
- Clippy warnings in large value I/O tests

## [0.2.15] - 2026-02-08

### Added
- Vectored I/O for server send path
- Zero-copy GET support for memcache ASCII and binary protocols

### Changed
- Default I/O engine changed from io_uring to mio
- io_uring sends switched from zero-copy (SendZc) to regular sends for better throughput
- io_uring continuation SQEs submitted inline during poll for lower latency
- io_uring send slot lifecycle decoupled from SendZc NOTIF events
- io_uring send SQEs flushed immediately after completion processing
- Removed inline SQE submit at end of io_uring poll()
- Refactored zero-copy GET integration into `process_from` with simplified server loop
- Updated dependencies

### Fixed
- io_uring vectored send (SendMsgZc) partial send handling
- io_uring VectoredSend dangling pointer and missing SendReady on NOTIF
- io_uring multishot recv accumulation leak
- io_uring spurious SendReady on partial send CQEs
- io_uring generation validation for send CQEs and handle_send_regular

## [0.2.14] - 2026-02-06

### Added
- Zero-copy send queue for large GET responses: values >= 1KB are sent directly from cache
  segment memory without copying into the write buffer, reducing memory bandwidth for large values
- Export `cache_core::sync` module for downstream crate compatibility with loom feature

### Fixed
- Collapse nested if statements in io-driver for clippy 1.93 compatibility

## [0.2.13] - 2026-02-05

### Fixed
- Remove blocking retry loops (thread::sleep up to 51.2ms) from SET command handlers in all
  protocols (RESP, Memcache ASCII, Memcache Binary) — fail fast on capacity errors instead of
  stalling the worker thread

### Changed
- Extract repeated error response byte literals into module-level constants in command execution

## [0.2.12] - 2026-02-05

### Added
- Prefill mode for cache warmup in benchmark tool
- Segment compaction on item deletion with spare segment pool
- Two-queue spare segment pool for compaction operations

### Changed
- Increased max value size to 4 GiB and segment size to 128 MiB
- Adaptive threshold for merge eviction policy
- Removed manual recv_mode and zero_copy config options from io-driver

### Security
- Update `bytes` to 1.11.1 to fix integer overflow in `BytesMut::reserve` (CVE-2026-25541)

### Fixed
- Memory pool now reserves segments for writes (prevents OutOfMemory with small heaps)
- Memory pool `free_count()` now correctly excludes spare queue from eviction decisions
- Server banner now shows configured I/O engine
- Benchmark excludes prefill/warmup from bandwidth metrics
- Benchmark division by zero in output formatters
- Large value responses in server
- Drain oversized SET values to prevent protocol desync

## [0.2.11] - 2026-02-02

### Fixed
- Heap cache index out of bounds panic under high concurrency (free list link validation)
- Protocol desync with io_uring singleshot mode (connection generation validation)
- Benchmark buffer too small panic with large values (dynamic buffer growth)

## [0.2.10] - 2026-02-02

### Added
- `get_or_insert` and `get_or_insert_with` methods on ArcCache for atomic insertion semantics
- New `crucible-proxy` crate: Redis-compatible caching proxy with local SegCache layer
  - Thread-per-core architecture with io_uring support
  - Connection pooling with least-loaded backend selection
  - Configurable eviction policies (random, fifo, cte, merge, s3fifo)
  - Automatic cache invalidation on write commands (SET, DEL)

### Changed
- io_uring driver removes redundant `submit()` call before `submit_and_wait()` (reduces syscalls)
- Proxy uses sharded counters to eliminate metric contention at high throughput

### Fixed
- Benchmark parquet recorder now skips snapshots during warmup phase to avoid empty leading data

## [0.2.9] - 2026-01-31

### Changed
- Viewer overview dashboard now shows key metrics in a single flat section (throughput, hit rate, latency, error rate, bandwidth)
- Bandwidth metrics now display in Mbps instead of MB/s
- Latency dashboard shows all operation types in one group
- PromQL queries use 10s windows for better responsiveness with short benchmark runs

### Fixed
- Viewer now correctly shows parquet source metadata (was showing "unknown")
- Time axes now synchronized across all charts using global min/max time range
- Histogram heatmaps now respect global time range

## [0.2.8] - 2026-01-31

### Fixed
- Momento protosocket now correctly handles cache misses (NotFound error treated as miss, not error)
- Rustdoc warnings for unescaped brackets in RESP command doc comments

## [0.2.7] - 2026-01-31

### Added
- Momento private endpoints support for VPC connectivity
- Heap cache backend now supports Redis-compatible data structures (hashes, lists, sets)
- Configurable toggle for heap cache data structures

### Changed
- Viewer now uses metriken-query crate for TSDB and PromQL support

### Fixed
- Momento protosocket port and endpoint prefix configuration
- Various clippy warnings for Rust 1.92

## [0.2.6] - 2026-01-30

### Fixed
- Benchmark saturation search now detects system saturation when achieved throughput falls below target rate (adds `min_throughput_ratio` parameter, default 90%)

## [0.2.5] - 2026-01-29

### Fixed
- Slab verifier segfault when accessing evicted slabs during concurrent operations
- Slab `slab_count` now accurately reflects live slabs (decremented on eviction)

## [0.2.4] - 2026-01-29

### Added
- Benchmark saturation search mode for automatic SLO-compliant throughput discovery
- Heap cache backend now supports jemalloc allocator
- Random eviction policy for heap cache backend
- Comprehensive smoketests for all backend/policy combinations

### Changed
- Slab cache uses atomic counter instead of RwLock for slab_count (performance improvement)

## [0.2.3] - 2026-01-29

### Fixed
- Slab eviction (LRC, LRA, Random) segfault when evicting slabs with uninitialized slots
- Slab random eviction failing due to including evicted/zombie slabs in selection

## [0.2.2] - 2026-01-28

### Fixed
- Release workflow now correctly uses repository variables for AWS_REGION
- Tag-release workflow uses PAT to trigger release workflow

## [0.2.1] - 2026-01-28

### Fixed
- Release workflow now correctly reads secrets for S3 and CloudFront configuration

## [0.2.0] - 2026-01-28

First public release.

### Added
- Cache server with RESP (Redis) and Memcache protocol support
- Multiple storage backends: segment (default), slab, heap
- Multiple eviction policies: S3-FIFO, FIFO, LRU variants, random, CTE, merge
- RAM to disk tiering for segment and slab backends
- Custom io-driver with io_uring support (Linux 6.0+) and mio fallback
- Thread-per-core architecture with CPU pinning
- Lock-free multi-choice hashtable with ghost entries
- Zero-copy I/O paths where possible
- Hugepage support (2MB/1GB)
- NUMA-aware memory allocation
- Prometheus metrics endpoint
- Benchmark tool with web dashboard viewer
- Parquet output for benchmark results
- Rezolus telemetry correlation in viewer
