# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-02-24

### Added
- **SET retry on eviction failure**: Async server handler retries SET commands that fail due to
  eviction pressure, with configurable timeout (`set_retry_timeout_us`). Prevents spurious
  failures under sustained write load
- **Async server as default**: `crucible-server` now uses the async ringline handler (io_uring
  with async task-per-connection) as the sole runtime

### Changed
- **Migrated to [ringline](https://github.com/ringline-rs/ringline)**: Replaced the local `krio`
  I/O framework with the published `ringline` crate. All handlers (server, proxy, benchmark) now
  implement `AsyncEventHandler` with the async task-per-connection model
- **Migrated to published protocol crates**: `resp-proto`, `memcache-proto`, `http2-proto`,
  `grpc-proto`, and `ketama` are now published crates on crates.io instead of in-tree
- **Proxy migrated to ringline-redis**: Proxy backend uses `ringline-redis::Client` instead of
  custom client module
- **Benchmark extracted to [cachecannon](https://github.com/cachecannon/cachecannon)**: The
  in-tree benchmark tool has been moved to a separate repository. CI smoketests and docs updated
  to reference cachecannon

### Fixed
- **Memcache binary unknown command hang**: Unsupported binary commands (Append, Prepend, Touch,
  Gat) now return an error response instead of silently returning nothing, which caused clients
  to hang indefinitely
- **Disk tier flush and read lifecycle**: Wired up io_uring disk flush worker and fixed read
  error paths to properly release segment ref_counts and staging buffers
- **Disk demotion under reader pressure**: Items can now be demoted to disk even when the source
  segment has active readers
- Clippy and rustdoc warnings across workspace

### Removed
- Local `krio` I/O framework (replaced by `ringline`)
- In-tree `client-*` Tokio-based client crates (not krio-native)
- In-tree `benchmark` crate (moved to cachecannon)
- Unused `ArcCache` wrapper, dead `workers` module, unused `slab` dependency
- Dead `stats` and `worker_id` fields from `AsyncServerHandler`

## [0.3.8] - 2026-02-19

### Fixed
- **Silent drop on cache full**: SET commands now return success instead of `-ERR cache full`
  when the cache is full and eviction fails. A cache is best-effort storage — silently dropping
  the item matches Redis behavior with eviction enabled and fixes compatibility with clients
  like valkey-benchmark. The `SET_ERRORS` metric is still incremented for observability. Applies
  to all three protocol handlers: RESP, Memcache ASCII, and Memcache binary.

## [0.3.7] - 2026-02-19

### Fixed
- **S3-FIFO promotion path**: `evict_from_layer()` now uses each layer's configured `next_layer`
  for demotion instead of always targeting the disk layer. Layer 0 correctly demotes to Layer 1
  (main queue), Layer 1 demotes to disk, fixing empty main queue segments.
- **Cascading eviction**: `ensure_space()` ensures downstream layers have free segments before
  demoting (bottom-up: disk → Layer 1 → Layer 0), preventing demotion failures from full targets.
- **Disk segment eviction**: Disk layer segments can now be evicted and recycled when full,
  preventing `OutOfMemory` after all disk segments are allocated.
- **Write buffer leaks**: `process_evicted_segment()` and `release_read()` now properly return
  write buffers to the pool when evicting unflushed segments or releasing condemned segments.
- **Write buffer pool ownership**: `AlignedBufferPool` moved into `IoUringDiskLayer` struct so
  all code paths (eviction, condemned cleanup, flush completion) can return buffers. Previously
  broke `--all-features` builds.

### Changed
- `IoUringDiskLayer::write_item()` now works via the `Layer` trait (was returning `Unsupported`)
- `IoUringDiskLayer::complete_flush()` no longer takes a `buffer_pool` parameter
- `IoUringDiskLayer::write_item_with_buffers()` no longer takes a `buffer_pool` parameter

### Added
- `write_buffer_count` config for `IoUringDiskLayerBuilder` and `IoUringDiskTierConfig`
  (default: 16). Controls RAM usage for disk write staging buffers. Under pressure, demotion
  degrades gracefully to discard.

## [0.3.6] - 2026-02-18

### Added
- **io_uring disk tier** for segcache: Direct I/O and NVMe io_uring passthrough backends for
  segment demotion/promotion, with block-aligned reads, lock-free disk segment pool, and
  configurable promotion threshold
- **krio async disk I/O API**: `open_direct_io_file()`, `direct_io_read()`, `open_nvme_device()`,
  `nvme_read()` free functions with `DiskIoFuture` for awaitable io_uring disk operations
- Async server integration tests (7 tests mirroring callback server test suite)
- Disk tier integration tests for both callback and async servers with DirectIo backend
- CI smoketest configs for async server and DirectIo disk tier

### Fixed
- Async server binary (`crucible-server-async`) now correctly uses `IoUringDiskTierConfig` for
  DirectIo/NVMe backends instead of mmap-based `DiskTierConfig`
- Async server disk I/O initialization moved from `on_accept()` (outside executor context) to
  `handle_connection()` (inside executor context) to prevent "called outside executor" panic
- `BasicHeader::from_bytes_unchecked()` calls replaced with `from_bytes()` for `--all-features`
  compatibility (the unchecked variant is `#[cfg(not(feature = "validation"))]`)
- `IoUringPool` unit tests gated with `#[cfg(not(feature = "loom"))]` to prevent loom scheduler
  panics from crossbeam-deque atomics outside `loom::model`
- Broken rustdoc link to `IoUringDiskLayer` in `server::disk_io`
- Clippy warnings: manual `abs_diff` pattern, unused `spare_queue` field

## [0.3.5] - 2026-02-17

### Added
- **krio 0.1**: Renamed I/O framework from `kompio` to `krio`, establishing standalone identity
  - Clean public API surface: internal modules now `pub(crate)` — only user-facing types re-exported
  - Async runtime with `AsyncEventHandler`: `spawn`, `sleep`, `timeout`, `select` combinators,
    awaitable sends, resilient timers, UDP foundation, and join combinators
  - Integration test suite (echo server: callback + async, multiple connections, large messages,
    connection lifecycle, graceful shutdown)
  - Free `connect()` and `on_start()` in async API
- **krio-redis**: Native RESP client crate with TLS, AUTH, ketama-sharded client, and Redis
  Cluster support (renamed from `krio-client`)
- **krio-memcache**: Native memcache client with full ASCII protocol support
- **krio-quic**: QUIC transport layer
- **http3**: HTTP/3 framing crate with Huffman encoding/decoding and QPACK support
- **crucible-http-client**: Async HTTP/2 client with TLS support
- **crucible-grpc-client**: Async gRPC client with e2e integration tests
- **crucible-momento-client**: Async Momento cache client with e2e integration tests
- **crucible-memcache-client**: Async Memcache client with e2e integration tests
- Async server handler for A/B benchmarking against callback handler
- Async proxy with ketama consistent hashing
- Benchmark Redis Cluster support with ketama key routing
- TLS support in `crucible-server`
- Zero-copy RESP parsing with `Bytes`-backed `Value` type
- Huffman encoding in QPACK encoder for smaller header blocks
- Redis Cluster protocol support in `protocol-resp`

### Changed
- `KompioBuilder` renamed to `KrioBuilder`
- `kompio` crate renamed to `krio`
- `krio-client` renamed to `krio-redis`
- `crucible-resp-client` (client-resp) rewritten as pure Tokio async Redis client
- krio-redis uses `Bytes` returns and `AsRef<[u8]>` params
- Scatter-gather SET sends in krio-redis to eliminate extra value copy

### Fixed
- Rate limiter token waste when no session has pipeline capacity
- `WithDataFuture` retries incomplete parses instead of signaling EOF
- Hybrid zero-copy sends for async server slab exhaustion
- Clippy warnings across workspace

## [0.3.4] - 2026-02-14

### Added
- Benchmark `backfill_on_miss` mode: on GET miss, automatically issue a SET for the same key
  to simulate cache-aside (read-through) workloads. Backfill SETs count against rate limit and
  pipeline depth, use zero-copy sends, and are tracked with dedicated metrics
  (`backfill_set_count`, `backfill_set_latency`)
- Configurable S3-FIFO small queue percent (`small_queue_percent` in `[cache]` config, default
  10, valid 1-50) for tuning the admission filter size in both segment and heap backends
- Per-connection send queue in krio: handlers call `send()`/`send_parts()` freely and krio
  queues excess sends internally, submitting the next one immediately in the CQE completion
  handler to eliminate throughput gaps at deep pipeline depths

### Fixed
- TCP stream interleaving with large pipelined responses: serialized sends to at most one
  in-flight SQE per connection, preventing kernel interleaving of concurrent SendMsgZc
  operations that corrupted RESP framing
- IO_LINK send chains removed: partial sends caused io_uring to execute linked SQEs
  immediately, interleaving unsent data with the next batch and corrupting the protocol stream
- Benchmark pipeline starvation: requests now driven from `on_send_complete` in addition to
  `on_tick`, keeping the pipeline filled without waiting for the next tick cycle
- Duplicate benchmark connections from `try_reconnect` during initial connect phase: sessions
  now marked with `reconnect_attempted` after creation to prevent double-connect
- Prefill metrics, connection gauge, and per-session prefill timeout accuracy
- Unnecessary i32 casts in krio chain tests

## [0.3.3] - 2026-02-13

### Fixed
- Pipelined large-value response stall (krio): when `process_from` hit write backpressure
  (pending > 256KB), remaining commands sat in the recv accumulator with no mechanism to resume.
  Added `replay_accumulated()` to re-invoke `on_data` after send completions drain write capacity
- Streaming recv CRLF livelock (server): when a large SET value's trailing `\r\n` was split across
  recv buffers, `continue_streaming_recv()` returned true without consuming data, causing an
  infinite processing loop. Fixed for RESP, memcache ASCII, and memcache binary protocols

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
- Event loop stall in client-only mode (krio): tick timeout now armed to prevent indefinite
  wait when no I/O completions arrive
- Default max_connections lowered to fit typical RLIMIT_NOFILE on Linux
- Server force-exits after drain timeout instead of hanging on shutdown
- Heap cache generation overflow panic: slot generation counters used 12-bit mask but
  TypedLocation only encodes 9 bits, causing panic after 512+ slot reuses
- ConnectionTable double-release during shutdown: duplicate Close CQEs could push the same
  index onto the free list twice, causing active_count() underflow

## [0.3.0] - 2026-02-11

### Added
- New `krio` I/O framework replacing `io-driver`: push-based io_uring event loop with
  `EventHandler` trait callbacks, scatter-gather `send_parts()` API, `InFlightSendSlab` for
  zero-copy SendMsgZc sends, and bundled acceptor/worker thread management
- Scatter-gather send for large value responses: header + value + trailer sent as a single
  atomic `SendMsgZc` operation instead of one 16KB chunk per event loop iteration, eliminating
  ~1.3ms of added latency for 4MB responses
- `RegionId::UNREGISTERED` sentinel for heap-backed zero-copy guards that skip io_uring
  fixed buffer validation
- `cache-bench`: standalone cache benchmarking tool for direct cache library performance testing

### Changed
- Server, benchmark, and proxy migrated from `io-driver` to `krio` EventHandler trait
- Transport types (`TlsTransport`, `PlainTransport`) moved from `io-driver` to `http2` crate
- CI test matrix reduced to Linux-only (macOS removed; krio requires io_uring / Linux 6.0+)

### Fixed
- Non-blocking segment eviction under reader pressure in cache-core
- All clippy -D warnings across workspace (collapsible_if, redundant_closure, implicit borrow
  patterns, io_other_error, etc.)
- Rustdoc invalid HTML tag warnings

### Removed
- `io-driver` crate (replaced by `krio`)

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
