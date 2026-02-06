# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Fixed

### Removed

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
