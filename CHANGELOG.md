# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Fixed

### Removed

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
