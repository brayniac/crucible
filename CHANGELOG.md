# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Fixed

### Removed

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
