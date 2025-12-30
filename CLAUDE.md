# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Crucible is a Rust library providing high-performance, segment-based cache implementations. The core crate (`cache-core`) provides building blocks for tiered caching systems with support for multiple eviction policies, TTL management, and lock-free concurrent access.

## Build Commands

```bash
# Build the workspace
cargo build

# Build for release (with LTO)
cargo build --release

# Run all tests
cargo test

# Run tests for a specific crate
cargo test -p cache-core

# Run a specific test
cargo test test_name

# Run tests with the loom feature for concurrency testing
cargo test -p cache-core --features loom

# Format code
cargo fmt

# Lint with clippy
cargo clippy
```

## Architecture

### Layer-Based Cache Design

The cache uses a tiered layer architecture where items flow through multiple storage layers:

```
+------------------+
|    Hashtable     |  <- Key -> (Location, Frequency, Ghost)
+--------+---------+
         |
         v
+------------------+
|     Layer 0      |  <- Admission queue (FIFO organization)
+--------+---------+
         | evict (demote hot items)
         v
+------------------+
|     Layer 1      |  <- Main cache (TTL bucket organization)
+------------------+
```

### Core Components

**TieredCache** (`cache.rs`) - Orchestrates multiple layers with a shared hashtable. All writes go to Layer 0; items are demoted to subsequent layers based on frequency during eviction.

**Layer Trait** (`layer/traits.rs`) - Defines the interface for cache layers. Two implementations:
- `FifoLayer` - FIFO-organized layer for S3FIFO admission queue
- `TtlLayer` - TTL bucket-organized layer for main cache storage

**Hashtable Trait** (`hashtable.rs`) - Lock-free cuckoo hashtable mapping keys to `(ItemLocation, Frequency)`. Supports ghost entries that preserve frequency after eviction for "second chance" semantics.

**Segment Trait** (`segment.rs`) - Fixed-size memory regions storing items sequentially. Segments use a state machine for concurrent access control and support both segment-level and per-item TTL.

**RamPool Trait** (`pool.rs`) - Manages segment allocation/deallocation. `MemoryPool` is the concrete implementation with optional hugepage support.

### Item Storage

Items are stored with two header formats:
- `BasicHeader` - Segment-level TTL (all items in segment share expiration)
- `TtlHeader` - Per-item TTL (each item has individual expiration)

Headers include optional magic bytes and checksums when the `validation` feature is enabled.

### Eviction Strategies

Configured via `EvictionStrategy`:
- **FIFO** - Evict oldest segment
- **Random** - Evict random segment
- **CTE** - Evict segment closest to expiration
- **Merge** - Combine segments, pruning low-frequency items

### Key Types

- `ItemLocation` - Packed (pool_id, segment_id, offset, generation) identifying an item
- `State` - Segment state machine (Free -> Reserved -> Linking -> Live -> Sealed -> ...)
- `LayerConfig` - Layer behavior configuration (ghosts, demotion threshold, eviction strategy)

## Concurrency Model

- All public types are `Send + Sync`
- Hashtable uses lock-free CAS operations
- Segments use atomic operations for state transitions and item appends
- Reference counting prevents segment clearing during active reads
- The `loom` feature enables deterministic concurrency testing

## Workspace Structure

```
cache/
  core/       # Main cache-core crate with layer-based architecture
  segcache/   # (planned) SegCache implementation
  s3fifo/     # (planned) S3FIFO implementation
```
