# ADR-001: CacheLayer Enum Dispatch

Date: 2026-04-28

Status: Accepted

## Context

Crucible's cache architecture uses a `TieredCache` that can hold multiple cache layers (FIFO, TTL, Disk). The question arose: should `CacheLayer` use:

1. **Enum dispatch** - `enum CacheLayer { Fifo(FifoLayer), Ttl(TtlLayer), ... }`
2. **Dynamic dispatch** - `Box<dyn Layer>` (trait object)
3. **Generic-based design** - `struct TieredCache<L: Layer>`

### The `Layer` Trait

```rust
pub trait Layer: Send + Sync {
    type Guard<'a>: ItemGuard<'a> where Self: 'a;
    // ... methods using GAT
}
```

The `Layer` trait uses a **Generic Associated Type (GAT)** for `Guard<'a>`, which:
- Returns an `ItemGuard<'a>` with lifetime tied to the layer
- Allows zero-copy access to segment data via `&'a [u8]`
- Makes the trait **not object-safe** (GATs cannot be used in trait objects)

## Decision

Use **enum dispatch** for `CacheLayer`:

```rust
pub enum CacheLayer {
    Fifo(FifoLayer),
    Ttl(TtlLayer),
    Disk(DiskLayer),
    IoUringDisk(IoUringDiskLayer),
}
```

## Considered Alternatives

### Alternative 1: Box<dyn Layer> (Dynamic Dispatch)

**Why rejected:**

```rust
pub enum CacheLayer {
    Fifo(Box<dyn Layer>),
    Ttl(Box<dyn Layer>),
    // ...
}
```

**Problems:**
- GATs (`Guard<'a>`) cannot be used in trait objects
- Would require redesigning the `Layer` trait to remove GATs
- Loss of zero-copy lifetime guarantees
- Double indirection (Box + enum variant)
- More heap allocations

### Alternative 2: Generic TieredCache<L: Layer>

**Why rejected:**

```rust
pub struct TieredCache<L: Layer> {
    layers: Vec<L>,
}
```

**Problems:**
- All layers must be the same type
- Cannot mix FIFO and TTL layers in a single cache
- Would require separate cache types for each configuration
- Loses the "tiered" nature of the architecture

### Alternative 3: Trait Object with Reduced Interface

**Why rejected:**

Remove GATs from the trait and return owned types:

```rust
pub trait Layer: Send + Sync {
    // No GAT - returns owned data
    fn get_item(&self, location: ItemLocation, key: &[u8]) -> Option<ItemData>;
}
```

**Problems:**
- Loses zero-copy capability
- Returns owned `Vec<u8>` instead of `&[u8]`
- Defeats the purpose of the segment-based design
- Performance regression in hot path

## Pros of Enum Dispatch

1. **Object-safe design maintained**: The `Layer` trait keeps GATs, enabling zero-copy
2. **Single allocation per layer**: No heap allocation for each layer variant
3. **No double indirection**: Direct variant access (enum + direct field)
4. **Zero-cost abstraction**: Compiler knows all variants at compile time
5. **Type safety**: Pattern matching ensures all cases handled
6. **Extensibility**: Easy to add new layer types (new enum variant)

## Cons of Enum Dispatch

1. **Code duplication in dispatch**: `match` statements in `CacheLayer` methods
2. **Larger binary size**: Each method has a match statement (minimal impact)
3. **No runtime polymorphism**: Cannot add new layer types without recompiling
4. **Enum grows with variants**: Each layer adds one variant

## Implementation

The enum dispatch is implemented via the `dispatch!` macro in `cache.rs`:

```rust
macro_rules! dispatch {
    ($self:expr, $method:ident ( $($arg:expr),* $(,)? )) => {
        match $self {
            CacheLayer::Fifo(layer) => layer.$method($($arg),*),
            CacheLayer::Ttl(layer) => layer.$method($($arg),*),
            CacheLayer::Disk(layer) => layer.$method($($arg),*),
            CacheLayer::IoUringDisk(layer) => layer.$method($($arg),*),
        }
    };
}
```

## Lessons

1. **GATs are powerful but limit polymorphism**: If you need trait objects, avoid GATs
2. **Enum dispatch is zero-cost**: For a small number of variants, enums beat trait objects
3. **Compile-time knowledge enables optimization**: Compiler can optimize match statements better than vtables
4. **Consider future extensibility**: If plugins/external layers needed, consider trait objects despite costs

## Reviewers

- AI Architecture Review (2026-04-28)
