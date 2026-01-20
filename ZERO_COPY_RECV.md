# Zero-Copy Receive Design

## Problem Statement

For write-heavy workloads with large values (e.g., 10MB), the current receive path has two inefficiencies:

1. **Coalescing allocation**: Many 16KB recv operations grow a `BytesMut` to hold the full value
2. **Copy to segment**: After parsing, the value is copied from coalesce buffer to cache segment

```
Current: kernel → coalesce_buf (alloc) → parse → segment (copy) → free coalesce_buf
Goal:    kernel → segment (direct)     → parse → done
```

## Protocol Analysis

Both RESP and Memcache have length-prefixed values, making scatter-gather receive possible:

### RESP SET
```
*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$10485760\r\n<10MB value>\r\n
^----------------------------------------^   ^--------------^
          Header (parseable)                  Value (known length)
```

After parsing the header, we know:
- Command: SET
- Key: "mykey"
- Value length: 10485760 bytes

### Memcache ASCII SET
```
set mykey 0 3600 10485760\r\n<10MB value>\r\n
^-------------------------^  ^--------------^
    Header (parseable)        Value (known length)
```

### Memcache Binary
```
[24-byte header with total_body_length field][key][extras][value]
```

## Proposed Design

### Phase 1: Streaming Parser

Add a two-phase parse API that can yield after parsing the header:

```rust
/// Result of incremental parsing
pub enum ParseProgress<'a> {
    /// Need more data to continue
    Incomplete,

    /// Header parsed, value pending. Provides info needed to allocate target buffer.
    NeedValue {
        /// Bytes consumed so far (header)
        consumed: usize,
        /// Command type for routing
        command: CommandHeader<'a>,
        /// Size of value to receive
        value_len: usize,
        /// Bytes of value already in buffer (if any)
        value_prefix: &'a [u8],
    },

    /// Fully parsed command
    Complete(Command<'a>, usize),
}

/// Minimal header info before value is received
pub enum CommandHeader<'a> {
    Set { key: &'a [u8], flags: u32, ttl: Duration },
    // Other commands that have large payloads...
}
```

### Phase 2: Cache Reserve API

Add ability to reserve segment space before receiving data:

```rust
impl Cache {
    /// Reserve space for a value, returning a mutable buffer.
    ///
    /// The reservation is uncommitted - call `commit_set` to finalize
    /// or drop the `SetReservation` to cancel.
    pub fn reserve_set(
        &self,
        key: &[u8],
        value_len: usize,
        ttl: Duration
    ) -> CacheResult<SetReservation> {
        // 1. Ensure space in layer 0 (may trigger eviction)
        // 2. Reserve segment space
        // 3. Return handle with mutable slice into segment
    }
}

/// Handle for an uncommitted SET operation
pub struct SetReservation<'a> {
    cache: &'a Cache,
    segment_id: u32,
    offset: usize,
    len: usize,
    key: Vec<u8>,
    ttl: Duration,
}

impl SetReservation<'_> {
    /// Get mutable slice for direct write (e.g., recv target)
    pub fn value_mut(&mut self) -> &mut [u8];

    /// Commit the SET after value is received
    pub fn commit(self) -> CacheResult<()>;
}

impl Drop for SetReservation<'_> {
    fn drop(&mut self) {
        // Rollback: release segment space if not committed
    }
}
```

### Phase 3: Driver Vectored Recv

Add TCP recvmsg support (currently only UDP has it):

```rust
impl IoDriver {
    /// Submit vectored receive into multiple buffers.
    ///
    /// Useful for scatter-gather: header into small buffer,
    /// payload directly into target (e.g., cache segment).
    fn submit_recvmsg_tcp(
        &mut self,
        id: ConnId,
        iovecs: &mut [IoSliceMut<'_>]
    ) -> io::Result<()>;
}
```

### Phase 4: Connection State Machine

Update connection to track parse state:

```rust
enum RecvState {
    /// Normal: receiving into coalesce buffer, parsing complete commands
    Normal,

    /// Header parsed, receiving value directly into segment
    ReceivingValue {
        /// Reservation for the SET
        reservation: SetReservation,
        /// Bytes of value received so far
        received: usize,
        /// Total value length expected
        total: usize,
    },
}
```

## Data Flow (Optimized)

```
1. recv() → small header data arrives

2. parse_incremental(buffer) → NeedValue {
       command: Set { key: "mykey", ... },
       value_len: 10485760,
       value_prefix: &[first 1000 bytes]
   }

3. reservation = cache.reserve_set("mykey", 10485760, ttl)

4. Copy value_prefix into reservation.value_mut()[..1000]

5. submit_recvmsg_tcp(conn_id, &mut [
       IoSliceMut::new(&mut small_buf),           // command tail / next header
       IoSliceMut::new(&mut reservation.value_mut()[1000..])  // rest of value
   ])

6. On completion: reservation.commit()
```

## Benefits

| Metric | Current (10MB SET) | Optimized |
|--------|-------------------|-----------|
| Heap allocations | 1 (10MB BytesMut) | 0 |
| Copies | 2 (kernel→buf, buf→segment) | 1 (kernel→segment) |
| Memory bandwidth | 30MB | 10MB |

## Implementation Complexity

| Component | Complexity | Notes |
|-----------|------------|-------|
| Streaming parser | Medium | New API, existing logic mostly reusable |
| Cache reserve API | Medium | New state machine for uncommitted writes |
| Driver recvmsg TCP | Low | Similar to existing UDP recvmsg |
| Connection state | Medium | Track ReceivingValue state |
| Error handling | Medium | Rollback on connection close, timeout |

## Edge Cases

1. **Value spans multiple recvs**: Track progress in `RecvState::ReceivingValue`
2. **Connection closes mid-value**: `Drop` on `SetReservation` releases segment space
3. **Parse error after partial receive**: Same as #2
4. **Segment exhaustion**: `reserve_set` returns error, fall back to normal path
5. **Pipelining**: After value complete, check for next command in small buffer

## Threshold Behavior

Only use zero-copy receive for values above a threshold (e.g., 64KB):

```rust
const ZERO_COPY_RECV_THRESHOLD: usize = 64 * 1024;

match parse_result {
    NeedValue { value_len, .. } if value_len >= ZERO_COPY_RECV_THRESHOLD => {
        // Use reservation + scatter-gather
    }
    _ => {
        // Normal path: coalesce buffer
    }
}
```

Small values don't benefit enough to justify the complexity.

## Alternatives Considered

### 1. io_uring splice/tee
- Kernel-to-kernel copy without user-space
- Requires file descriptors for segment memory (memfd)
- More complex, Linux-only, limited benefit over recvmsg

### 2. DPDK / kernel bypass
- Eliminates all kernel copies
- Massive complexity, different driver entirely
- Not portable

### 3. Larger recv buffers
- Increase buffer_size to 1MB+
- Reduces coalescing but still copies to segment
- Wastes memory for small values

## Recommended Implementation Order

1. **Streaming parser** - Can be tested in isolation
2. **Cache reserve API** - Core building block
3. **Integration** - Wire up in connection handler
4. **Driver recvmsg TCP** - Optimization (can use multiple recv initially)
5. **Benchmarking** - Validate benefits on real workloads
