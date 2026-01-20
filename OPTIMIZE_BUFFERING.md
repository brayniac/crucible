# Buffering Optimization Analysis

This document analyzes memory copy counts in the read/write paths across different I/O backends and identifies optimization opportunities.

## Current State

### Copy Counts by Backend

#### SET Path (Network → Cache Segment)

| Backend | Network→App | App→Segment | Total |
|---------|-------------|-------------|-------|
| **Tokio** | 2 (kernel→temp_buf→recv_buf) | 1 | **3** |
| **Mio** | 1 (kernel→recv_state) | 1 | **2** |
| **io_uring multishot** | 1 (ring_buf→coalesce) | 1 | **2** |
| **io_uring single-shot** | 0 (direct to pool_buf) | 1 | **1** |

#### GET Path (Cache Segment → Network)

| Backend | Segment→WriteBuf | WriteBuf→Network | Total |
|---------|------------------|------------------|-------|
| **Tokio** | 1 | 1 (write syscall) | **2** |
| **Mio** | 1 | 1 (write syscall) | **2** |
| **io_uring** | 1 | 2 (→frag_buf→send_buf) + 0 (SendZc) | **3** |

### Why io_uring Has Extra Send Copies

The io_uring send path uses double-buffering for SendZc:

1. `frag_buf` (BytesMut): Accumulates data while previous SendZc is in flight
2. `send_buf` (Vec): Stable buffer for in-flight SendZc - required because BytesMut could reallocate

```
write_buf → frag_buf → send_buf → kernel (SendZc, zero-copy)
   copy 1     copy 2     copy 3=0
```

## Optimization Opportunities

### Priority 1: Tokio temp_buf Elimination (Easy)

**Status: IMPLEMENTED**

**Problem:** Tokio reads into temp_buf, then copies to recv_buf.

```rust
// Before (2 copies from kernel):
let mut temp_buf = vec![0u8; 64 * 1024];
match stream.try_read(&mut temp_buf) {
    Ok(n) => recv_buf.append(&temp_buf[..n]);  // unnecessary copy
}
```

**Solution:** Read directly into recv_buf's spare capacity.

```rust
// After (1 copy from kernel):
recv_buf.reserve(READ_BUF_SIZE);
let spare = recv_buf.spare_capacity_mut();
match stream.try_read(spare) {
    Ok(n) => unsafe { recv_buf.advance(n) };
}
```

**Impact:** -1 copy on SET path for Tokio backend.

### Priority 2: io_uring send_buf Elimination (Medium)

**Status: IMPLEMENTED**

**Problem:** Data is copied from frag_buf to send_buf before SendZc.

```rust
// Before (copies data):
fn prepare_send(&mut self) -> Option<(*const u8, usize)> {
    self.send_buf.extend_from_slice(&self.frag_buf[..chunk_size]);  // COPY
    // ...
}
```

**Solution:** Use `BytesMut::split_to().freeze()` to get an immutable `Bytes` handle without copying. `Bytes` is reference-counted and immutable, safe for SendZc.

```rust
// After (no copy - just Arc increment):
fn prepare_send(&mut self) -> Option<Bytes> {
    let chunk = self.frag_buf.split_to(chunk_size).freeze();  // NO COPY
    Some(chunk)
}
```

**Impact:** -1 copy on GET path for io_uring backend.

### Priority 3: Scatter-Gather GET (Hard)

**Status: NOT IMPLEMENTED**

**Problem:** Values are copied from segment to write_buf in `execute_resp()`.

```rust
cache.with_value(key, |value| {
    write_buf.extend_from_slice(value);  // copies entire value
});
```

**Solution:** Use scatter-gather I/O to send directly from segment memory.

**New Cache API:**
```rust
trait Cache {
    fn get_ref(&self, key: &[u8]) -> Option<ValueRef>;
}

struct ValueRef {
    guard: SegmentReadGuard,  // Prevents eviction
    data: *const u8,
    len: usize,
}
```

**New Driver API:**
```rust
trait IoDriver {
    fn send_vectored(&mut self, id: ConnId, iov: &[IoSlice]) -> io::Result<usize>;
}
```

**GET flow:**
```rust
let header = format!("${}\r\n", value.len());
let value_ref = cache.get_ref(key)?;

driver.send_vectored(conn_id, &[
    IoSlice::new(header.as_bytes()),
    IoSlice::new(value_ref.as_slice()),  // direct from segment!
    IoSlice::new(b"\r\n"),
])?;
```

**Challenges:**
- Must hold `ValueRef` until SendZc notif arrives (io_uring)
- Track in-flight refs per connection
- Handle connection close with pending sends

**Impact:** -1 copy on GET path (the largest copy - the actual value).

### Priority 4: Remove Connection::write_buf

**Status: NOT IMPLEMENTED**

After implementing scatter-gather GET, Connection::write_buf becomes unnecessary for values. Only a small frame_buf needed for protocol framing (~64 bytes for headers).

## Optimized Copy Counts

### Current State (After Priority 1 & 2)

| Operation | Tokio | Mio | io_uring multishot | io_uring single-shot |
|-----------|-------|-----|--------------------|--------------------|
| SET (small) | 2 | 2 | 2 | 1 |
| GET (small) | 2 | 2 | 2 | 2 |
| **Total** | **4** | **4** | **4** | **3** |

### After All Optimizations (Future)

| Operation | Tokio | Mio | io_uring multishot | io_uring single-shot |
|-----------|-------|-----|--------------------|--------------------|
| SET (small) | 2 | 2 | 2 | 1 |
| GET (small) | 1* | 1* | 1* | 0** |
| **Total** | **3** | **3** | **3** | **1** |

\* Header/trailer copied (~22 bytes), value zero-copy via scatter-gather
\** True zero-copy: recv from pool_buf, send via scatter-gather SendZc

## Implementation Notes

### io_uring SendZc Buffer Requirements

SendZc requires the buffer to remain valid and unchanged until the kernel sends a notification (CQE with `IORING_CQE_F_NOTIF`). This is why:

1. `Bytes` (from freeze()) is safe: immutable, reference-counted
2. `BytesMut` is NOT safe: could reallocate on append
3. `Vec` is safe if no modifications: but we use Bytes for cleaner semantics

### Segment Guard Lifetime for Scatter-Gather

For Priority 3, segment guards must be held during async send:

```rust
struct UringConnection {
    in_flight_values: Vec<ValueRef>,  // Released on SendZc notif
}

fn on_send_notif(&mut self) {
    self.in_flight_values.clear();  // Release segment guards
}
```

This prevents segment eviction while kernel is reading from segment memory.

### Backpressure Considerations

Holding segment guards during sends creates backpressure coupling:
- Slow clients hold guards longer
- Guards block segment eviction
- Could cause memory pressure under load

Mitigations:
- Limit concurrent scatter-gather sends per connection
- Fall back to copy-based send when limit reached
- Timeout on guard hold (with graceful degradation)
