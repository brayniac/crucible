# Plan: NVMe io_uring Passthrough Support in Kompio

## Summary

Add NVMe io_uring passthrough (`IORING_OP_URING_CMD`) to kompio, enabling a
low-copy storage-to-network path: NVMe read → registered buffer → SendMsgZc.

## Approach: Single Ring with Big SQE/CQE

Switch kompio's ring from `IoUring<Entry, Entry>` (64B SQE, 16B CQE) to
`IoUring<Entry128, Entry32>` (128B SQE, 32B CQE). This is the simplest
approach because:

- The io-uring 0.7.11 crate already supports `Entry128`, `Entry32`, and
  `UringCmd80` — no dependency changes needed
- `Entry` implements `Into<Entry128>`, so all existing network opcodes
  (RecvMulti, Send, SendMsgZc, etc.) work unchanged on a Big ring
- Memory cost is +32 KB per worker (256 SQ × 64B extra + 1024 CQ × 16B extra),
  negligible vs the ~20 MB already allocated per worker for buffer pools
- Single ring means NVMe and network ops share registered buffers natively —
  no cross-ring coordination needed

## Changes by File

### 1. `io/kompio/src/ring.rs` — Ring setup and SQE submission

**Ring type change:**
- Change `ring: IoUring` to `ring: IoUring<Entry128, Entry32>`
- In `Ring::setup()`, the `IoUring::builder()` already infers
  `IORING_SETUP_SQE128 | IORING_SETUP_CQE32` from the type parameters

**push_sqe changes:**
- Change `push_sqe(&mut self, entry: Entry)` to accept `Entry128`
- All existing callers build `Entry` values — wrap with `.into()` at call sites
  (the `From<Entry> for Entry128` impl pads with zeros)

**New submission method:**
```rust
pub(crate) unsafe fn push_sqe128(&mut self, entry: Entry128) -> io::Result<()>
```
For NVMe commands that produce `Entry128` directly from `UringCmd80::build()`.

**New NVMe submit helpers:**
```rust
pub fn submit_nvme_read(
    &mut self, fd_index: u32, nsid: u32, lba: u64,
    num_blocks: u16, buf_index: u16, buf_addr: u64, buf_len: u32,
    user_data: u64,
) -> io::Result<()>

pub fn submit_nvme_write(
    &mut self, fd_index: u32, nsid: u32, lba: u64,
    num_blocks: u16, buf_index: u16, buf_addr: u64, buf_len: u32,
    user_data: u64,
) -> io::Result<()>
```

These construct `nvme_uring_cmd` structs (NVMe Read = opcode 0x02,
Write = opcode 0x01) and submit via `UringCmd80`.

### 2. `io/kompio/src/completion.rs` — OpTag and UserData

**New OpTag variant:**
```rust
NvmeCmd = 12,
```

**UserData encoding unchanged** — the existing 64-bit layout works:
- Bits 63..56: OpTag (NvmeCmd)
- Bits 55..32: device slot index (24 bits, reusing conn_index field)
- Bits 31..0: command sequence / buffer index (32 bits)

**CQE32 handling:**
Big CQEs return an extra 16 bytes. For NVMe, this contains the NVMe completion
queue entry (status, result). Add a helper to extract the NVMe-specific result
from the extra CQE data. The completion loop needs to pass this extra data
through for NvmeCmd completions.

### 3. `io/kompio/src/event_loop.rs` — Completion dispatch

**CQE iteration change:**
When using `Entry32` CQEs, the completion iterator yields `Entry32` values.
We need to extract both the standard fields (user_data, result, flags) and
the extra 16 bytes for NVMe completions. For non-NVMe ops, the extra bytes
are ignored.

**New dispatch arm:**
```rust
OpTag::NvmeCmd => self.handle_nvme_cmd(ud, result, flags, extra),
```

**New handler method:**
```rust
fn handle_nvme_cmd(&mut self, ud: UserData, result: i32, flags: u32, extra: &[u8; 16]) {
    let device_index = ud.conn_index();
    let seq = ud.payload();
    // Release command slot
    // Build NvmeCompletion from extra bytes (NVMe status, result)
    // Call handler.on_nvme_complete(ctx, device, completion)
}
```

**New EventLoop fields:**
```rust
nvme_devices: Vec<NvmeDeviceState>,   // Per-device state
nvme_cmd_slab: NvmeCmdSlab,           // In-flight command tracking
```

### 4. `io/kompio/src/handler.rs` — EventHandler trait and DriverCtx

**New EventHandler callback (with default no-op):**
```rust
fn on_nvme_complete(
    &mut self,
    _ctx: &mut DriverCtx,
    _device: NvmeDevice,
    _completion: NvmeCompletion,
) {}
```

**New DriverCtx methods:**
```rust
/// Open and register an NVMe device. Returns a device handle.
pub fn open_nvme_device(&mut self, path: &str) -> io::Result<NvmeDevice>

/// Submit an NVMe read into a registered buffer.
pub fn nvme_read(
    &mut self, device: NvmeDevice, lba: u64, num_blocks: u16,
    buf_index: u16, buf_offset: usize, buf_len: u32,
) -> io::Result<u32>  // returns command sequence number

/// Submit an NVMe write from a registered buffer.
pub fn nvme_write(
    &mut self, device: NvmeDevice, lba: u64, num_blocks: u16,
    buf_index: u16, buf_offset: usize, buf_len: u32,
) -> io::Result<u32>

/// Close an NVMe device.
pub fn close_nvme_device(&mut self, device: NvmeDevice) -> io::Result<()>
```

### 5. `io/kompio/src/nvme.rs` — New module for NVMe types

**NVMe command struct (matches kernel `nvme_uring_cmd`):**
```rust
#[repr(C)]
pub struct NvmeUringCmd {
    pub opcode: u8,
    pub flags: u8,
    pub rsvd1: u16,
    pub nsid: u32,
    pub cdw2: u32,
    pub cdw3: u32,
    pub metadata: u64,
    pub addr: u64,
    pub metadata_len: u32,
    pub data_len: u32,
    pub cdw10: u32,
    pub cdw11: u32,
    pub cdw12: u32,
    pub cdw13: u32,
    pub cdw14: u32,
    pub cdw15: u32,
    pub timeout_ms: u32,
    pub rsvd2: u32,
}
```
This is 72 bytes, fits in the 80-byte `UringCmd80` command field.

**NVMe command opcodes:**
```rust
pub const NVME_CMD_READ: u8 = 0x02;
pub const NVME_CMD_WRITE: u8 = 0x01;
pub const NVME_CMD_FLUSH: u8 = 0x00;
```

**NVMe uring_cmd sub-opcodes:**
```rust
pub const NVME_URING_CMD_IO: u32 = 0;
pub const NVME_URING_CMD_IO_VEC: u32 = 1;
```

**Device state:**
```rust
pub struct NvmeDevice {
    pub(crate) index: u16,  // Slot in device table
}

pub(crate) struct NvmeDeviceState {
    pub fd_index: u32,       // Index in io_uring fixed file table
    pub nsid: u32,           // Namespace ID (usually 1)
    pub block_size: u32,     // Logical block size (512 or 4096)
    pub max_transfer: u32,   // MDTS in bytes
    pub active: bool,
    pub in_flight: u32,      // Commands in flight
}
```

**Command tracking slab:**
```rust
pub(crate) struct NvmeCmdSlab {
    entries: Vec<NvmeCmdEntry>,
    free_list: Vec<u16>,
}

struct NvmeCmdEntry {
    device_index: u16,
    buf_index: u16,
    in_use: bool,
}
```

**NVMe completion result:**
```rust
pub struct NvmeCompletion {
    pub seq: u32,            // Command sequence number
    pub status: u16,         // NVMe status code
    pub result: u64,         // Command-specific result
    pub device: NvmeDevice,
}
```

### 6. `io/kompio/src/config.rs` — Configuration

**New config fields:**
```rust
pub struct NvmeConfig {
    /// Maximum number of NVMe devices that can be opened.
    pub max_devices: u16,
    /// Maximum NVMe commands in flight per worker.
    pub max_commands_in_flight: u16,
}

// In Config:
pub nvme: Option<NvmeConfig>,
```

When `nvme` is `None` (default), no NVMe infrastructure is allocated, and the
ring stays at standard size. When `Some`, the ring uses Big SQE/CQE.

**Decision: Conditional Big ring.**
If `config.nvme.is_some()`, create `IoUring<Entry128, Entry32>`.
If `None`, keep `IoUring<Entry, Entry>`. This avoids any overhead for
users who don't need NVMe. Implemented via an enum:

```rust
pub(crate) enum RingInner {
    Standard(IoUring<Entry, Entry>),
    Big(IoUring<Entry128, Entry32>),
}
```

All submission helpers dispatch through this enum. The hot path (network ops)
pays only a branch prediction cost, which is essentially free since the ring
type is fixed for the lifetime of the worker.

### 7. `io/kompio/src/lib.rs` — Module registration

Add `pub mod nvme;` and re-export key types:
- `NvmeDevice`, `NvmeCompletion`, `NvmeConfig`

## Implementation Order

1. **`nvme.rs`** — Types only (NvmeUringCmd, NvmeDevice, NvmeDeviceState,
   NvmeCmdSlab, NvmeCompletion, NvmeConfig). No io_uring interaction yet.

2. **`config.rs`** — Add `nvme: Option<NvmeConfig>` to Config with `None`
   default.

3. **`ring.rs`** — Add `RingInner` enum, update `Ring::setup()` to check
   config.nvme, add `push_sqe128()`, add `submit_nvme_read/write()`. Update
   existing `push_sqe()` to dispatch through enum.

4. **`completion.rs`** — Add `OpTag::NvmeCmd`, update `from_u8()`.

5. **`event_loop.rs`** — Add `nvme_devices` and `nvme_cmd_slab` fields.
   Update CQE drain loop to handle `Entry32` extra data. Add
   `handle_nvme_cmd()`. Wire into `dispatch_cqe()`.

6. **`handler.rs`** — Add `on_nvme_complete()` to EventHandler trait (default
   no-op). Add DriverCtx methods: `open_nvme_device()`, `nvme_read()`,
   `nvme_write()`, `close_nvme_device()`.

7. **`lib.rs`** — Module declaration and re-exports.

8. **Tests** — Unit tests for NvmeUringCmd struct layout (size = 72, alignment),
   NvmeCmdSlab alloc/release, UserData encoding with NvmeCmd tag. Integration
   test requires an NVMe device, so gate behind `#[cfg(feature = "nvme-test")]`.

## What This Does NOT Include

- **Block allocator** — Managing which LBAs are free/used on the NVMe device.
  That's a cache-core concern (analogous to segment allocation), not an I/O
  framework concern. Kompio provides the I/O primitives; the cache server
  decides what to read/write and where.

- **Filesystem** — By design. Passthrough bypasses the filesystem entirely.

- **Cache integration** — Wiring the kompio NVMe API into the server's
  EventHandler to implement a persistent cache tier. That's a follow-up.

- **Admin commands** — Only I/O commands (read/write/flush) for now. Identify
  and other admin commands can be added later.

## Buffer Sharing Design

The registered buffer pool (`FixedBufferRegistry`) is ring-level. NVMe reads
and SendMsgZc sends reference the same registered buffers by index. The
lifecycle:

```
1. nvme_read(device, lba, blocks, buf_index=3, ...)
   → SQE submitted, buffer 3 owned by kernel/NVMe controller

2. on_nvme_complete(completion)
   → Buffer 3 now contains data from NVMe
   → Application can inspect/process the data

3. ctx.send_parts(conn).guard_registered(buf_index=3, ptr, len).submit()
   → SendMsgZc SQE submitted, buffer 3 owned by kernel/NIC

4. on_send_complete(result)
   → Initial send done, but buffer NOT yet safe

5. [ZC notification CQE arrives]
   → Buffer 3 is now safe to reuse for next NVMe read
```

The application (EventHandler implementation) is responsible for tracking
buffer ownership across these phases. Kompio provides the primitives; the
server manages the state machine.

## Kernel Requirements

- Existing: Linux 6.0+ (SendMsgZc)
- NVMe passthrough: Linux 5.19+ (already covered by 6.0 requirement)
- NVMe device permissions: `CAP_SYS_ADMIN` or appropriate udev rules on
  `/dev/ng*`
