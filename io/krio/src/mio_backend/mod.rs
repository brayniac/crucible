//! mio/epoll fallback backend for krio.
//!
//! This module provides a readiness-based event loop using mio as a fallback
//! for environments where io_uring is unavailable (e.g., older Linux kernels).
//!
//! # Differences from io_uring backend
//!
//! - **No zero-copy sends**: `SendMsgZc` is not available; all data is copied
//!   through the kernel send buffer. `SendGuard` data is gathered into the copy
//!   pool before sending; guards are dropped immediately after gathering.
//! - **No ring-provided buffers**: Recv uses a shared per-worker read buffer
//!   instead of kernel-managed buffer rings.
//! - **No fixed file table**: Connections use raw fds with standard epoll.
//! - **No IO_LINK chains**: Sends are dispatched individually.
//! - **No NVMe passthrough or direct I/O**: These require io_uring.
//! - **No async API**: Only the callback-based `EventHandler` is supported.
//!
//! The callback-based `EventHandler` trait has the same API as the io_uring
//! backend, so application code can be compiled against either backend without
//! changes.

pub mod ctx;
pub mod event_loop;
