use std::cell::Cell;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::driver::Driver;
use crate::handler::ConnToken;
use crate::runtime::{Executor, IoResult};

/// Raw pointer to the driver + executor state, set before polling each task.
///
/// # Safety
///
/// This is safe because:
/// 1. Single-threaded: each worker thread has its own driver/executor.
/// 2. Scoped: set before poll, cleared after poll. The pointer is only
///    dereferenced within a Future::poll call.
/// 3. The pointed-to data lives on the worker thread's stack (in AsyncEventLoop::run).
pub(crate) struct DriverState {
    pub(crate) driver: *mut Driver,
    pub(crate) executor: *mut Executor,
}

thread_local! {
    pub(crate) static CURRENT_DRIVER: Cell<*mut DriverState> =
        const { Cell::new(std::ptr::null_mut()) };
}

/// Set the thread-local driver pointer before polling a task.
pub(crate) fn set_driver_state(state: *mut DriverState) {
    CURRENT_DRIVER.with(|c| c.set(state));
}

/// Clear the thread-local driver pointer after polling a task.
pub(crate) fn clear_driver_state() {
    CURRENT_DRIVER.with(|c| c.set(std::ptr::null_mut()));
}

/// Access the thread-local driver state. Panics if called outside the executor.
fn with_state<R>(f: impl FnOnce(&mut Driver, &mut Executor) -> R) -> R {
    let ptr = CURRENT_DRIVER.with(|c| c.get());
    assert!(!ptr.is_null(), "called outside executor");
    let state = unsafe { &mut *ptr };
    let driver = unsafe { &mut *state.driver };
    let executor = unsafe { &mut *state.executor };
    f(driver, executor)
}

/// The async equivalent of `ConnToken` + `DriverCtx`. Passed to the
/// connection's async fn, provides I/O methods.
///
/// A `ConnCtx` is valid for the lifetime of the connection's async task.
/// When the connection is closed, the task is dropped along with the `ConnCtx`.
#[derive(Clone, Copy)]
pub struct ConnCtx {
    pub(crate) conn_index: u32,
    pub(crate) generation: u32,
}

impl ConnCtx {
    /// Create a new ConnCtx for the given connection.
    pub(crate) fn new(conn_index: u32, generation: u32) -> Self {
        ConnCtx {
            conn_index,
            generation,
        }
    }

    /// Returns the connection slot index. Useful for indexing into per-connection arrays.
    pub fn index(&self) -> usize {
        self.conn_index as usize
    }

    /// Returns the `ConnToken` for this connection.
    pub fn token(&self) -> ConnToken {
        ConnToken::new(self.conn_index, self.generation)
    }

    // ── Recv ─────────────────────────────────────────────────────────

    /// Wait until recv data is available, then process it.
    ///
    /// The closure receives accumulated bytes and returns the number of bytes consumed.
    /// Resolves immediately when data is already buffered (cache-hit hot path).
    pub fn with_data<F: FnOnce(&[u8]) -> usize>(&self, f: F) -> WithDataFuture<F> {
        WithDataFuture {
            conn_index: self.conn_index,
            f: Some(f),
        }
    }

    // ── Send (synchronous / fire-and-forget) ─────────────────────────

    /// Send data (fire-and-forget). Copies data into the send pool and submits
    /// the SQE. One copy, no heap allocation, no future.
    ///
    /// This is the hot-path send for cache responses. The CQE is handled
    /// internally by the executor (resource cleanup + send queue advancement).
    /// Errors are only returned for submission failures (pool exhausted, SQ full).
    pub fn send(&self, data: &[u8]) -> io::Result<()> {
        with_state(|driver, _| {
            let mut ctx = driver.make_ctx();
            ctx.send(self.token(), data)
        })
    }

    /// Begin building a scatter-gather send with mixed copy + zero-copy guard parts.
    ///
    /// This mirrors `DriverCtx::send_parts()` — use `.copy(data)` for copied parts
    /// and `.guard(guard)` for zero-copy parts backed by `SendGuard`. Call `.submit()`
    /// to submit the SQE. Fire-and-forget: no future returned.
    pub fn send_parts(&self) -> AsyncSendBuilder {
        AsyncSendBuilder {
            token: self.token(),
        }
    }

    // ── Send (awaitable) ─────────────────────────────────────────────

    /// Send data and await completion. Copies data into the send pool, submits
    /// the SQE eagerly, then returns a future that resolves with the total bytes
    /// sent (or error).
    ///
    /// Use this when you need backpressure or send completion notification.
    /// For cache responses where you don't need to wait, use [`send()`](Self::send).
    pub fn send_await(&self, data: &[u8]) -> io::Result<SendFuture> {
        with_state(|driver, executor| {
            let mut ctx = driver.make_ctx();
            ctx.send(self.token(), data)?;
            executor.send_waiters[self.conn_index as usize] = true;
            Ok(SendFuture {
                conn_index: self.conn_index,
            })
        })
    }

    // ── Connect ──────────────────────────────────────────────────────

    /// Initiate an outbound TCP connection and await the result.
    ///
    /// Returns a new `ConnCtx` for the peer connection on success.
    pub fn connect(&self, addr: SocketAddr) -> io::Result<ConnectFuture> {
        with_state(|driver, executor| {
            let mut ctx = driver.make_ctx();
            let token = ctx
                .connect(addr)
                .map_err(|e| io::Error::other(e.to_string()))?;
            executor.connect_waiters[token.index as usize] = true;
            Ok(ConnectFuture {
                conn_index: token.index,
                generation: token.generation,
            })
        })
    }

    /// Initiate an outbound TCP connection with a timeout and await the result.
    pub fn connect_with_timeout(
        &self,
        addr: SocketAddr,
        timeout_ms: u64,
    ) -> io::Result<ConnectFuture> {
        with_state(|driver, executor| {
            let mut ctx = driver.make_ctx();
            let token = ctx
                .connect_with_timeout(addr, timeout_ms)
                .map_err(|e| io::Error::other(e.to_string()))?;
            executor.connect_waiters[token.index as usize] = true;
            Ok(ConnectFuture {
                conn_index: token.index,
                generation: token.generation,
            })
        })
    }

    // ── Close / metadata ─────────────────────────────────────────────

    /// Close this connection.
    pub fn close(&self) {
        let ptr = CURRENT_DRIVER.with(|c| c.get());
        if ptr.is_null() {
            return;
        }
        let state = unsafe { &mut *ptr };
        let driver = unsafe { &mut *state.driver };
        driver.close_connection(self.conn_index);
    }

    /// Access peer address.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        with_state(|driver, _| {
            let conn = driver.connections.get(self.conn_index)?;
            if conn.generation != self.generation {
                return None;
            }
            conn.peer_addr
        })
    }

    /// Check if this connection is outbound (initiated via connect).
    pub fn is_outbound(&self) -> bool {
        with_state(|driver, _| {
            driver
                .connections
                .get(self.conn_index)
                .map(|cs| cs.generation == self.generation && cs.outbound)
                .unwrap_or(false)
        })
    }
}

// ── AsyncSendBuilder ─────────────────────────────────────────────────

/// Builder for scatter-gather sends in the async API.
///
/// Wraps `DriverCtx::send_parts()` — call `.copy()` and `.guard()` to add
/// parts, then `.submit()`. This is a synchronous builder; the send is
/// fire-and-forget (no future).
pub struct AsyncSendBuilder {
    token: ConnToken,
}

impl AsyncSendBuilder {
    /// Build and submit the send by calling the provided closure with a
    /// `SendBuilder` from the `DriverCtx`.
    ///
    /// The closure receives the `SendBuilder` and should chain `.copy()` / `.guard()`
    /// calls then call `.submit()`.
    ///
    /// # Example
    /// ```ignore
    /// conn.send_parts().build(|b| {
    ///     b.copy(header).guard(value_guard).submit()
    /// })?;
    /// ```
    pub fn build<F>(self, f: F) -> io::Result<()>
    where
        F: FnOnce(crate::handler::SendBuilder<'_, '_>) -> io::Result<()>,
    {
        with_state(|driver, _| {
            let mut ctx = driver.make_ctx();
            let builder = ctx.send_parts(self.token);
            f(builder)
        })
    }
}

// ── WithDataFuture ───────────────────────────────────────────────────

/// Future returned by [`ConnCtx::with_data`].
pub struct WithDataFuture<F> {
    conn_index: u32,
    f: Option<F>,
}

impl<F: FnOnce(&[u8]) -> usize + Unpin> Future for WithDataFuture<F> {
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<usize> {
        with_state(|driver, executor| {
            let data = driver.accumulators.data(self.conn_index);
            if data.is_empty() {
                // No data available — register as recv waiter and park.
                executor.recv_waiters[self.conn_index as usize] = true;
                return Poll::Pending;
            }

            // Data available — call closure immediately (zero-overhead hot path).
            let f = self.f.take().expect("WithDataFuture polled after Ready");
            let consumed = f(data);
            driver.accumulators.consume(self.conn_index, consumed);
            Poll::Ready(consumed)
        })
    }
}

// ── SendFuture ───────────────────────────────────────────────────────

/// Future that awaits send completion. The SQE was already submitted eagerly
/// by [`ConnCtx::send_await`] — this future only waits for the CQE result.
/// No data stored in the future. No allocation.
pub struct SendFuture {
    conn_index: u32,
}

impl Future for SendFuture {
    type Output = io::Result<u32>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u32>> {
        with_state(|_driver, executor| {
            match executor.io_results[self.conn_index as usize].take() {
                Some(IoResult::Send(result)) => Poll::Ready(result),
                _ => {
                    // Not ready yet — re-register waiter.
                    executor.send_waiters[self.conn_index as usize] = true;
                    Poll::Pending
                }
            }
        })
    }
}

// ── ConnectFuture ────────────────────────────────────────────────────

/// Future that awaits an outbound TCP connection. The connect SQE was submitted
/// eagerly by [`ConnCtx::connect`] — this future waits for the CQE result.
pub struct ConnectFuture {
    conn_index: u32,
    generation: u32,
}

impl Future for ConnectFuture {
    type Output = io::Result<ConnCtx>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<ConnCtx>> {
        with_state(|_driver, executor| {
            match executor.io_results[self.conn_index as usize].take() {
                Some(IoResult::Connect(result)) => match result {
                    Ok(()) => Poll::Ready(Ok(ConnCtx::new(self.conn_index, self.generation))),
                    Err(e) => Poll::Ready(Err(e)),
                },
                _ => {
                    // Not ready yet — re-register waiter.
                    executor.connect_waiters[self.conn_index as usize] = true;
                    Poll::Pending
                }
            }
        })
    }
}
