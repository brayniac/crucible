use std::future::Future;
use std::pin::Pin;

use crate::handler::DriverCtx;
use crate::runtime::io::{ConnCtx, UdpCtx};

/// Trait for async connection handlers.
///
/// Consumers implement this trait to handle connections using `async fn` code
/// instead of the push-based [`EventHandler`](crate::handler::EventHandler)
/// callbacks. Each accepted connection gets a long-lived async task that runs
/// for the connection's lifetime.
///
/// # Example
///
/// ```ignore
/// struct EchoHandler;
///
/// impl AsyncEventHandler for EchoHandler {
///     fn on_accept(&self, conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
///         Box::pin(async move {
///             loop {
///                 let n = conn.with_data(|data| {
///                     // Echo back everything received.
///                     conn.send(data);
///                     data.len()
///                 }).await;
///                 if n == 0 {
///                     break;
///                 }
///             }
///         })
///     }
///
///     fn create_for_worker(worker_id: usize) -> Self {
///         EchoHandler
///     }
/// }
/// ```
pub trait AsyncEventHandler: Send + 'static {
    /// Handle an accepted connection. Runs for the connection's lifetime.
    /// When the returned future completes, the connection is closed.
    fn on_accept(&self, conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>>;

    /// Periodic tick (synchronous, same as EventHandler::on_tick).
    fn on_tick(&mut self, _ctx: &mut DriverCtx<'_>) {}

    /// Handle a bound UDP socket. Called once per UDP socket during startup.
    ///
    /// Return `Some(future)` to spawn a standalone task that handles datagrams
    /// for this socket. The future typically loops on [`UdpCtx::recv_from()`].
    /// Return `None` to ignore this UDP socket.
    fn on_udp_bind(&self, _udp: UdpCtx) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        None
    }

    /// Eventfd notification (synchronous).
    fn on_notify(&mut self, _ctx: &mut DriverCtx<'_>) {}

    /// Async entry point called once during worker startup.
    ///
    /// Return `Some(future)` to spawn a standalone task that runs before the
    /// event loop begins accepting connections. This is useful for client-only
    /// applications (no `.bind()`) that need to initiate outbound connections
    /// via [`connect()`](crate::connect).
    ///
    /// The future can call [`request_shutdown()`](crate::request_shutdown) to
    /// stop the worker when done. Return `None` (the default) to skip.
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        None
    }

    /// Create per-worker instance.
    fn create_for_worker(worker_id: usize) -> Self
    where
        Self: Sized;
}
