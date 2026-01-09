//! Driver builder with fluent API.

use crate::driver::IoDriver;
use crate::types::IoEngine;
use std::io;

/// Builder for creating an I/O driver with custom configuration.
///
/// # Example
///
/// ```ignore
/// use io_driver::{Driver, IoEngine};
///
/// let driver = Driver::builder()
///     .engine(IoEngine::Auto)
///     .buffer_size(16 * 1024)
///     .buffer_count(2048)
///     .build()?;
/// ```
#[derive(Debug, Clone)]
pub struct DriverBuilder {
    engine: IoEngine,
    buffer_size: usize,
    buffer_count: u16,
    sq_depth: u32,
    max_connections: u32,
    sqpoll: bool,
    recv_mode: crate::types::RecvMode,
}

impl Default for DriverBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DriverBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            engine: IoEngine::Auto,
            buffer_size: 16 * 1024, // 16KB (TLS max record size)
            buffer_count: 2048,     // Enough for 1024 connections (2 buffers each)
            sq_depth: 1024,         // io_uring submission queue depth
            max_connections: 8192,  // Maximum registered connections
            sqpoll: false,          // SQPOLL disabled by default
            recv_mode: crate::types::RecvMode::default(),
        }
    }

    /// Set the I/O engine to use.
    ///
    /// - `Auto`: Automatically select the best backend (default)
    /// - `Mio`: Use mio (epoll/kqueue)
    /// - `Uring`: Use io_uring (Linux only)
    pub fn engine(mut self, engine: IoEngine) -> Self {
        self.engine = engine;
        self
    }

    /// Set the size of each buffer in the buffer pool.
    ///
    /// For io_uring, this affects the ring-provided buffer size.
    /// Default: 16KB (TLS max record size)
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Set the number of buffers in the pool.
    ///
    /// For io_uring, this must be a power of 2.
    /// Default: 2048 (enough for 1024 connections with 2 buffers each)
    pub fn buffer_count(mut self, count: u16) -> Self {
        self.buffer_count = count;
        self
    }

    /// Set the io_uring submission queue depth.
    ///
    /// Only applies to the io_uring backend.
    /// Default: 1024
    pub fn sq_depth(mut self, depth: u32) -> Self {
        self.sq_depth = depth;
        self
    }

    /// Set the maximum number of connections that can be registered.
    ///
    /// For io_uring, this determines the size of the registered file table.
    /// Default: 8192
    pub fn max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }

    /// Enable or disable SQPOLL mode for io_uring.
    ///
    /// SQPOLL uses a kernel thread to poll the submission queue,
    /// reducing latency but requiring CAP_SYS_NICE or root.
    /// Default: false
    pub fn sqpoll(mut self, enabled: bool) -> Self {
        self.sqpoll = enabled;
        self
    }

    /// Set the recv mode for io_uring.
    ///
    /// - `Multishot` (default): The driver automatically submits multishot recv
    ///   operations using ring-provided buffers, producing `CompletionKind::Recv`
    ///   events with data copied to an internal buffer.
    ///
    /// - `SingleShot`: The driver does not automatically start recv operations.
    ///   The caller must manually submit recv operations using `submit_recv()`,
    ///   which produces `CompletionKind::RecvComplete` events.
    ///
    /// Only applies to the io_uring backend.
    /// Default: Multishot
    pub fn recv_mode(mut self, mode: crate::types::RecvMode) -> Self {
        self.recv_mode = mode;
        self
    }

    /// Build the driver with the configured settings.
    pub fn build(self) -> io::Result<Box<dyn IoDriver>> {
        match self.engine {
            IoEngine::Auto => {
                #[cfg(all(target_os = "linux", feature = "io_uring"))]
                {
                    if crate::uring::is_supported() {
                        return self.build_uring();
                    }
                }
                self.build_mio()
            }
            IoEngine::Mio => self.build_mio(),
            IoEngine::Uring => {
                #[cfg(all(target_os = "linux", feature = "io_uring"))]
                {
                    if !crate::uring::is_supported() {
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "io_uring is not supported on this kernel (requires 6.0+)",
                        ));
                    }
                    self.build_uring()
                }
                #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
                {
                    Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "io_uring is only available on Linux with the io_uring feature",
                    ))
                }
            }
        }
    }

    fn build_mio(self) -> io::Result<Box<dyn IoDriver>> {
        Ok(Box::new(crate::mio::MioDriver::with_config(
            self.buffer_size,
            self.max_connections as usize,
        )?))
    }

    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    fn build_uring(self) -> io::Result<Box<dyn IoDriver>> {
        Ok(Box::new(crate::uring::UringDriver::with_config(
            self.sq_depth,
            self.buffer_size,
            self.buffer_count,
            self.max_connections,
            self.sqpoll,
            self.recv_mode,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_new() {
        let builder = DriverBuilder::new();
        // Check defaults
        assert_eq!(builder.engine, IoEngine::Auto);
        assert_eq!(builder.buffer_size, 16 * 1024);
        assert_eq!(builder.buffer_count, 2048);
        assert_eq!(builder.sq_depth, 1024);
        assert_eq!(builder.max_connections, 8192);
        assert!(!builder.sqpoll);
        assert_eq!(builder.recv_mode, crate::types::RecvMode::Multishot);
    }

    #[test]
    fn test_builder_default() {
        let builder1 = DriverBuilder::new();
        let builder2 = DriverBuilder::default();
        assert_eq!(builder1.engine, builder2.engine);
        assert_eq!(builder1.buffer_size, builder2.buffer_size);
        assert_eq!(builder1.buffer_count, builder2.buffer_count);
    }

    #[test]
    fn test_builder_engine() {
        let builder = DriverBuilder::new().engine(IoEngine::Mio);
        assert_eq!(builder.engine, IoEngine::Mio);
    }

    #[test]
    fn test_builder_buffer_size() {
        let builder = DriverBuilder::new().buffer_size(32768);
        assert_eq!(builder.buffer_size, 32768);
    }

    #[test]
    fn test_builder_buffer_count() {
        let builder = DriverBuilder::new().buffer_count(512);
        assert_eq!(builder.buffer_count, 512);
    }

    #[test]
    fn test_builder_sq_depth() {
        let builder = DriverBuilder::new().sq_depth(512);
        assert_eq!(builder.sq_depth, 512);
    }

    #[test]
    fn test_builder_max_connections() {
        let builder = DriverBuilder::new().max_connections(16384);
        assert_eq!(builder.max_connections, 16384);
    }

    #[test]
    fn test_builder_sqpoll() {
        let builder = DriverBuilder::new().sqpoll(true);
        assert!(builder.sqpoll);
    }

    #[test]
    fn test_builder_recv_mode() {
        let builder = DriverBuilder::new().recv_mode(crate::types::RecvMode::SingleShot);
        assert_eq!(builder.recv_mode, crate::types::RecvMode::SingleShot);
    }

    #[test]
    fn test_builder_chaining() {
        let builder = DriverBuilder::new()
            .engine(IoEngine::Mio)
            .buffer_size(8192)
            .buffer_count(128)
            .sq_depth(128)
            .max_connections(4096)
            .sqpoll(true)
            .recv_mode(crate::types::RecvMode::SingleShot);

        assert_eq!(builder.engine, IoEngine::Mio);
        assert_eq!(builder.buffer_size, 8192);
        assert_eq!(builder.buffer_count, 128);
        assert_eq!(builder.sq_depth, 128);
        assert_eq!(builder.max_connections, 4096);
        assert!(builder.sqpoll);
        assert_eq!(builder.recv_mode, crate::types::RecvMode::SingleShot);
    }

    #[test]
    fn test_builder_clone() {
        let builder1 = DriverBuilder::new().engine(IoEngine::Mio).buffer_size(4096);
        let builder2 = builder1.clone();
        assert_eq!(builder1.engine, builder2.engine);
        assert_eq!(builder1.buffer_size, builder2.buffer_size);
    }

    #[test]
    fn test_builder_debug() {
        let builder = DriverBuilder::new();
        let debug_str = format!("{:?}", builder);
        assert!(debug_str.contains("DriverBuilder"));
    }

    #[test]
    fn test_builder_build_mio() {
        let result = DriverBuilder::new().engine(IoEngine::Mio).build();
        assert!(result.is_ok());
    }

    #[test]
    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    fn test_builder_build_uring_unsupported() {
        let result = DriverBuilder::new().engine(IoEngine::Uring).build();
        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        }
    }

    #[test]
    fn test_builder_build_auto() {
        // Auto should always succeed (falls back to mio)
        let result = DriverBuilder::new().engine(IoEngine::Auto).build();
        assert!(result.is_ok());
    }
}
