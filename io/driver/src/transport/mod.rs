//! Transport layer abstraction.
//!
//! This module provides a `Transport` trait that abstracts over raw TCP
//! and TLS-encrypted connections, allowing protocol implementations
//! to work with either.

mod plain;

#[cfg(feature = "tls")]
mod tls;

pub use plain::PlainTransport;

#[cfg(feature = "tls")]
pub use tls::{TlsConfig, TlsTransport};

use std::io;

/// Transport state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportState {
    /// Transport is performing handshake (TLS only).
    Handshaking,
    /// Transport is ready for application data.
    Ready,
    /// Transport encountered an error.
    Error,
    /// Transport is closed.
    Closed,
}

/// Abstraction over raw TCP and TLS transports.
///
/// This trait provides a completion-based interface for sending and
/// receiving data, compatible with ioru's event loop model.
pub trait Transport {
    /// Get the current transport state.
    fn state(&self) -> TransportState;

    /// Check if the transport is ready for application data.
    fn is_ready(&self) -> bool {
        self.state() == TransportState::Ready
    }

    /// Queue data to be sent.
    ///
    /// Returns the number of bytes queued, or `WouldBlock` if the
    /// send buffer is full.
    fn send(&mut self, data: &[u8]) -> io::Result<usize>;

    /// Read available decrypted data.
    ///
    /// Returns the number of bytes read, or `WouldBlock` if no data
    /// is available.
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize>;

    /// Process raw data received from the socket.
    ///
    /// For TLS, this decrypts the data. For plain, this just buffers it.
    /// Call this when you receive data from ioru.
    fn on_recv(&mut self, data: &[u8]) -> io::Result<()>;

    /// Get data that needs to be sent on the socket.
    ///
    /// For TLS, this returns encrypted data. For plain, this returns
    /// the queued application data.
    fn pending_send(&self) -> &[u8];

    /// Mark bytes as sent on the socket.
    fn advance_send(&mut self, n: usize);

    /// Check if there's pending data to send.
    fn has_pending_send(&self) -> bool {
        !self.pending_send().is_empty()
    }

    /// Initiate shutdown.
    fn shutdown(&mut self) -> io::Result<()>;
}

/// Result of processing incoming data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessResult {
    /// Need more data to continue.
    NeedMoreData,
    /// Have application data ready to read.
    DataReady,
    /// Handshake completed (TLS only).
    HandshakeComplete,
    /// Connection is closing.
    Closing,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transport_state_variants() {
        let handshaking = TransportState::Handshaking;
        let ready = TransportState::Ready;
        let error = TransportState::Error;
        let closed = TransportState::Closed;

        assert_eq!(handshaking, TransportState::Handshaking);
        assert_eq!(ready, TransportState::Ready);
        assert_eq!(error, TransportState::Error);
        assert_eq!(closed, TransportState::Closed);
    }

    #[test]
    fn test_transport_state_equality() {
        assert_eq!(TransportState::Ready, TransportState::Ready);
        assert_ne!(TransportState::Ready, TransportState::Error);
    }

    #[test]
    fn test_transport_state_clone() {
        let state1 = TransportState::Ready;
        let state2 = state1;
        assert_eq!(state1, state2);
    }

    #[test]
    fn test_transport_state_debug() {
        let state = TransportState::Handshaking;
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("Handshaking"));
    }

    #[test]
    fn test_process_result_variants() {
        let need_more = ProcessResult::NeedMoreData;
        let data_ready = ProcessResult::DataReady;
        let handshake = ProcessResult::HandshakeComplete;
        let closing = ProcessResult::Closing;

        assert_eq!(need_more, ProcessResult::NeedMoreData);
        assert_eq!(data_ready, ProcessResult::DataReady);
        assert_eq!(handshake, ProcessResult::HandshakeComplete);
        assert_eq!(closing, ProcessResult::Closing);
    }

    #[test]
    fn test_process_result_equality() {
        assert_eq!(ProcessResult::DataReady, ProcessResult::DataReady);
        assert_ne!(ProcessResult::DataReady, ProcessResult::NeedMoreData);
    }

    #[test]
    fn test_process_result_clone() {
        let result1 = ProcessResult::DataReady;
        let result2 = result1;
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_process_result_debug() {
        let result = ProcessResult::HandshakeComplete;
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("HandshakeComplete"));
    }
}
