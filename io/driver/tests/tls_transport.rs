//! Integration tests for TLS transport.
//!
//! These tests verify TLS transport functionality using the public API.

use io_driver::transport::{TlsConfig, TlsTransport, Transport, TransportState};

#[test]
fn test_tls_config_new() {
    let config = TlsConfig::new().unwrap();
    // Should create a valid config with default root certificates
    let transport = TlsTransport::new(&config, "example.com").unwrap();
    assert_eq!(transport.state(), TransportState::Handshaking);
}

#[test]
fn test_tls_config_http2() {
    let config = TlsConfig::http2().unwrap();
    let transport = TlsTransport::new(&config, "example.com").unwrap();
    assert_eq!(transport.state(), TransportState::Handshaking);
    assert!(transport.has_pending_send());
}

#[test]
fn test_tls_config_with_alpn() {
    let protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let config = TlsConfig::with_alpn(protocols).unwrap();
    let transport = TlsTransport::new(&config, "example.com").unwrap();
    assert!(transport.has_pending_send());
}

#[test]
fn test_tls_config_default() {
    let config = TlsConfig::default();
    let transport = TlsTransport::new(&config, "example.com").unwrap();
    assert!(!transport.is_ready());
}

#[test]
fn test_tls_transport_invalid_server_name() {
    let config = TlsConfig::new().unwrap();
    // Empty server name is invalid
    let result = TlsTransport::new(&config, "");
    assert!(result.is_err());
}

#[test]
fn test_tls_transport_send_during_handshake() {
    let config = TlsConfig::new().unwrap();
    let mut transport = TlsTransport::new(&config, "example.com").unwrap();

    // Can't send during handshake
    let result = transport.send(b"hello");
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotConnected);
}

#[test]
fn test_tls_transport_recv_empty() {
    let config = TlsConfig::new().unwrap();
    let mut transport = TlsTransport::new(&config, "example.com").unwrap();

    let mut buf = [0u8; 1024];
    let result = transport.recv(&mut buf);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
}

#[test]
fn test_tls_transport_shutdown() {
    let config = TlsConfig::new().unwrap();
    let mut transport = TlsTransport::new(&config, "example.com").unwrap();

    transport.shutdown().unwrap();
    assert_eq!(transport.state(), TransportState::Closed);
    // Should have close_notify data to send
    assert!(transport.has_pending_send());
}

#[test]
fn test_tls_transport_pending_send() {
    let config = TlsConfig::new().unwrap();
    let transport = TlsTransport::new(&config, "example.com").unwrap();

    let pending = transport.pending_send();
    assert!(!pending.is_empty());
}

#[test]
fn test_tls_transport_advance_send() {
    let config = TlsConfig::new().unwrap();
    let mut transport = TlsTransport::new(&config, "example.com").unwrap();

    let initial_len = transport.pending_send().len();
    assert!(initial_len > 0);

    // Advance partially
    transport.advance_send(10);
    assert_eq!(transport.pending_send().len(), initial_len - 10);

    // Advance remaining
    transport.advance_send(initial_len - 10);
    assert!(transport.pending_send().is_empty());
}

#[test]
fn test_tls_transport_on_recv_empty() {
    let config = TlsConfig::new().unwrap();
    let mut transport = TlsTransport::new(&config, "example.com").unwrap();

    let result = transport.on_recv(&[]);
    assert!(result.is_ok());
}

#[test]
fn test_tls_transport_on_recv_garbage() {
    let config = TlsConfig::new().unwrap();
    let mut transport = TlsTransport::new(&config, "example.com").unwrap();

    // Garbage data should cause TLS error
    let result = transport.on_recv(b"not valid TLS data");
    assert!(result.is_err());
    assert_eq!(transport.state(), TransportState::Error);
}

#[test]
fn test_tls_transport_alpn_protocol_before_handshake() {
    let config = TlsConfig::http2().unwrap();
    let transport = TlsTransport::new(&config, "example.com").unwrap();

    // ALPN not negotiated yet
    assert!(transport.alpn_protocol().is_none());
}

#[test]
fn test_tls_transport_state_machine() {
    let config = TlsConfig::new().unwrap();
    let mut transport = TlsTransport::new(&config, "example.com").unwrap();

    // Initial state
    assert_eq!(transport.state(), TransportState::Handshaking);
    assert!(!transport.is_ready());

    // Has handshake data to send
    assert!(transport.has_pending_send());
    let handshake_data = transport.pending_send().to_vec();
    assert!(!handshake_data.is_empty());

    // Advance all handshake data (simulate sending)
    transport.advance_send(handshake_data.len());
    assert!(!transport.has_pending_send());

    // Shutdown
    transport.shutdown().unwrap();
    assert_eq!(transport.state(), TransportState::Closed);
}

#[test]
fn test_tls_transport_multiple_on_recv() {
    let config = TlsConfig::new().unwrap();
    let mut transport = TlsTransport::new(&config, "example.com").unwrap();

    // Multiple empty on_recv calls should be fine
    for _ in 0..5 {
        let result = transport.on_recv(&[]);
        assert!(result.is_ok());
    }
}

#[test]
fn test_tls_transport_different_server_names() {
    let config = TlsConfig::new().unwrap();

    // Various valid server names
    let server_names = [
        "example.com",
        "test.example.org",
        "sub.domain.com",
        "localhost",
    ];

    for name in server_names {
        let transport = TlsTransport::new(&config, name).unwrap();
        assert_eq!(transport.state(), TransportState::Handshaking);
    }
}

#[test]
fn test_tls_transport_alpn_protocols() {
    // Test with different ALPN configurations
    let configs = [
        TlsConfig::new().unwrap(),
        TlsConfig::http2().unwrap(),
        TlsConfig::with_alpn(vec![b"h2".to_vec()]).unwrap(),
        TlsConfig::with_alpn(vec![b"http/1.1".to_vec()]).unwrap(),
        TlsConfig::with_alpn(vec![b"h2".to_vec(), b"http/1.1".to_vec()]).unwrap(),
    ];

    for config in configs {
        let transport = TlsTransport::new(&config, "example.com").unwrap();
        assert!(transport.has_pending_send());
    }
}

#[test]
fn test_tls_config_reuse() {
    let config = TlsConfig::new().unwrap();

    // Create multiple transports from the same config
    let transport1 = TlsTransport::new(&config, "example.com").unwrap();
    let transport2 = TlsTransport::new(&config, "test.com").unwrap();
    let transport3 = TlsTransport::new(&config, "localhost").unwrap();

    assert!(transport1.has_pending_send());
    assert!(transport2.has_pending_send());
    assert!(transport3.has_pending_send());
}

#[test]
fn test_tls_transport_handshake_data_format() {
    let config = TlsConfig::new().unwrap();
    let transport = TlsTransport::new(&config, "example.com").unwrap();

    let pending = transport.pending_send();

    // TLS ClientHello should start with record type 0x16 (handshake)
    assert!(!pending.is_empty());
    assert_eq!(pending[0], 0x16, "Should be TLS handshake record type");

    // Version should be TLS 1.0 (0x0301) in the record layer for compatibility
    // even when using TLS 1.2/1.3
    assert_eq!(pending[1], 0x03);
    assert!(pending[2] <= 0x03); // TLS version
}
