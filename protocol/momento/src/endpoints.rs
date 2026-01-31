//! Momento endpoints discovery via HTTP API.
//!
//! This module provides functionality to discover Momento cache server addresses
//! by calling the Momento HTTP endpoints API. This is used for:
//!
//! - **Private endpoints**: Connect to Momento servers via VPC private endpoints
//! - **Load balancing**: Get multiple server addresses for connection distribution
//! - **Availability zone routing**: Route to servers in specific AZs for lower latency
//!
//! # Example
//!
//! ```ignore
//! use protocol_momento::{Credential, EndpointsFetcher};
//!
//! let credential = Credential::from_env()?;
//! let mut fetcher = EndpointsFetcher::new(&credential)?;
//!
//! // Drive the connection until we get addresses
//! loop {
//!     fetcher.drive(&mut tcp_stream)?;
//!     if let Some(addresses) = fetcher.take_addresses() {
//!         println!("Got addresses: {:?}", addresses);
//!         break;
//!     }
//! }
//! ```

use crate::Credential;
use http2::{
    Connection, ConnectionEvent, ConnectionState, HeaderField, TlsConfig, TlsTransport, Transport,
    TransportState,
};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};

/// Addresses returned from the endpoints API, organized by availability zone.
#[derive(Debug, Clone, Default)]
pub struct Addresses {
    /// Map of availability zone ID to list of addresses.
    azs: HashMap<String, Vec<SocketAddr>>,
}

impl Addresses {
    /// Create empty addresses.
    pub fn new() -> Self {
        Self {
            azs: HashMap::new(),
        }
    }

    /// Get all addresses, optionally filtered by availability zone.
    ///
    /// If `az_id` is provided, returns only addresses in that zone.
    /// If no addresses exist for that zone (or az_id is None), returns all addresses.
    pub fn for_az(&self, az_id: Option<&str>) -> Vec<SocketAddr> {
        if let Some(az) = az_id {
            if let Some(addrs) = self.azs.get(az) {
                if !addrs.is_empty() {
                    return addrs.clone();
                }
            }
        }
        // Return all addresses from all AZs
        self.azs.values().flatten().copied().collect()
    }

    /// Get all addresses regardless of availability zone.
    pub fn all(&self) -> Vec<SocketAddr> {
        self.azs.values().flatten().copied().collect()
    }

    /// Check if there are any addresses.
    pub fn is_empty(&self) -> bool {
        self.azs.values().all(|v| v.is_empty())
    }

    /// Get the availability zones.
    pub fn zones(&self) -> impl Iterator<Item = &str> {
        self.azs.keys().map(|s| s.as_str())
    }

    /// Parse addresses from JSON response.
    ///
    /// Expected format:
    /// ```json
    /// {
    ///   "us-west-2a": [{"socket_address": "10.0.1.5:9004"}],
    ///   "us-west-2b": [{"socket_address": "10.0.1.6:9004"}]
    /// }
    /// ```
    pub fn parse_json(json: &str) -> Option<Self> {
        let mut addresses = Addresses::new();

        // Simple JSON parsing without serde
        // Find each "az_id": [...] block
        let json = json.trim();
        if !json.starts_with('{') || !json.ends_with('}') {
            return None;
        }

        let inner = &json[1..json.len() - 1];
        let mut pos = 0;
        let bytes = inner.as_bytes();

        while pos < bytes.len() {
            // Skip whitespace
            while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
                pos += 1;
            }
            if pos >= bytes.len() {
                break;
            }

            // Expect "az_id"
            if bytes[pos] != b'"' {
                // Skip comma
                if bytes[pos] == b',' {
                    pos += 1;
                    continue;
                }
                return None;
            }
            pos += 1;

            // Read AZ ID
            let az_start = pos;
            while pos < bytes.len() && bytes[pos] != b'"' {
                pos += 1;
            }
            if pos >= bytes.len() {
                return None;
            }
            let az_id = String::from_utf8_lossy(&bytes[az_start..pos]).to_string();
            pos += 1;

            // Skip : and whitespace
            while pos < bytes.len() && (bytes[pos] == b':' || bytes[pos].is_ascii_whitespace()) {
                pos += 1;
            }

            // Expect [
            if pos >= bytes.len() || bytes[pos] != b'[' {
                return None;
            }
            pos += 1;

            // Parse array of addresses
            let mut az_addrs = Vec::new();
            while pos < bytes.len() {
                // Skip whitespace
                while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
                    pos += 1;
                }
                if pos >= bytes.len() {
                    return None;
                }

                // Check for end of array
                if bytes[pos] == b']' {
                    pos += 1;
                    break;
                }

                // Skip comma
                if bytes[pos] == b',' {
                    pos += 1;
                    continue;
                }

                // Expect { for address object
                if bytes[pos] != b'{' {
                    return None;
                }

                // Find the matching }
                let obj_start = pos;
                pos += 1;
                let mut depth = 1;
                while pos < bytes.len() && depth > 0 {
                    match bytes[pos] {
                        b'{' => depth += 1,
                        b'}' => depth -= 1,
                        _ => {}
                    }
                    pos += 1;
                }

                // Parse the object as a string
                let obj_str = std::str::from_utf8(&bytes[obj_start..pos]).ok()?;
                if let Some(addr) = parse_socket_address(obj_str) {
                    az_addrs.push(addr);
                }
            }

            if !az_addrs.is_empty() {
                addresses.azs.insert(az_id, az_addrs);
            }
        }

        Some(addresses)
    }
}

/// Parse "socket_address": "ip:port" from a JSON object string.
/// Returns the parsed SocketAddr if found.
fn parse_socket_address(obj_str: &str) -> Option<SocketAddr> {
    // Find "socket_address"
    let key = "\"socket_address\"";
    let key_pos = obj_str.find(key)?;
    let after_key = &obj_str[key_pos + key.len()..];

    // Skip : and whitespace
    let after_colon = after_key.trim_start().strip_prefix(':')?.trim_start();

    // Get quoted value
    let value_start = after_colon.strip_prefix('"')?;
    let value_end = value_start.find('"')?;
    let addr_str = &value_start[..value_end];

    // Parse as SocketAddr
    addr_str.parse().ok()
}

/// Fetches Momento cache server addresses via the HTTP endpoints API.
///
/// This is a synchronous, blocking implementation that uses Crucible's
/// http2 crate for HTTP/2 communication.
pub struct EndpointsFetcher {
    /// HTTP endpoint to call.
    http_endpoint: String,
    /// Authorization token.
    auth_token: String,
    /// Whether to request private endpoints.
    use_private: bool,
    /// TLS hostname for SNI.
    tls_host: String,
}

impl EndpointsFetcher {
    /// Create a new endpoints fetcher.
    ///
    /// # Arguments
    /// * `credential` - Momento credential with endpoint and token
    /// * `use_private` - Whether to request private endpoints
    pub fn new(credential: &Credential, use_private: bool) -> Self {
        Self {
            http_endpoint: credential.http_endpoint().to_string(),
            auth_token: credential.token().to_string(),
            use_private,
            tls_host: credential.tls_host().to_string(),
        }
    }

    /// Fetch addresses synchronously (blocking).
    ///
    /// This method blocks until addresses are fetched or an error occurs.
    pub fn fetch(&self) -> io::Result<Addresses> {
        // Parse endpoint to get host:port
        let http_host = self
            .http_endpoint
            .strip_prefix("https://")
            .unwrap_or(&self.http_endpoint);

        // Default to port 443 for HTTPS
        let addr_str = if http_host.contains(':') {
            http_host.to_string()
        } else {
            format!("{}:443", http_host)
        };

        // Resolve and connect
        let addr = addr_str
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "could not resolve endpoint"))?;

        let mut stream = TcpStream::connect(addr)?;
        stream.set_nonblocking(true)?;
        stream.set_nodelay(true)?;

        // Create TLS transport with HTTP/2 ALPN
        let tls_config = TlsConfig::http2()?;
        let mut tls = TlsTransport::new(&tls_config, &self.tls_host)?;

        // Drive TLS handshake
        self.drive_tls_handshake(&mut tls, &mut stream)?;

        // Create HTTP/2 connection
        let mut conn = Connection::new(tls);

        // Drive HTTP/2 connection setup
        self.drive_http2_setup(&mut conn, &mut stream)?;

        // Send request
        let path = if self.use_private {
            "/endpoints?private=true"
        } else {
            "/endpoints"
        };

        let host = self
            .http_endpoint
            .strip_prefix("https://")
            .unwrap_or(&self.http_endpoint);

        let headers = [
            HeaderField::new(b":method", b"GET"),
            HeaderField::new(b":path", path.as_bytes()),
            HeaderField::new(b":scheme", b"https"),
            HeaderField::new(b":authority", host.as_bytes()),
            HeaderField::new(b"authorization", self.auth_token.as_bytes()),
        ];

        let stream_id = conn.start_request(&headers, true)?;

        // Drive until we get response
        let mut response_headers = Vec::new();
        let mut response_body = Vec::new();
        let mut got_response = false;

        let mut buf = [0u8; 16384];
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);

        while !got_response && std::time::Instant::now() < deadline {
            // Send pending data
            self.flush_connection(&mut conn, &mut stream)?;

            // Read data
            match stream.read(&mut buf) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed",
                    ));
                }
                Ok(n) => {
                    conn.on_recv(&buf[..n])?;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(e) => return Err(e),
            }

            // Process events
            for event in conn.poll_events() {
                match event {
                    ConnectionEvent::Headers {
                        stream_id: sid,
                        headers,
                        ..
                    } if sid == stream_id => {
                        response_headers = headers;
                    }
                    ConnectionEvent::Data {
                        stream_id: sid,
                        data,
                        end_stream,
                    } if sid == stream_id => {
                        response_body.extend_from_slice(&data);
                        if end_stream {
                            got_response = true;
                        }
                    }
                    ConnectionEvent::Error(e) => {
                        return Err(io::Error::new(io::ErrorKind::Other, format!("{:?}", e)));
                    }
                    _ => {}
                }
            }
        }

        if !got_response {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timeout waiting for response",
            ));
        }

        // Check status
        let status = response_headers
            .iter()
            .find(|h| h.name == b":status")
            .map(|h| h.value.as_slice())
            .unwrap_or(b"0");

        if status != b"200" {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "HTTP {} from endpoints API",
                    String::from_utf8_lossy(status)
                ),
            ));
        }

        // Parse JSON response
        let json = String::from_utf8_lossy(&response_body);
        Addresses::parse_json(&json).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "failed to parse endpoints JSON")
        })
    }

    fn drive_tls_handshake(
        &self,
        tls: &mut TlsTransport,
        stream: &mut TcpStream,
    ) -> io::Result<()> {
        let mut buf = [0u8; 16384];
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);

        while std::time::Instant::now() < deadline {
            // Send pending TLS data
            while tls.has_pending_send() {
                let data = tls.pending_send();
                match stream.write(data) {
                    Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
                    Ok(n) => tls.advance_send(n),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e),
                }
            }

            // Check if handshake complete
            if tls.state() == TransportState::Ready {
                return Ok(());
            }
            if tls.state() == TransportState::Error {
                return Err(io::Error::other("TLS handshake failed"));
            }

            // Read data
            match stream.read(&mut buf) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                Ok(n) => tls.on_recv(&buf[..n])?,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(e) => return Err(e),
            }
        }

        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "TLS handshake timeout",
        ))
    }

    fn drive_http2_setup<T: Transport>(
        &self,
        conn: &mut Connection<T>,
        stream: &mut TcpStream,
    ) -> io::Result<()> {
        let mut buf = [0u8; 16384];
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);

        while std::time::Instant::now() < deadline {
            // Send pending data
            self.flush_connection(conn, stream)?;

            // Check if ready
            if conn.state() == ConnectionState::Open {
                return Ok(());
            }

            // Read data
            match stream.read(&mut buf) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                Ok(n) => conn.on_recv(&buf[..n])?,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(e) => return Err(e),
            }

            // Process events (discard for now)
            let _ = conn.poll_events();
        }

        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "HTTP/2 setup timeout",
        ))
    }

    fn flush_connection<T: Transport>(
        &self,
        conn: &mut Connection<T>,
        stream: &mut TcpStream,
    ) -> io::Result<()> {
        while conn.has_pending_send() {
            let data = conn.pending_send();
            match stream.write(data) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
                Ok(n) => conn.advance_send(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_addresses_empty() {
        let addrs = Addresses::new();
        assert!(addrs.is_empty());
        assert!(addrs.all().is_empty());
    }

    #[test]
    fn test_addresses_for_az() {
        let mut addrs = Addresses::new();
        let addr1: SocketAddr = "10.0.1.5:9004".parse().unwrap();
        let addr2: SocketAddr = "10.0.1.6:9004".parse().unwrap();

        addrs.azs.insert("us-west-2a".to_string(), vec![addr1]);
        addrs.azs.insert("us-west-2b".to_string(), vec![addr2]);

        // Specific AZ
        assert_eq!(addrs.for_az(Some("us-west-2a")), vec![addr1]);
        assert_eq!(addrs.for_az(Some("us-west-2b")), vec![addr2]);

        // Unknown AZ returns all
        let all = addrs.for_az(Some("unknown"));
        assert_eq!(all.len(), 2);

        // None returns all
        let all = addrs.for_az(None);
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_addresses_parse_json() {
        let json = r#"{
            "us-west-2a": [{"socket_address": "10.0.1.5:9004"}],
            "us-west-2b": [{"socket_address": "10.0.1.6:9004"}, {"socket_address": "10.0.1.7:9004"}]
        }"#;

        let addrs = Addresses::parse_json(json).unwrap();
        assert!(!addrs.is_empty());

        let az_a = addrs.for_az(Some("us-west-2a"));
        assert_eq!(az_a.len(), 1);
        assert_eq!(az_a[0].to_string(), "10.0.1.5:9004");

        let az_b = addrs.for_az(Some("us-west-2b"));
        assert_eq!(az_b.len(), 2);
    }

    #[test]
    fn test_addresses_parse_json_empty() {
        let json = "{}";
        let addrs = Addresses::parse_json(json).unwrap();
        assert!(addrs.is_empty());
    }

    #[test]
    fn test_addresses_parse_json_invalid() {
        assert!(Addresses::parse_json("not json").is_none());
        assert!(Addresses::parse_json("[1,2,3]").is_none());
    }

    #[test]
    fn test_parse_socket_address() {
        let obj = r#"{"socket_address": "10.0.1.5:9004"}"#;
        let addr = parse_socket_address(obj).unwrap();
        assert_eq!(addr.to_string(), "10.0.1.5:9004");
    }
}
