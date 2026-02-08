//! Momento session for benchmarking Momento cache.
//!
//! Unlike RESP/Memcache sessions that use the IoDriver for I/O,
//! MomentoSession handles its own TLS+HTTP/2+gRPC stack internally.

use super::{ConnectionState, RequestResult, RequestType};
use crate::config::{Config, MomentoWireFormat};

use io_driver::{TlsConfig, TlsTransport, Transport, TransportState};
use protocol_momento::{
    CacheClient, CacheValue, CompletedOp, Credential, EndpointsFetcher, WireFormat,
};

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

/// Default fallback address when credential endpoint cannot be parsed.
const DEFAULT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 443);

/// A request in the pipeline waiting for a response.
#[derive(Debug)]
struct InFlightRequest {
    id: u64,
    request_type: RequestType,
    queued_at: Instant,
}

/// Momento session that handles TLS+HTTP/2+gRPC internally.
pub struct MomentoSession {
    /// The underlying TCP stream (owned by this session).
    stream: Option<TcpStream>,
    /// TLS transport (before HTTP/2 setup).
    tls_transport: Option<TlsTransport>,
    /// The Momento cache client (after connection is established).
    client: Option<CacheClient<TlsTransport>>,
    /// Cache name for operations.
    cache_name: String,
    /// TTL for SET operations.
    ttl: Duration,
    /// Connection state.
    state: ConnectionState,
    /// In-flight request tracking for latency measurement.
    in_flight: VecDeque<InFlightRequest>,
    /// Next request ID.
    next_id: u64,
    /// Maximum pipeline depth.
    max_pipeline_depth: usize,
    /// Credential for authentication.
    credential: Credential,
    /// Wire format (gRPC or protosocket).
    wire_format: WireFormat,
    /// Receive buffer for TLS handshake.
    recv_buf: Vec<u8>,
    /// Error message if connection failed.
    error: Option<String>,
    /// Private endpoint addresses (if using private endpoints).
    private_addresses: Vec<SocketAddr>,
    /// Index for round-robin address selection.
    address_index: usize,
}

impl MomentoSession {
    /// Create a new Momento session.
    pub fn new(config: &Config) -> io::Result<Self> {
        // Convert config wire format to protocol wire format
        let wire_format = match config.momento.wire_format {
            MomentoWireFormat::Grpc => WireFormat::Grpc,
            MomentoWireFormat::Protosocket => WireFormat::Protosocket,
        };

        // Get credential from environment
        let credential = Credential::from_env()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

        // Override endpoint if specified in config
        let credential = if let Some(ref endpoint) = config.momento.endpoint {
            Credential::with_endpoint(credential.token().to_string(), endpoint.clone())
        } else {
            credential
        };

        // Apply wire format to credential
        let credential = credential.with_wire_format(wire_format);

        // Fetch private endpoints if configured
        let private_addresses = if config.momento.use_private_endpoints {
            let fetcher = EndpointsFetcher::new(&credential, true);
            let addresses = fetcher.fetch().map_err(|e| {
                io::Error::other(format!("failed to fetch private endpoints: {}", e))
            })?;

            // Get AZ from config or environment variable
            let az_filter: Option<String> = config
                .momento
                .availability_zone
                .clone()
                .or_else(|| std::env::var("MOMENTO_AZ").ok());

            let addrs = addresses.for_az(az_filter.as_deref());
            if addrs.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "no private endpoints available",
                ));
            }
            addrs
        } else {
            Vec::new()
        };

        Ok(Self {
            stream: None,
            tls_transport: None,
            client: None,
            cache_name: config.momento.cache_name.clone(),
            ttl: Duration::from_secs(config.momento.ttl_seconds),
            state: ConnectionState::Disconnected,
            in_flight: VecDeque::with_capacity(config.connection.pipeline_depth),
            next_id: 0,
            max_pipeline_depth: config.connection.pipeline_depth,
            credential,
            wire_format,
            recv_buf: vec![0u8; 16384],
            error: None,
            private_addresses,
            address_index: 0,
        })
    }

    /// Get the target address.
    pub fn addr(&self) -> SocketAddr {
        // Parse from credential endpoint
        let host = self.credential.host();
        let port = self.credential.port();
        format!("{}:{}", host, port).parse().unwrap_or(DEFAULT_ADDR)
    }

    /// Connect to the Momento server.
    pub fn connect(&mut self) -> io::Result<()> {
        // Determine connection address
        let (addr, tls_host) = if !self.private_addresses.is_empty() {
            // Use private endpoint address (round-robin)
            let addr = self.private_addresses[self.address_index % self.private_addresses.len()];
            self.address_index = self.address_index.wrapping_add(1);
            // Use credential host for TLS SNI (certificate verification)
            (addr, self.credential.tls_host().to_string())
        } else {
            // Use credential endpoint (DNS resolution)
            let host = self.credential.host();
            let port = self.credential.port();
            let addr_str = format!("{}:{}", host, port);
            let addr = addr_str.to_socket_addrs()?.next().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "could not resolve endpoint")
            })?;
            (addr, host.to_string())
        };

        // Connect TCP
        let stream = TcpStream::connect(addr)?;
        stream.set_nonblocking(true)?;
        stream.set_nodelay(true)?;
        self.stream = Some(stream);

        // Create TLS transport with appropriate config for wire format
        // gRPC requires ALPN for HTTP/2, protosocket uses plain TLS
        let tls_config = match self.wire_format {
            WireFormat::Grpc => TlsConfig::http2()?,
            WireFormat::Protosocket => TlsConfig::new()?,
        };
        let transport = TlsTransport::new(&tls_config, &tls_host)?;
        self.tls_transport = Some(transport);

        self.state = ConnectionState::Connecting;
        Ok(())
    }

    /// Drive the connection (TLS handshake, HTTP/2 setup, etc.)
    /// Returns true if the connection is ready for requests.
    pub fn drive(&mut self) -> io::Result<bool> {
        match self.state {
            ConnectionState::Connecting => {
                // Drive TLS handshake
                if self.drive_tls_handshake()? {
                    // TLS ready, create CacheClient with appropriate transport
                    let transport = self.tls_transport.take().unwrap();
                    let mut client = match self.wire_format {
                        WireFormat::Grpc => {
                            CacheClient::with_transport(transport, self.credential.clone())
                        }
                        WireFormat::Protosocket => CacheClient::with_protosocket_transport(
                            transport,
                            self.credential.clone(),
                        ),
                    };
                    client.on_transport_ready()?;
                    self.client = Some(client);
                }

                // Drive I/O (HTTP/2 or protosocket) if client exists
                if self.client.is_some() {
                    self.drive_client_io()?;
                    if self.client.as_ref().map(|c| c.is_ready()).unwrap_or(false) {
                        self.state = ConnectionState::Connected;
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            ConnectionState::Connected => {
                // Drive any pending I/O
                self.drive_client_io()?;
                Ok(true)
            }
            ConnectionState::Disconnected => Ok(false),
        }
    }

    /// Drive TLS handshake to completion.
    fn drive_tls_handshake(&mut self) -> io::Result<bool> {
        let transport = match self.tls_transport.as_mut() {
            Some(t) => t,
            None => return Ok(false),
        };
        let stream = match self.stream.as_mut() {
            Some(s) => s,
            None => return Ok(false),
        };

        // Send any pending TLS data
        while transport.has_pending_send() {
            let data = transport.pending_send();
            match stream.write(data) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
                Ok(n) => transport.advance_send(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        // Check if handshake is complete
        if transport.state() == TransportState::Ready {
            return Ok(true);
        }
        if transport.state() == TransportState::Error {
            self.state = ConnectionState::Disconnected;
            self.error = Some("TLS handshake failed".to_string());
            return Err(io::Error::other("TLS handshake failed"));
        }

        // Read data from socket
        match stream.read(&mut self.recv_buf) {
            Ok(0) => {
                self.state = ConnectionState::Disconnected;
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            Ok(n) => transport.on_recv(&self.recv_buf[..n])?,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(e),
        }

        Ok(false)
    }

    /// Drive client I/O (HTTP/2+gRPC or protosocket).
    fn drive_client_io(&mut self) -> io::Result<()> {
        let client = match self.client.as_mut() {
            Some(c) => c,
            None => return Ok(()),
        };
        let stream = match self.stream.as_mut() {
            Some(s) => s,
            None => return Ok(()),
        };

        // Send any pending data
        while client.has_pending_send() {
            let data = client.pending_send();
            match stream.write(data) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
                Ok(n) => client.advance_send(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    self.state = ConnectionState::Disconnected;
                    return Err(e);
                }
            }
        }

        // Read data from socket
        match stream.read(&mut self.recv_buf) {
            Ok(0) => {
                self.state = ConnectionState::Disconnected;
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            Ok(n) => client.on_recv(&self.recv_buf[..n])?,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => {
                self.state = ConnectionState::Disconnected;
                return Err(e);
            }
        }

        Ok(())
    }

    /// Check if the session is connected and ready.
    pub fn is_connected(&self) -> bool {
        self.state == ConnectionState::Connected && self.client.is_some()
    }

    /// Check if pipeline has room for more requests.
    #[inline]
    pub fn can_send(&self) -> bool {
        self.is_connected() && self.in_flight.len() < self.max_pipeline_depth
    }

    /// Get the number of in-flight requests.
    #[inline]
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    /// Queue a GET request.
    #[inline]
    pub fn get(&mut self, key: &[u8], now: Instant) -> Option<u64> {
        if !self.can_send() {
            return None;
        }

        let client = self.client.as_mut()?;

        match client.get(&self.cache_name, key) {
            Ok(_pending) => {
                let id = self.next_id;
                self.next_id += 1;

                self.in_flight.push_back(InFlightRequest {
                    id,
                    request_type: RequestType::Get,
                    queued_at: now,
                });

                Some(id)
            }
            Err(_) => None,
        }
    }

    /// Queue a SET request.
    #[inline]
    pub fn set(&mut self, key: &[u8], value: &[u8], now: Instant) -> Option<u64> {
        if !self.can_send() {
            return None;
        }

        let client = self.client.as_mut()?;

        match client.set_with_ttl(&self.cache_name, key, value, self.ttl) {
            Ok(_pending) => {
                let id = self.next_id;
                self.next_id += 1;

                self.in_flight.push_back(InFlightRequest {
                    id,
                    request_type: RequestType::Set,
                    queued_at: now,
                });

                Some(id)
            }
            Err(_) => None,
        }
    }

    /// Queue a DELETE request.
    #[inline]
    pub fn delete(&mut self, key: &[u8], now: Instant) -> Option<u64> {
        if !self.can_send() {
            return None;
        }

        let client = self.client.as_mut()?;

        match client.delete(&self.cache_name, key) {
            Ok(_pending) => {
                let id = self.next_id;
                self.next_id += 1;

                self.in_flight.push_back(InFlightRequest {
                    id,
                    request_type: RequestType::Delete,
                    queued_at: now,
                });

                Some(id)
            }
            Err(_) => None,
        }
    }

    /// Process completed operations and extract results.
    #[inline]
    pub fn poll_responses(
        &mut self,
        results: &mut Vec<RequestResult>,
        now: Instant,
    ) -> io::Result<usize> {
        // First drive I/O
        self.drive_client_io()?;

        let client = match self.client.as_mut() {
            Some(c) => c,
            None => return Ok(0),
        };

        let completed = client.poll();
        let count = completed.len();

        for op in completed {
            // Pop the oldest in-flight request
            if let Some(req) = self.in_flight.pop_front() {
                let latency_ns = now.duration_since(req.queued_at).as_nanos() as u64;

                let (success, is_error, hit) = match op {
                    CompletedOp::Get { result, .. } => match result {
                        Ok(CacheValue::Hit(_)) => (true, false, Some(true)),
                        Ok(CacheValue::Miss) => (true, false, Some(false)),
                        Err(_) => (false, true, None),
                    },
                    CompletedOp::Set { result, .. } => match result {
                        Ok(()) => (true, false, None),
                        Err(_) => (false, true, None),
                    },
                    CompletedOp::Delete { result, .. } => match result {
                        Ok(()) => (true, false, None),
                        Err(_) => (false, true, None),
                    },
                };

                results.push(RequestResult {
                    id: req.id,
                    success,
                    is_error_response: is_error,
                    latency_ns,
                    ttfb_ns: None,
                    request_type: req.request_type,
                    hit,
                });
            }
        }

        Ok(count)
    }

    /// Mark the session as disconnected.
    pub fn disconnect(&mut self) {
        self.state = ConnectionState::Disconnected;
        self.client = None;
        self.tls_transport = None;
        self.stream = None;
    }

    /// Clear all state for reconnection.
    pub fn reset(&mut self) {
        self.disconnect();
        self.in_flight.clear();
        self.error = None;
    }
}
