//! Momento connection for benchmarking Momento cache via kompio.
//!
//! Uses kompio's native TLS (`connect_tls`) so that Momento connections
//! benefit from io_uring (multishot recv, SendMsgZc, CPU pinning). The
//! `CacheClient<PlainTransport>` handles HTTP/2 framing and gRPC while
//! kompio handles TCP + TLS transparently.

use super::{RequestResult, RequestType};
use crate::config::{Config, MomentoWireFormat};

use http2::PlainTransport;
use protocol_momento::{
    CacheClient, CacheValue, CompletedOp, Credential, EndpointsFetcher, WireFormat,
};

use std::collections::VecDeque;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::{Duration, Instant};

/// A request in the pipeline waiting for a response.
#[derive(Debug)]
struct InFlightRequest {
    id: u64,
    request_type: RequestType,
    queued_at: Instant,
}

/// Kompio-integrated Momento connection.
///
/// Unlike the old `MomentoSession` which managed its own TcpStream and TLS,
/// this struct only tracks the protocol-level state. TCP + TLS is handled
/// by kompio (`connect_tls` / `on_data` / `ctx.send`).
pub struct MomentoConn {
    /// The Momento cache client using PlainTransport (kompio handles TLS).
    client: CacheClient<PlainTransport>,
    /// Cache name for operations.
    cache_name: String,
    /// TTL for SET operations.
    ttl: Duration,
    /// In-flight request tracking for latency measurement.
    in_flight: VecDeque<InFlightRequest>,
    /// Next request ID.
    next_id: u64,
    /// Maximum pipeline depth.
    max_pipeline_depth: usize,
    /// Whether the client has completed the HTTP/2 handshake (or protosocket auth).
    ready: bool,
}

impl MomentoConn {
    /// Create a new Momento connection with PlainTransport.
    ///
    /// Call `on_transport_ready()` after kompio's TLS handshake completes
    /// (`on_connect` callback), then flush pending bytes.
    pub fn new(credential: &Credential, config: &Config) -> Self {
        let client = match credential.wire_format() {
            WireFormat::Grpc => {
                CacheClient::with_transport(PlainTransport::new(), credential.clone())
            }
            WireFormat::Protosocket => {
                CacheClient::with_protosocket_transport(PlainTransport::new(), credential.clone())
            }
        };

        Self {
            client,
            cache_name: config.momento.cache_name.clone(),
            ttl: Duration::from_secs(config.momento.ttl_seconds),
            in_flight: VecDeque::with_capacity(config.connection.pipeline_depth),
            next_id: 0,
            max_pipeline_depth: config.connection.pipeline_depth,
            ready: false,
        }
    }

    /// Initialize the HTTP/2 connection preface (or protosocket auth).
    ///
    /// Call this from `on_connect` after TLS handshake succeeds. After this,
    /// flush `pending_send()` via `ctx.send()`.
    pub fn on_transport_ready(&mut self) -> io::Result<()> {
        self.client.on_transport_ready()
    }

    /// Feed plaintext data received from kompio's `on_data` callback.
    ///
    /// Uses `feed_data()` to skip the PlainTransport recv buffer entirely.
    pub fn feed_data(&mut self, data: &[u8]) -> io::Result<()> {
        self.client.feed_data(data)
    }

    /// Check if the connection is ready for cache operations.
    pub fn is_ready(&self) -> bool {
        self.ready
    }

    /// Check if pipeline has room for more requests.
    #[inline]
    pub fn can_send(&self) -> bool {
        self.ready && self.in_flight.len() < self.max_pipeline_depth
    }

    /// Get data pending to be sent on the socket.
    pub fn pending_send(&self) -> &[u8] {
        self.client.pending_send()
    }

    /// Mark bytes as sent.
    pub fn advance_send(&mut self, n: usize) {
        self.client.advance_send(n);
    }

    /// Check if there's data to send.
    pub fn has_pending_send(&self) -> bool {
        self.client.has_pending_send()
    }

    /// Queue a GET request.
    #[inline]
    pub fn get(&mut self, key: &[u8], now: Instant) -> Option<u64> {
        if !self.can_send() {
            return None;
        }

        match self.client.get(&self.cache_name, key) {
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

        match self
            .client
            .set_with_ttl(&self.cache_name, key, value, self.ttl)
        {
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

        match self.client.delete(&self.cache_name, key) {
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

    /// Poll for completed operations and extract results.
    ///
    /// Call after `feed_data()` to process responses. Also updates `ready`
    /// state when the HTTP/2 settings exchange completes.
    pub fn poll_responses(&mut self, results: &mut Vec<RequestResult>, now: Instant) -> usize {
        // Check if client became ready
        if !self.ready && self.client.is_ready() {
            self.ready = true;
        }

        let completed = self.client.poll();
        let count = completed.len();

        for op in completed {
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
                    key_id: None,
                    backfill: false,
                });
            }
        }

        count
    }

    /// Get the number of in-flight requests.
    #[inline]
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }
}

/// Shared Momento setup resolved once per worker (credential, addresses, TLS host).
pub struct MomentoSetup {
    /// Resolved credential.
    pub credential: Credential,
    /// Target addresses for connections.
    pub addresses: Vec<SocketAddr>,
    /// TLS SNI hostname.
    pub tls_host: String,
}

impl MomentoSetup {
    /// Resolve Momento configuration from the benchmark config.
    ///
    /// Fetches private endpoints if configured, resolves DNS, and builds
    /// the credential with the correct wire format.
    pub fn from_config(config: &Config) -> io::Result<Self> {
        let wire_format = match config.momento.wire_format {
            MomentoWireFormat::Grpc => WireFormat::Grpc,
            MomentoWireFormat::Protosocket => WireFormat::Protosocket,
        };

        let credential = Credential::from_env()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

        let credential = if let Some(ref endpoint) = config.momento.endpoint {
            Credential::with_endpoint(credential.token().to_string(), endpoint.clone())
        } else {
            credential
        };

        let credential = credential.with_wire_format(wire_format);

        let (addresses, tls_host) = if config.momento.use_private_endpoints {
            let fetcher = EndpointsFetcher::new(&credential, true);
            let endpoint_info = fetcher.fetch().map_err(|e| {
                io::Error::other(format!("failed to fetch private endpoints: {}", e))
            })?;

            let az_filter: Option<String> = config
                .momento
                .availability_zone
                .clone()
                .or_else(|| std::env::var("MOMENTO_AZ").ok());

            let addrs = endpoint_info.for_az(az_filter.as_deref());
            if addrs.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "no private endpoints available",
                ));
            }
            (addrs, credential.tls_host().to_string())
        } else {
            let host = credential.host();
            let port = credential.port();
            let addr_str = format!("{}:{}", host, port);
            let addrs: Vec<SocketAddr> = addr_str.to_socket_addrs()?.collect();
            if addrs.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "could not resolve endpoint",
                ));
            }
            (addrs, host.to_string())
        };

        Ok(Self {
            credential,
            addresses,
            tls_host,
        })
    }
}

/// Build a rustls ClientConfig for Momento TLS connections.
///
/// - gRPC: ALPN set to `["h2"]` for HTTP/2
/// - Protosocket: no ALPN (plain TLS)
pub fn build_momento_tls_config(wire_format: WireFormat) -> io::Result<rustls::ClientConfig> {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    if wire_format == WireFormat::Grpc {
        config.alpn_protocols = vec![b"h2".to_vec()];
    }

    Ok(config)
}
