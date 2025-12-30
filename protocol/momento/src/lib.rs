//! protocol-momento - Lightweight Momento cache implementation for io-driver-based applications.
//!
//! This crate provides a minimal Momento cache client and server that uses the gRPC API
//! without pulling in heavy dependencies like tokio or prost.
//!
//! # Features
//!
//! - Get, Set, Delete cache operations
//! - Client and server implementations
//! - Minimal protobuf encoding (no code generation)
//! - Completion-based I/O via io-driver
//!
//! # Client Example
//!
//! ```ignore
//! use protocol_momento::{CacheClient, Credential};
//! use io_driver::TlsConfig;
//!
//! // Create client
//! let credential = Credential::from_token("your-api-token")?;
//! let mut client = CacheClient::new(credential)?;
//!
//! // Cache operations happen through the event loop
//! // (see full example for event loop integration)
//! ```

mod client;
mod credential;
mod error;
pub mod proto;
mod server;

pub use client::{CacheClient, CacheValue, CompletedOp, PendingDelete, PendingGet, PendingSet};
pub use credential::Credential;
pub use error::{Error, Result};
pub use server::{CacheRequest, CacheResponse, CacheServer, CacheServerEvent};

// Re-export useful types
pub use grpc::{Code, Status};
pub use io_driver::{TlsConfig, TlsTransport};
