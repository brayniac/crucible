//! grpc - gRPC implementation for io-driver-based applications.
//!
//! This crate provides gRPC framing on top of HTTP/2, designed to work
//! with the io-driver I/O driver. It does not use async/await or tokio.
//!
//! # Features
//!
//! - gRPC message framing (length-prefixed)
//! - Unary RPC support (client and server)
//! - Streaming RPC support
//! - gRPC status codes and metadata
//! - Timeout support
//!
//! # Architecture
//!
//! gRPC messages are framed as:
//! - 1 byte: compressed flag (0 = uncompressed)
//! - 4 bytes: message length (big-endian u32)
//! - N bytes: message payload (typically protobuf)
//!
//! This crate handles the framing layer. Protobuf encoding/decoding
//! is left to the application or higher-level crates.

mod client;
mod frame;
mod metadata;
mod server;
mod status;

pub use client::{Call, CallBuilder, CallEvent, Channel};
pub use frame::{MessageDecoder, decode_message, encode_message};
pub use metadata::Metadata;
pub use server::{GrpcServerEvent, Request, Server};
pub use status::{Code, Status};

// Re-export useful types from dependencies
pub use http2::{Connection, StreamId};
pub use io_driver::{TlsConfig, TlsTransport, Transport};
