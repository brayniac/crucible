//! Simple CLI tool to test Momento cache operations.
//!
//! Usage:
//! ```text
//!   momento-cli get <cache> <key>
//!   momento-cli set <cache> <key> <value> [--ttl <seconds>]
//!   momento-cli delete <cache> <key>
//! ```
//!
//! Environment:
//! - MOMENTO_API_KEY: Momento API key (required)
//! - MOMENTO_ENDPOINT: Explicit endpoint (e.g., cache.us-west-2.momentohq.com:9004)
//! - MOMENTO_WIRE_FORMAT: "grpc" or "protosocket"

use std::env;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use io_driver::{TlsConfig, TlsTransport, Transport, TransportState};
use protocol_momento::{CacheClient, CacheValue, CompletedOp, Credential, WireFormat};

fn main() {
    if let Err(e) = run() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 4 {
        print_usage();
        return Err("insufficient arguments".into());
    }

    let command = &args[1];
    let cache_name = &args[2];
    let key = &args[3];

    // Parse command
    let operation = match command.as_str() {
        "get" => Operation::Get,
        "set" => {
            if args.len() < 5 {
                return Err("set requires a value".into());
            }
            let value = args[4].clone();
            let ttl = parse_ttl(&args)?;
            Operation::Set { value, ttl }
        }
        "delete" => Operation::Delete,
        _ => {
            print_usage();
            return Err(format!("unknown command: {}", command).into());
        }
    };

    // Get credential from environment
    let credential = Credential::from_env()?;

    // Connect and execute
    execute_operation(credential, cache_name, key, operation)
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  momento-cli get <cache> <key>");
    eprintln!("  momento-cli set <cache> <key> <value> [--ttl <seconds>]");
    eprintln!("  momento-cli delete <cache> <key>");
    eprintln!();
    eprintln!("Environment:");
    eprintln!("  MOMENTO_API_KEY     - Momento API key (required)");
    eprintln!("  MOMENTO_REGION      - Region for cache endpoint (e.g., us-west-2)");
    eprintln!(
        "  MOMENTO_ENDPOINT    - Explicit endpoint (e.g., cache.us-west-2.momentohq.com:9004)"
    );
    eprintln!("  MOMENTO_WIRE_FORMAT - Wire format: \"grpc\" (default) or \"protosocket\"");
}

fn parse_ttl(args: &[String]) -> Result<Duration, Box<dyn std::error::Error>> {
    for i in 0..args.len() - 1 {
        if args[i] == "--ttl" {
            let secs: u64 = args[i + 1].parse()?;
            return Ok(Duration::from_secs(secs));
        }
    }
    Ok(Duration::from_secs(3600)) // Default 1 hour
}

enum Operation {
    Get,
    Set { value: String, ttl: Duration },
    Delete,
}

fn execute_operation(
    credential: Credential,
    cache_name: &str,
    key: &str,
    operation: Operation,
) -> Result<(), Box<dyn std::error::Error>> {
    // Resolve endpoint
    let host = credential.host();
    let port = credential.port();
    let wire_format = credential.wire_format();
    let addr = format!("{}:{}", host, port);

    // Connect TCP
    println!("Connecting to {}...", addr);
    let mut stream = TcpStream::connect(&addr)?;
    stream.set_nonblocking(true)?;

    // Create TLS transport based on wire format
    let tls_config = match wire_format {
        WireFormat::Grpc => TlsConfig::http2()?,
        WireFormat::Protosocket => TlsConfig::new()?,
    };
    let mut transport = TlsTransport::new(&tls_config, host)?;

    // Drive TLS handshake
    println!("TLS handshake...");
    drive_handshake(&mut transport, &mut stream)?;

    // Verify ALPN for gRPC
    match wire_format {
        WireFormat::Grpc => {
            if let Some(alpn) = transport.alpn_protocol() {
                if alpn != b"h2" {
                    return Err(format!("unexpected ALPN: {:?}", alpn).into());
                }
                println!("ALPN: h2");
            }
        }
        WireFormat::Protosocket => {
            println!("Wire format: protosocket");
        }
    }

    // Create cache client based on wire format
    let mut client = match wire_format {
        WireFormat::Grpc => CacheClient::with_transport(transport, credential),
        WireFormat::Protosocket => CacheClient::with_protosocket_transport(transport, credential),
    };
    client.on_transport_ready()?;

    // Drive connection setup
    match wire_format {
        WireFormat::Grpc => println!("HTTP/2 connection setup..."),
        WireFormat::Protosocket => println!("Protosocket authentication..."),
    }
    drive_connection_setup(&mut client, &mut stream)?;

    // Execute operation
    match operation {
        Operation::Get => {
            println!("GET {}/{}", cache_name, key);
            client.get(cache_name, key.as_bytes())?;
        }
        Operation::Set { ref value, ttl } => {
            println!("SET {}/{} = {:?} (ttl: {:?})", cache_name, key, value, ttl);
            client.set_with_ttl(cache_name, key.as_bytes(), value.as_bytes(), ttl)?;
        }
        Operation::Delete => {
            println!("DELETE {}/{}", cache_name, key);
            client.delete(cache_name, key.as_bytes())?;
        }
    }

    // Drive until we get a response
    println!("Waiting for response...");
    let result = drive_until_complete(&mut client, &mut stream)?;

    // Print result
    match result {
        CompletedOp::Get { result, .. } => match result {
            Ok(CacheValue::Hit(value)) => {
                println!("HIT: {}", String::from_utf8_lossy(&value));
            }
            Ok(CacheValue::Miss) => {
                println!("MISS");
            }
            Err(e) => {
                println!("ERROR: {}", e);
            }
        },
        CompletedOp::Set { result, .. } => match result {
            Ok(()) => println!("OK"),
            Err(e) => println!("ERROR: {}", e),
        },
        CompletedOp::Delete { result, .. } => match result {
            Ok(()) => println!("OK"),
            Err(e) => println!("ERROR: {}", e),
        },
    }

    Ok(())
}

/// Drive TLS handshake to completion.
fn drive_handshake(transport: &mut TlsTransport, stream: &mut TcpStream) -> io::Result<()> {
    let mut buf = [0u8; 16384];

    loop {
        // Send any pending data
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
            return Ok(());
        }
        if transport.state() == TransportState::Error {
            return Err(io::Error::other("TLS handshake failed"));
        }

        // Read data from socket
        match stream.read(&mut buf) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(n) => transport.on_recv(&buf[..n])?,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Wait a bit for socket to be ready
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Drive connection setup (HTTP/2 for gRPC, auth for protosocket).
fn drive_connection_setup<T: Transport>(
    client: &mut CacheClient<T>,
    stream: &mut TcpStream,
) -> io::Result<()> {
    let mut buf = [0u8; 16384];
    let debug = std::env::var("DEBUG").is_ok();

    loop {
        // Send any pending data
        while client.has_pending_send() {
            let data = client.pending_send();
            if debug {
                eprintln!(
                    "SEND {} bytes: {:02x?}",
                    data.len(),
                    &data[..data.len().min(64)]
                );
            }
            match stream.write(data) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
                Ok(n) => client.advance_send(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        // Check if connection is ready
        if client.is_ready() {
            return Ok(());
        }

        // Read data from socket
        match stream.read(&mut buf) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(n) => {
                if debug {
                    eprintln!("RECV {} bytes: {:02x?}", n, &buf[..n.min(64)]);
                }
                client.on_recv(&buf[..n])?;
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Drive until we get a completed operation.
fn drive_until_complete<T: Transport>(
    client: &mut CacheClient<T>,
    stream: &mut TcpStream,
) -> io::Result<CompletedOp> {
    let mut buf = [0u8; 16384];

    loop {
        // Send any pending data
        while client.has_pending_send() {
            let data = client.pending_send();
            match stream.write(data) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
                Ok(n) => client.advance_send(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        // Check for completed operations
        let completed = client.poll();
        if !completed.is_empty() {
            return Ok(completed.into_iter().next().unwrap());
        }

        // Read data from socket
        match stream.read(&mut buf) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(n) => {
                client.on_recv(&buf[..n])?;
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => return Err(e),
        }
    }
}
