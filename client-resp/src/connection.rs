use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::BytesMut;
use protocol_resp::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use crate::error::ClientError;

/// A command in flight: pre-encoded RESP bytes + oneshot for the response.
pub(crate) struct InFlightCommand {
    pub encoded: Vec<u8>,
    pub tx: oneshot::Sender<Result<Value, ClientError>>,
}

/// Spawn a connection manager task for one connection slot.
///
/// The task connects with exponential backoff, then runs a multiplexed
/// select! loop: reading responses and writing batched commands. On
/// disconnect it fails all pending requests and reconnects.
pub(crate) fn spawn_connection(
    addr: SocketAddr,
    connect_timeout: Duration,
    tcp_nodelay: bool,
    cmd_rx: mpsc::Receiver<InFlightCommand>,
) {
    tokio::spawn(connection_task(addr, connect_timeout, tcp_nodelay, cmd_rx));
}

async fn connection_task(
    addr: SocketAddr,
    connect_timeout: Duration,
    tcp_nodelay: bool,
    mut cmd_rx: mpsc::Receiver<InFlightCommand>,
) {
    let mut backoff_ms: u64 = 100;
    const MAX_BACKOFF_MS: u64 = 32_000;

    'outer: loop {
        // Connect with backoff
        let stream = loop {
            match tokio::time::timeout(connect_timeout, TcpStream::connect(addr)).await {
                Ok(Ok(s)) => {
                    let _ = s.set_nodelay(tcp_nodelay);
                    backoff_ms = 100; // reset on success
                    break s;
                }
                _ => {
                    // Wait before retrying, but also drain any commands that arrive
                    // so senders don't block forever during reconnect.
                    let sleep = tokio::time::sleep(Duration::from_millis(backoff_ms));
                    tokio::pin!(sleep);
                    loop {
                        tokio::select! {
                            _ = &mut sleep => break,
                            cmd = cmd_rx.recv() => {
                                match cmd {
                                    Some(c) => {
                                        let _ = c.tx.send(Err(ClientError::ConnectionClosed));
                                    }
                                    None => break 'outer, // channel closed, shut down
                                }
                            }
                        }
                    }
                    backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                }
            }
        };

        let (reader, writer) = stream.into_split();
        let mut reader = reader;
        let mut writer = BufWriter::new(writer);
        let mut pending: VecDeque<oneshot::Sender<Result<Value, ClientError>>> = VecDeque::new();
        let mut read_buf = BytesMut::with_capacity(8192);

        // Multiplexed select! loop
        loop {
            tokio::select! {
                // Write branch: receive command, drain all available, flush once
                cmd = cmd_rx.recv() => {
                    match cmd {
                        None => break 'outer, // channel closed
                        Some(first) => {
                            pending.push_back(first.tx);
                            if writer.write_all(&first.encoded).await.is_err() {
                                break; // reconnect
                            }

                            // Drain all available commands (batching)
                            while let Ok(next) = cmd_rx.try_recv() {
                                pending.push_back(next.tx);
                                if writer.write_all(&next.encoded).await.is_err() {
                                    break;
                                }
                            }

                            if writer.flush().await.is_err() {
                                break; // reconnect
                            }
                        }
                    }
                }

                // Read branch: read data, parse all complete responses
                result = reader.read_buf(&mut read_buf) => {
                    match result {
                        Ok(0) | Err(_) => break, // EOF or error → reconnect
                        Ok(_) => {
                            // Parse all complete values
                            loop {
                                match Value::parse(&read_buf) {
                                    Ok((value, consumed)) => {
                                        let _ = read_buf.split_to(consumed);
                                        if let Some(tx) = pending.pop_front() {
                                            let _ = tx.send(Ok(value));
                                        }
                                    }
                                    Err(protocol_resp::ParseError::Incomplete) => break,
                                    Err(e) => {
                                        // Protocol error — fail all pending
                                        for tx in pending.drain(..) {
                                            let _ = tx.send(Err(ClientError::Protocol(e.clone())));
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Fail all pending requests on disconnect
        for tx in pending.drain(..) {
            let _ = tx.send(Err(ClientError::ConnectionClosed));
        }

        // Loop back to reconnect
    }
}
