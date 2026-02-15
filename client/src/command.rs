use std::time::Instant;

use bytes::Bytes;
use tokio::sync::oneshot;

use kompio::{GuardBox, RegionId, SendGuard};

use crate::error::ClientError;

/// Minimum value size to use zero-copy SendMsgZc path.
/// Below this, the overhead of slab allocation + NOTIF CQE exceeds the copy cost.
pub(crate) const ZC_THRESHOLD: usize = 512;

/// Command sent from the async caller to a kompio worker.
pub(crate) enum Command {
    Get {
        key: Bytes,
        tx: oneshot::Sender<Result<Option<Bytes>, ClientError>>,
    },
    Set {
        key: Bytes,
        value: Bytes,
        ttl_secs: Option<u64>,
        tx: oneshot::Sender<Result<(), ClientError>>,
    },
    Del {
        key: Bytes,
        tx: oneshot::Sender<Result<u64, ClientError>>,
    },
    Ping {
        tx: oneshot::Sender<Result<(), ClientError>>,
    },
}

impl Command {
    /// Extract the key for routing. Returns empty slice for keyless commands.
    pub fn key(&self) -> &[u8] {
        match self {
            Command::Get { key, .. } => key,
            Command::Set { key, .. } => key,
            Command::Del { key, .. } => key,
            Command::Ping { .. } => &[],
        }
    }

    /// Convert this command into a PendingResponse, consuming the oneshot sender.
    /// Records the current instant as the send timestamp.
    pub fn into_pending(self) -> PendingResponse {
        let kind = match self {
            Command::Get { tx, .. } => ResponseKind::Get(tx),
            Command::Set { tx, .. } => ResponseKind::Set(tx),
            Command::Del { tx, .. } => ResponseKind::Del(tx),
            Command::Ping { tx, .. } => ResponseKind::Ping(tx),
        };
        PendingResponse {
            kind,
            sent_at: Instant::now(),
        }
    }

    /// Encode this command as RESP into `buf`. Used for small commands (copy path).
    /// Returns the number of bytes written.
    pub fn encode(&self, buf: &mut [u8]) -> usize {
        match self {
            Command::Get { key, .. } => protocol_resp::Request::get(key).encode(buf),
            Command::Set {
                key,
                value,
                ttl_secs,
                ..
            } => {
                let req = protocol_resp::Request::set(key, value);
                match ttl_secs {
                    Some(ttl) => req.ex(*ttl).encode(buf),
                    None => req.encode(buf),
                }
            }
            Command::Del { key, .. } => protocol_resp::Request::del(key).encode(buf),
            Command::Ping { .. } => protocol_resp::Request::ping().encode(buf),
        }
    }
}

// ── Zero-copy SET encoding ───────────────────────────────────────────────

/// Encode the RESP header for a SET command (everything before the value body).
///
/// Writes: `*<argc>\r\n$3\r\nSET\r\n$<keylen>\r\n<key>\r\n$<vallen>\r\n`
pub(crate) fn encode_set_header(
    buf: &mut Vec<u8>,
    key: &[u8],
    value_len: usize,
    has_ttl: bool,
) {
    let argc: u8 = if has_ttl { 5 } else { 3 };
    buf.push(b'*');
    push_decimal(buf, argc as u64);
    buf.extend_from_slice(b"\r\n$3\r\nSET\r\n$");
    push_decimal(buf, key.len() as u64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(key);
    buf.extend_from_slice(b"\r\n$");
    push_decimal(buf, value_len as u64);
    buf.extend_from_slice(b"\r\n");
    // Value body follows as zero-copy guard
}

/// Encode the RESP suffix for a SET command (after the value body).
///
/// Without TTL: `\r\n`
/// With TTL:    `\r\n$2\r\nEX\r\n$<digits>\r\n<ttl>\r\n`
pub(crate) fn encode_set_suffix(buf: &mut Vec<u8>, ttl_secs: Option<u64>) {
    buf.extend_from_slice(b"\r\n");
    if let Some(ttl) = ttl_secs {
        buf.extend_from_slice(b"$2\r\nEX\r\n$");
        // Format TTL digits, then write length prefix + digits
        let mut ttl_buf = Vec::with_capacity(20);
        push_decimal(&mut ttl_buf, ttl);
        push_decimal(buf, ttl_buf.len() as u64);
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(&ttl_buf);
        buf.extend_from_slice(b"\r\n");
    }
}

/// Write a u64 as decimal digits into a Vec.
fn push_decimal(buf: &mut Vec<u8>, n: u64) {
    if n == 0 {
        buf.push(b'0');
        return;
    }
    let start = buf.len();
    let mut val = n;
    while val > 0 {
        buf.push(b'0' + (val % 10) as u8);
        val /= 10;
    }
    buf[start..].reverse();
}

// ── BytesGuard: zero-copy SendGuard backed by Bytes ──────────────────────

/// SendGuard that pins a `Bytes` reference until the kernel completes the
/// zero-copy send. The Arc inside Bytes keeps the backing memory alive.
///
/// Size: 32 bytes (Bytes is 4 words on 64-bit), well within GuardBox's 64-byte limit.
pub(crate) struct BytesGuard(pub Bytes);

impl SendGuard for BytesGuard {
    fn as_ptr_len(&self) -> (*const u8, u32) {
        (self.0.as_ptr(), self.0.len() as u32)
    }

    fn region(&self) -> RegionId {
        RegionId::UNREGISTERED
    }
}

/// Create a GuardBox wrapping a Bytes value for zero-copy sends.
pub(crate) fn bytes_guard(value: Bytes) -> GuardBox {
    GuardBox::new(BytesGuard(value))
}

// ── PendingResponse ──────────────────────────────────────────────────────

/// Tracks a pending response on a connection's in-flight queue.
pub(crate) struct PendingResponse {
    pub kind: ResponseKind,
    pub sent_at: Instant,
}

/// The operation type and its oneshot sender.
pub(crate) enum ResponseKind {
    Get(oneshot::Sender<Result<Option<Bytes>, ClientError>>),
    Set(oneshot::Sender<Result<(), ClientError>>),
    Del(oneshot::Sender<Result<u64, ClientError>>),
    Ping(oneshot::Sender<Result<(), ClientError>>),
}

impl PendingResponse {
    /// Complete this pending response with the parsed RESP value.
    pub fn complete(self, value: protocol_resp::Value) {
        match self.kind {
            ResponseKind::Get(tx) => {
                let result = match value {
                    protocol_resp::Value::BulkString(data) => {
                        // Vec<u8> → Bytes: takes ownership, no copy.
                        Ok(Some(Bytes::from(data)))
                    }
                    protocol_resp::Value::Null => Ok(None),
                    protocol_resp::Value::Error(msg) => {
                        Err(ClientError::Redis(String::from_utf8_lossy(&msg).into_owned()))
                    }
                    other => Err(ClientError::Protocol(format!(
                        "unexpected GET response: {other:?}",
                    ))),
                };
                let _ = tx.send(result);
            }
            ResponseKind::Set(tx) => {
                let result = match value {
                    protocol_resp::Value::SimpleString(ref s) if s == b"OK" => Ok(()),
                    protocol_resp::Value::Error(msg) => {
                        Err(ClientError::Redis(String::from_utf8_lossy(&msg).into_owned()))
                    }
                    other => Err(ClientError::Protocol(format!(
                        "unexpected SET response: {other:?}",
                    ))),
                };
                let _ = tx.send(result);
            }
            ResponseKind::Del(tx) => {
                let result = match value {
                    protocol_resp::Value::Integer(n) => Ok(n as u64),
                    protocol_resp::Value::Error(msg) => {
                        Err(ClientError::Redis(String::from_utf8_lossy(&msg).into_owned()))
                    }
                    other => Err(ClientError::Protocol(format!(
                        "unexpected DEL response: {other:?}",
                    ))),
                };
                let _ = tx.send(result);
            }
            ResponseKind::Ping(tx) => {
                let result = match value {
                    protocol_resp::Value::SimpleString(ref s) if s == b"PONG" => Ok(()),
                    protocol_resp::Value::Error(msg) => {
                        Err(ClientError::Redis(String::from_utf8_lossy(&msg).into_owned()))
                    }
                    other => Err(ClientError::Protocol(format!(
                        "unexpected PING response: {other:?}",
                    ))),
                };
                let _ = tx.send(result);
            }
        }
    }

    /// Fail this pending response with the given error.
    pub fn fail(self, err: ClientError) {
        match self.kind {
            ResponseKind::Get(tx) => {
                let _ = tx.send(Err(err));
            }
            ResponseKind::Set(tx) => {
                let _ = tx.send(Err(err));
            }
            ResponseKind::Del(tx) => {
                let _ = tx.send(Err(err));
            }
            ResponseKind::Ping(tx) => {
                let _ = tx.send(Err(err));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_decimal_zero() {
        let mut buf = Vec::new();
        push_decimal(&mut buf, 0);
        assert_eq!(&buf, b"0");
    }

    #[test]
    fn push_decimal_large() {
        let mut buf = Vec::new();
        push_decimal(&mut buf, 123456);
        assert_eq!(&buf, b"123456");
    }

    #[test]
    fn encode_set_header_no_ttl() {
        let mut buf = Vec::new();
        encode_set_header(&mut buf, b"mykey", 100, false);
        assert_eq!(&buf, b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$100\r\n");
    }

    #[test]
    fn encode_set_header_with_ttl() {
        let mut buf = Vec::new();
        encode_set_header(&mut buf, b"k", 5, true);
        assert_eq!(&buf, b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\n");
    }

    #[test]
    fn encode_set_suffix_no_ttl() {
        let mut buf = Vec::new();
        encode_set_suffix(&mut buf, None);
        assert_eq!(&buf, b"\r\n");
    }

    #[test]
    fn encode_set_suffix_with_ttl() {
        let mut buf = Vec::new();
        encode_set_suffix(&mut buf, Some(3600));
        assert_eq!(&buf, b"\r\n$2\r\nEX\r\n$4\r\n3600\r\n");
    }

    #[test]
    fn bytes_guard_size() {
        assert!(
            std::mem::size_of::<BytesGuard>() <= 64,
            "BytesGuard is {} bytes, must fit in GuardBox (64 bytes)",
            std::mem::size_of::<BytesGuard>()
        );
    }

    #[test]
    fn bytes_guard_ptr_len() {
        let data = Bytes::from_static(b"hello world");
        let guard = BytesGuard(data.clone());
        let (ptr, len) = guard.as_ptr_len();
        assert_eq!(ptr, data.as_ptr());
        assert_eq!(len, 11);
    }
}
