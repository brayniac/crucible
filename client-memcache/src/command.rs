use std::io::Write;
use std::time::Instant;

use bytes::Bytes;
use tokio::sync::oneshot;

use krio::{GuardBox, RegionId, SendGuard};

use crate::error::ClientError;

/// Minimum value size to use zero-copy SendMsgZc path.
/// Below this, the overhead of slab allocation + NOTIF CQE exceeds the copy cost.
pub(crate) const ZC_THRESHOLD: usize = 512;

/// Command sent from the async caller to a krio worker.
pub(crate) enum Command {
    Get {
        key: Bytes,
        tx: oneshot::Sender<Result<Option<(Bytes, u32)>, ClientError>>,
    },
    Set {
        key: Bytes,
        value: Bytes,
        flags: u32,
        exptime: u32,
        tx: oneshot::Sender<Result<(), ClientError>>,
    },
    Del {
        key: Bytes,
        tx: oneshot::Sender<Result<bool, ClientError>>,
    },
    Version {
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
            Command::Version { .. } => &[],
        }
    }

    /// Convert this command into a PendingResponse, consuming the oneshot sender.
    /// Records the current instant as the send timestamp.
    pub fn into_pending(self) -> PendingResponse {
        let kind = match self {
            Command::Get { tx, .. } => ResponseKind::Get(tx),
            Command::Set { tx, .. } => ResponseKind::Set(tx),
            Command::Del { tx, .. } => ResponseKind::Del(tx),
            Command::Version { tx, .. } => ResponseKind::Version(tx),
        };
        PendingResponse {
            kind,
            sent_at: Instant::now(),
        }
    }

    /// Encode this command as Memcache ASCII into `buf`. Used for small commands (copy path).
    /// Returns the number of bytes written.
    pub fn encode(&self, buf: &mut [u8]) -> usize {
        match self {
            Command::Get { key, .. } => protocol_memcache::Request::get(key).encode(buf),
            Command::Set {
                key,
                value,
                flags,
                exptime,
                ..
            } => protocol_memcache::Request::set(key, value)
                .flags(*flags)
                .exptime(*exptime)
                .encode(buf),
            Command::Del { key, .. } => protocol_memcache::Request::delete(key).encode(buf),
            Command::Version { .. } => protocol_memcache::Request::version().encode(buf),
        }
    }
}

// -- Zero-copy SET encoding ------------------------------------------------

/// Encode the Memcache SET header (everything before the value body).
///
/// Writes: `set <key> <flags> <exptime> <bytes>\r\n`
pub(crate) fn encode_set_header(
    buf: &mut Vec<u8>,
    key: &[u8],
    value_len: usize,
    flags: u32,
    exptime: u32,
) {
    buf.extend_from_slice(b"set ");
    buf.extend_from_slice(key);
    buf.push(b' ');
    // Use write! for the numeric fields
    write!(buf, "{} {} {}\r\n", flags, exptime, value_len).unwrap();
    // Value body follows as zero-copy guard
}

/// Encode the Memcache SET suffix (after the value body).
///
/// Always: `\r\n`
pub(crate) fn encode_set_suffix(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"\r\n");
}

// -- BytesGuard: zero-copy SendGuard backed by Bytes ----------------------

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

// -- PendingResponse ------------------------------------------------------

/// Tracks a pending response on a connection's in-flight queue.
pub(crate) struct PendingResponse {
    pub kind: ResponseKind,
    pub sent_at: Instant,
}

/// The operation type and its oneshot sender.
pub(crate) enum ResponseKind {
    Get(oneshot::Sender<Result<Option<(Bytes, u32)>, ClientError>>),
    Set(oneshot::Sender<Result<(), ClientError>>),
    Del(oneshot::Sender<Result<bool, ClientError>>),
    Version(oneshot::Sender<Result<(), ClientError>>),
}

impl PendingResponse {
    /// Complete this pending response with the parsed Memcache response.
    pub fn complete(self, response: protocol_memcache::Response) {
        match self.kind {
            ResponseKind::Get(tx) => {
                let result = match response {
                    protocol_memcache::Response::Values(values) => {
                        if values.is_empty() {
                            Ok(None)
                        } else {
                            let v = &values[0];
                            Ok(Some((Bytes::from(v.data.clone()), v.flags)))
                        }
                    }
                    protocol_memcache::Response::Error => {
                        Err(ClientError::Memcache("ERROR".into()))
                    }
                    protocol_memcache::Response::ClientError(msg) => Err(ClientError::Memcache(
                        String::from_utf8_lossy(&msg).into_owned(),
                    )),
                    protocol_memcache::Response::ServerError(msg) => Err(ClientError::Memcache(
                        String::from_utf8_lossy(&msg).into_owned(),
                    )),
                    other => Err(ClientError::Protocol(format!(
                        "unexpected GET response: {other:?}",
                    ))),
                };
                let _ = tx.send(result);
            }
            ResponseKind::Set(tx) => {
                let result = match response {
                    protocol_memcache::Response::Stored => Ok(()),
                    protocol_memcache::Response::NotStored => {
                        Err(ClientError::Memcache("NOT_STORED".into()))
                    }
                    protocol_memcache::Response::Error => {
                        Err(ClientError::Memcache("ERROR".into()))
                    }
                    protocol_memcache::Response::ClientError(msg) => Err(ClientError::Memcache(
                        String::from_utf8_lossy(&msg).into_owned(),
                    )),
                    protocol_memcache::Response::ServerError(msg) => Err(ClientError::Memcache(
                        String::from_utf8_lossy(&msg).into_owned(),
                    )),
                    other => Err(ClientError::Protocol(format!(
                        "unexpected SET response: {other:?}",
                    ))),
                };
                let _ = tx.send(result);
            }
            ResponseKind::Del(tx) => {
                let result = match response {
                    protocol_memcache::Response::Deleted => Ok(true),
                    protocol_memcache::Response::NotFound => Ok(false),
                    protocol_memcache::Response::Error => {
                        Err(ClientError::Memcache("ERROR".into()))
                    }
                    protocol_memcache::Response::ClientError(msg) => Err(ClientError::Memcache(
                        String::from_utf8_lossy(&msg).into_owned(),
                    )),
                    protocol_memcache::Response::ServerError(msg) => Err(ClientError::Memcache(
                        String::from_utf8_lossy(&msg).into_owned(),
                    )),
                    other => Err(ClientError::Protocol(format!(
                        "unexpected DELETE response: {other:?}",
                    ))),
                };
                let _ = tx.send(result);
            }
            ResponseKind::Version(tx) => {
                let result = match response {
                    protocol_memcache::Response::Version(_) => Ok(()),
                    protocol_memcache::Response::Error => {
                        Err(ClientError::Memcache("ERROR".into()))
                    }
                    protocol_memcache::Response::ClientError(msg) => Err(ClientError::Memcache(
                        String::from_utf8_lossy(&msg).into_owned(),
                    )),
                    protocol_memcache::Response::ServerError(msg) => Err(ClientError::Memcache(
                        String::from_utf8_lossy(&msg).into_owned(),
                    )),
                    other => Err(ClientError::Protocol(format!(
                        "unexpected VERSION response: {other:?}",
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
            ResponseKind::Version(tx) => {
                let _ = tx.send(Err(err));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_set_header_basic() {
        let mut buf = Vec::new();
        encode_set_header(&mut buf, b"mykey", 100, 0, 0);
        assert_eq!(&buf, b"set mykey 0 0 100\r\n");
    }

    #[test]
    fn encode_set_header_with_flags_exptime() {
        let mut buf = Vec::new();
        encode_set_header(&mut buf, b"k", 5, 42, 3600);
        assert_eq!(&buf, b"set k 42 3600 5\r\n");
    }

    #[test]
    fn encode_set_suffix_crlf() {
        let mut buf = Vec::new();
        encode_set_suffix(&mut buf);
        assert_eq!(&buf, b"\r\n");
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
