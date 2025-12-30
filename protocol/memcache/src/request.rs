//! Client-side request encoding for Memcache ASCII protocol.
//!
//! This module provides encoding of Memcache commands for client applications.

use std::io::Write;

/// A request builder for encoding Memcache commands.
#[derive(Debug, Clone)]
pub enum Request<'a> {
    /// GET command: `get <key>\r\n`
    Get { key: &'a [u8] },
    /// Multi-GET command: `get <key1> <key2> ...\r\n`
    Gets { keys: &'a [&'a [u8]] },
    /// SET command: `set <key> <flags> <exptime> <bytes>\r\n<data>\r\n`
    Set {
        key: &'a [u8],
        value: &'a [u8],
        flags: u32,
        exptime: u32,
    },
    /// DELETE command: `delete <key>\r\n`
    Delete { key: &'a [u8] },
    /// FLUSH_ALL command: `flush_all\r\n`
    FlushAll,
    /// VERSION command: `version\r\n`
    Version,
    /// QUIT command: `quit\r\n`
    Quit,
}

impl<'a> Request<'a> {
    /// Create a GET request.
    #[inline]
    pub fn get(key: &'a [u8]) -> Self {
        Request::Get { key }
    }

    /// Create a multi-GET request.
    #[inline]
    pub fn gets(keys: &'a [&'a [u8]]) -> Self {
        Request::Gets { keys }
    }

    /// Create a SET request.
    #[inline]
    pub fn set(key: &'a [u8], value: &'a [u8]) -> SetRequest<'a> {
        SetRequest {
            key,
            value,
            flags: 0,
            exptime: 0,
        }
    }

    /// Create a DELETE request.
    #[inline]
    pub fn delete(key: &'a [u8]) -> Self {
        Request::Delete { key }
    }

    /// Create a FLUSH_ALL request.
    #[inline]
    pub fn flush_all() -> Self {
        Request::FlushAll
    }

    /// Create a VERSION request.
    #[inline]
    pub fn version() -> Self {
        Request::Version
    }

    /// Create a QUIT request.
    #[inline]
    pub fn quit() -> Self {
        Request::Quit
    }

    /// Encode this request into a buffer.
    ///
    /// Returns the number of bytes written.
    pub fn encode(&self, buf: &mut [u8]) -> usize {
        match self {
            Request::Get { key } => encode_get(buf, key),
            Request::Gets { keys } => encode_gets(buf, keys),
            Request::Set {
                key,
                value,
                flags,
                exptime,
            } => encode_set(buf, key, value, *flags, *exptime),
            Request::Delete { key } => encode_delete(buf, key),
            Request::FlushAll => encode_simple(buf, b"flush_all"),
            Request::Version => encode_simple(buf, b"version"),
            Request::Quit => encode_simple(buf, b"quit"),
        }
    }
}

/// Builder for SET requests with optional flags and exptime.
#[derive(Debug, Clone)]
pub struct SetRequest<'a> {
    key: &'a [u8],
    value: &'a [u8],
    flags: u32,
    exptime: u32,
}

impl<'a> SetRequest<'a> {
    /// Set the flags value.
    #[inline]
    pub fn flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    /// Set the expiration time in seconds.
    #[inline]
    pub fn exptime(mut self, exptime: u32) -> Self {
        self.exptime = exptime;
        self
    }

    /// Build the final request.
    #[inline]
    pub fn build(self) -> Request<'a> {
        Request::Set {
            key: self.key,
            value: self.value,
            flags: self.flags,
            exptime: self.exptime,
        }
    }

    /// Encode this request directly into a buffer.
    ///
    /// Returns the number of bytes written.
    #[inline]
    pub fn encode(&self, buf: &mut [u8]) -> usize {
        encode_set(buf, self.key, self.value, self.flags, self.exptime)
    }
}

/// Encode a GET command: `get <key>\r\n`
fn encode_get(buf: &mut [u8], key: &[u8]) -> usize {
    let mut pos = 0;
    buf[pos..pos + 4].copy_from_slice(b"get ");
    pos += 4;
    buf[pos..pos + key.len()].copy_from_slice(key);
    pos += key.len();
    buf[pos..pos + 2].copy_from_slice(b"\r\n");
    pos + 2
}

/// Encode a multi-GET command: `get <key1> <key2> ...\r\n`
fn encode_gets(buf: &mut [u8], keys: &[&[u8]]) -> usize {
    if keys.is_empty() {
        return 0;
    }

    let mut pos = 0;
    buf[pos..pos + 3].copy_from_slice(b"get");
    pos += 3;

    for key in keys {
        buf[pos] = b' ';
        pos += 1;
        buf[pos..pos + key.len()].copy_from_slice(key);
        pos += key.len();
    }

    buf[pos..pos + 2].copy_from_slice(b"\r\n");
    pos + 2
}

/// Encode a SET command: `set <key> <flags> <exptime> <bytes>\r\n<data>\r\n`
fn encode_set(buf: &mut [u8], key: &[u8], value: &[u8], flags: u32, exptime: u32) -> usize {
    let mut pos = 0;

    // set <key>
    buf[pos..pos + 4].copy_from_slice(b"set ");
    pos += 4;
    buf[pos..pos + key.len()].copy_from_slice(key);
    pos += key.len();
    buf[pos] = b' ';
    pos += 1;

    // <flags>
    let mut cursor = std::io::Cursor::new(&mut buf[pos..]);
    write!(cursor, "{} {} {}\r\n", flags, exptime, value.len()).unwrap();
    pos += cursor.position() as usize;

    // <data>\r\n
    buf[pos..pos + value.len()].copy_from_slice(value);
    pos += value.len();
    buf[pos..pos + 2].copy_from_slice(b"\r\n");
    pos + 2
}

/// Encode a DELETE command: `delete <key>\r\n`
fn encode_delete(buf: &mut [u8], key: &[u8]) -> usize {
    let mut pos = 0;
    buf[pos..pos + 7].copy_from_slice(b"delete ");
    pos += 7;
    buf[pos..pos + key.len()].copy_from_slice(key);
    pos += key.len();
    buf[pos..pos + 2].copy_from_slice(b"\r\n");
    pos + 2
}

/// Encode a simple command with no arguments.
fn encode_simple(buf: &mut [u8], cmd: &[u8]) -> usize {
    buf[..cmd.len()].copy_from_slice(cmd);
    buf[cmd.len()..cmd.len() + 2].copy_from_slice(b"\r\n");
    cmd.len() + 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_get() {
        let mut buf = [0u8; 64];
        let len = Request::get(b"mykey").encode(&mut buf);
        assert_eq!(&buf[..len], b"get mykey\r\n");
    }

    #[test]
    fn test_encode_gets() {
        let mut buf = [0u8; 64];
        let keys: &[&[u8]] = &[b"key1", b"key2", b"key3"];
        let len = Request::gets(keys).encode(&mut buf);
        assert_eq!(&buf[..len], b"get key1 key2 key3\r\n");
    }

    #[test]
    fn test_encode_set() {
        let mut buf = [0u8; 64];
        let len = Request::set(b"mykey", b"myvalue").encode(&mut buf);
        assert_eq!(&buf[..len], b"set mykey 0 0 7\r\nmyvalue\r\n");
    }

    #[test]
    fn test_encode_set_with_options() {
        let mut buf = [0u8; 64];
        let len = Request::set(b"mykey", b"myvalue")
            .flags(123)
            .exptime(3600)
            .encode(&mut buf);
        assert_eq!(&buf[..len], b"set mykey 123 3600 7\r\nmyvalue\r\n");
    }

    #[test]
    fn test_encode_delete() {
        let mut buf = [0u8; 64];
        let len = Request::delete(b"mykey").encode(&mut buf);
        assert_eq!(&buf[..len], b"delete mykey\r\n");
    }

    #[test]
    fn test_encode_flush_all() {
        let mut buf = [0u8; 64];
        let len = Request::flush_all().encode(&mut buf);
        assert_eq!(&buf[..len], b"flush_all\r\n");
    }

    #[test]
    fn test_encode_version() {
        let mut buf = [0u8; 64];
        let len = Request::version().encode(&mut buf);
        assert_eq!(&buf[..len], b"version\r\n");
    }

    #[test]
    fn test_encode_quit() {
        let mut buf = [0u8; 64];
        let len = Request::quit().encode(&mut buf);
        assert_eq!(&buf[..len], b"quit\r\n");
    }

    // Additional tests for improved coverage

    #[test]
    fn test_set_request_build() {
        let mut buf = [0u8; 64];
        let request = Request::set(b"mykey", b"myvalue")
            .flags(42)
            .exptime(600)
            .build();
        let len = request.encode(&mut buf);
        assert_eq!(&buf[..len], b"set mykey 42 600 7\r\nmyvalue\r\n");
    }

    #[test]
    fn test_encode_gets_empty() {
        let mut buf = [0u8; 64];
        let keys: &[&[u8]] = &[];
        let len = Request::gets(keys).encode(&mut buf);
        assert_eq!(len, 0);
    }

    #[test]
    fn test_encode_gets_single() {
        let mut buf = [0u8; 64];
        let keys: &[&[u8]] = &[b"single"];
        let len = Request::gets(keys).encode(&mut buf);
        assert_eq!(&buf[..len], b"get single\r\n");
    }

    #[test]
    fn test_request_debug() {
        let req = Request::get(b"key");
        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("Get"));
    }

    #[test]
    fn test_request_clone() {
        let req1 = Request::get(b"key");
        let req2 = req1.clone();
        // Both should encode the same
        let mut buf1 = [0u8; 64];
        let mut buf2 = [0u8; 64];
        let len1 = req1.encode(&mut buf1);
        let len2 = req2.encode(&mut buf2);
        assert_eq!(&buf1[..len1], &buf2[..len2]);
    }

    #[test]
    fn test_set_request_debug() {
        let req = Request::set(b"key", b"value");
        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("SetRequest"));
    }

    #[test]
    fn test_set_request_clone() {
        let req1 = Request::set(b"key", b"value").flags(1);
        let req2 = req1.clone();
        let mut buf1 = [0u8; 64];
        let mut buf2 = [0u8; 64];
        let len1 = req1.encode(&mut buf1);
        let len2 = req2.encode(&mut buf2);
        assert_eq!(&buf1[..len1], &buf2[..len2]);
    }

    #[test]
    fn test_encode_set_via_request() {
        let mut buf = [0u8; 64];
        let request = Request::Set {
            key: b"k",
            value: b"v",
            flags: 0,
            exptime: 0,
        };
        let len = request.encode(&mut buf);
        assert_eq!(&buf[..len], b"set k 0 0 1\r\nv\r\n");
    }

    #[test]
    fn test_encode_gets_via_request() {
        let mut buf = [0u8; 64];
        let keys: &[&[u8]] = &[b"a", b"b"];
        let request = Request::Gets { keys };
        let len = request.encode(&mut buf);
        assert_eq!(&buf[..len], b"get a b\r\n");
    }
}
