//! Server-side command parsing.
//!
//! This module parses RESP protocol data into structured commands for server implementations.
//! It provides zero-copy parsing where possible, with command arguments referencing the input buffer.

use crate::error::ParseError;
use crate::value::ParseOptions;

/// A parsed Redis command with references to the original buffer.
///
/// Commands are parsed with zero-copy semantics - the key and value fields
/// reference slices of the original input buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command<'a> {
    /// PING command
    Ping,
    /// GET key
    Get { key: &'a [u8] },
    /// SET key value [EX seconds] [PX milliseconds] [NX|XX]
    Set {
        key: &'a [u8],
        value: &'a [u8],
        ex: Option<u64>,
        px: Option<u64>,
        nx: bool,
        xx: bool,
    },
    /// DEL key
    Del { key: &'a [u8] },
    /// MGET key [key ...]
    MGet { keys: Vec<&'a [u8]> },
    /// CONFIG subcommand [args...]
    Config {
        subcommand: &'a [u8],
        args: Vec<&'a [u8]>,
    },
    /// FLUSHDB
    FlushDb,
    /// FLUSHALL
    FlushAll,
    /// HELLO [protover [AUTH username password] [SETNAME clientname]]
    /// Used for RESP3 protocol negotiation.
    #[cfg(feature = "resp3")]
    Hello {
        /// Protocol version (2 or 3)
        proto_version: Option<u8>,
        /// AUTH username and password
        auth: Option<(&'a [u8], &'a [u8])>,
        /// Client name
        client_name: Option<&'a [u8]>,
    },
}

impl<'a> Command<'a> {
    /// Parse a command from a byte buffer using default limits.
    ///
    /// Returns the parsed command and the number of bytes consumed.
    ///
    /// # Zero-copy
    ///
    /// The returned command contains references to the input buffer, so the buffer
    /// must outlive the command. This avoids allocation for keys and values.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::Incomplete` if more data is needed.
    /// Returns other errors for malformed or unknown commands.
    #[inline]
    pub fn parse(buffer: &'a [u8]) -> Result<(Self, usize), ParseError> {
        Self::parse_with_options(buffer, &ParseOptions::default())
    }

    /// Parse a command from a byte buffer with custom options.
    ///
    /// This is useful for setting custom limits on bulk string size to prevent
    /// denial-of-service attacks or to enforce server-side value size limits.
    ///
    /// # Zero-copy
    ///
    /// The returned command contains references to the input buffer, so the buffer
    /// must outlive the command. This avoids allocation for keys and values.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::Incomplete` if more data is needed.
    /// Returns `ParseError::BulkStringTooLong` if a bulk string exceeds the limit.
    /// Returns other errors for malformed or unknown commands.
    #[inline]
    pub fn parse_with_options(
        buffer: &'a [u8],
        options: &ParseOptions,
    ) -> Result<(Self, usize), ParseError> {
        let mut cursor = Cursor::new(buffer, options.max_bulk_string_len);

        // Read array header
        if cursor.remaining() < 1 {
            return Err(ParseError::Incomplete);
        }
        if cursor.get_u8() != b'*' {
            return Err(ParseError::Protocol("expected array".to_string()));
        }

        // Read array length
        let count = cursor.read_integer()?;
        if count < 1 {
            return Err(ParseError::Protocol(
                "array must have at least 1 element".to_string(),
            ));
        }
        // Reject unreasonably large arrays to prevent OOM attacks
        const MAX_ARRAY_LEN: usize = 1024 * 1024; // 1M elements max
        if count > MAX_ARRAY_LEN {
            return Err(ParseError::Protocol("array too large".to_string()));
        }

        // Read command name
        let cmd_name = cursor.read_bulk_string()?;
        let cmd_str = std::str::from_utf8(cmd_name)
            .map_err(|_| ParseError::Protocol("invalid UTF-8 in command".to_string()))?;

        // Parse command based on name (case-insensitive)
        let command = match () {
            _ if cmd_str.eq_ignore_ascii_case("ping") => {
                if count != 1 {
                    return Err(ParseError::WrongArity(
                        "PING takes no arguments".to_string(),
                    ));
                }
                Command::Ping
            }

            _ if cmd_str.eq_ignore_ascii_case("get") => {
                if count != 2 {
                    return Err(ParseError::WrongArity(
                        "GET requires exactly 1 argument".to_string(),
                    ));
                }
                let key = cursor.read_bulk_string()?;
                Command::Get { key }
            }

            _ if cmd_str.eq_ignore_ascii_case("set") => {
                if count < 3 {
                    return Err(ParseError::WrongArity(
                        "SET requires at least 2 arguments".to_string(),
                    ));
                }
                let key = cursor.read_bulk_string()?;
                let value = cursor.read_bulk_string()?;

                let mut ex = None;
                let mut px = None;
                let mut nx = false;
                let mut xx = false;

                let mut remaining_args = count - 3;
                while remaining_args > 0 {
                    let option = cursor.read_bulk_string()?;
                    let option_str = std::str::from_utf8(option)
                        .map_err(|_| ParseError::Protocol("invalid UTF-8 in option".to_string()))?;

                    if option_str.eq_ignore_ascii_case("ex") {
                        if remaining_args < 2 {
                            return Err(ParseError::Protocol("EX requires a value".to_string()));
                        }
                        let ttl_bytes = cursor.read_bulk_string()?;
                        let ttl_str = std::str::from_utf8(ttl_bytes).map_err(|_| {
                            ParseError::Protocol("invalid UTF-8 in TTL".to_string())
                        })?;
                        let ttl_secs = ttl_str
                            .parse::<u64>()
                            .map_err(|_| ParseError::Protocol("invalid TTL value".to_string()))?;
                        ex = Some(ttl_secs);
                        remaining_args -= 2;
                    } else if option_str.eq_ignore_ascii_case("px") {
                        if remaining_args < 2 {
                            return Err(ParseError::Protocol("PX requires a value".to_string()));
                        }
                        let ttl_bytes = cursor.read_bulk_string()?;
                        let ttl_str = std::str::from_utf8(ttl_bytes).map_err(|_| {
                            ParseError::Protocol("invalid UTF-8 in TTL".to_string())
                        })?;
                        let ttl_ms = ttl_str
                            .parse::<u64>()
                            .map_err(|_| ParseError::Protocol("invalid TTL value".to_string()))?;
                        px = Some(ttl_ms);
                        remaining_args -= 2;
                    } else if option_str.eq_ignore_ascii_case("nx") {
                        nx = true;
                        remaining_args -= 1;
                    } else if option_str.eq_ignore_ascii_case("xx") {
                        xx = true;
                        remaining_args -= 1;
                    } else {
                        return Err(ParseError::Protocol(format!(
                            "unknown SET option: {}",
                            option_str
                        )));
                    }
                }

                Command::Set {
                    key,
                    value,
                    ex,
                    px,
                    nx,
                    xx,
                }
            }

            _ if cmd_str.eq_ignore_ascii_case("del") => {
                if count != 2 {
                    return Err(ParseError::WrongArity(
                        "DEL requires exactly 1 argument".to_string(),
                    ));
                }
                let key = cursor.read_bulk_string()?;
                Command::Del { key }
            }

            _ if cmd_str.eq_ignore_ascii_case("mget") => {
                if count < 2 {
                    return Err(ParseError::WrongArity(
                        "MGET requires at least 1 argument".to_string(),
                    ));
                }
                let mut keys = Vec::with_capacity(count - 1);
                for _ in 0..(count - 1) {
                    keys.push(cursor.read_bulk_string()?);
                }
                Command::MGet { keys }
            }

            _ if cmd_str.eq_ignore_ascii_case("config") => {
                if count < 2 {
                    return Err(ParseError::WrongArity(
                        "CONFIG requires at least 1 argument".to_string(),
                    ));
                }
                let subcommand = cursor.read_bulk_string()?;
                let mut args = Vec::with_capacity(count - 2);
                for _ in 0..(count - 2) {
                    args.push(cursor.read_bulk_string()?);
                }
                Command::Config { subcommand, args }
            }

            _ if cmd_str.eq_ignore_ascii_case("flushdb") => Command::FlushDb,
            _ if cmd_str.eq_ignore_ascii_case("flushall") => Command::FlushAll,

            #[cfg(feature = "resp3")]
            _ if cmd_str.eq_ignore_ascii_case("hello") => {
                let mut proto_version = None;
                let mut auth = None;
                let mut client_name = None;

                let mut remaining_args = count - 1;

                // Parse optional protocol version
                if remaining_args > 0 {
                    let version_bytes = cursor.read_bulk_string()?;
                    let version_str = std::str::from_utf8(version_bytes).map_err(|_| {
                        ParseError::Protocol("invalid UTF-8 in version".to_string())
                    })?;
                    let version: u8 = version_str.parse().map_err(|_| {
                        ParseError::Protocol("invalid protocol version".to_string())
                    })?;
                    proto_version = Some(version);
                    remaining_args -= 1;
                }

                // Parse optional AUTH and SETNAME
                while remaining_args > 0 {
                    let option = cursor.read_bulk_string()?;
                    let option_str = std::str::from_utf8(option)
                        .map_err(|_| ParseError::Protocol("invalid UTF-8 in option".to_string()))?;

                    if option_str.eq_ignore_ascii_case("auth") {
                        if remaining_args < 3 {
                            return Err(ParseError::Protocol(
                                "AUTH requires username and password".to_string(),
                            ));
                        }
                        let username = cursor.read_bulk_string()?;
                        let password = cursor.read_bulk_string()?;
                        auth = Some((username, password));
                        remaining_args -= 3;
                    } else if option_str.eq_ignore_ascii_case("setname") {
                        if remaining_args < 2 {
                            return Err(ParseError::Protocol(
                                "SETNAME requires a name".to_string(),
                            ));
                        }
                        let name = cursor.read_bulk_string()?;
                        client_name = Some(name);
                        remaining_args -= 2;
                    } else {
                        return Err(ParseError::Protocol(format!(
                            "unknown HELLO option: {}",
                            option_str
                        )));
                    }
                }

                Command::Hello {
                    proto_version,
                    auth,
                    client_name,
                }
            }

            _ => {
                return Err(ParseError::UnknownCommand(cmd_str.to_string()));
            }
        };

        Ok((command, cursor.position()))
    }

    /// Returns the command name as a string.
    pub fn name(&self) -> &'static str {
        match self {
            Command::Ping => "PING",
            Command::Get { .. } => "GET",
            Command::Set { .. } => "SET",
            Command::Del { .. } => "DEL",
            Command::MGet { .. } => "MGET",
            Command::Config { .. } => "CONFIG",
            Command::FlushDb => "FLUSHDB",
            Command::FlushAll => "FLUSHALL",
            #[cfg(feature = "resp3")]
            Command::Hello { .. } => "HELLO",
        }
    }
}

/// A cursor for reading RESP data from a buffer.
struct Cursor<'a> {
    buffer: &'a [u8],
    pos: usize,
    max_bulk_string_len: usize,
}

impl<'a> Cursor<'a> {
    fn new(buffer: &'a [u8], max_bulk_string_len: usize) -> Self {
        Self {
            buffer,
            pos: 0,
            max_bulk_string_len,
        }
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.buffer.len() - self.pos
    }

    #[inline]
    fn position(&self) -> usize {
        self.pos
    }

    #[inline]
    fn get_u8(&mut self) -> u8 {
        let b = self.buffer[self.pos];
        self.pos += 1;
        b
    }

    fn read_integer(&mut self) -> Result<usize, ParseError> {
        let line = self.read_line()?;

        if line.is_empty() {
            return Err(ParseError::InvalidInteger("empty integer".to_string()));
        }

        // Limit integer length to prevent overflow during parsing.
        // usize::MAX is at most 20 digits, so 19 is a safe limit.
        if line.len() > 19 {
            return Err(ParseError::InvalidInteger("integer too large".to_string()));
        }

        let mut result = 0usize;
        for &byte in line {
            if !byte.is_ascii_digit() {
                return Err(ParseError::InvalidInteger(
                    "non-digit character".to_string(),
                ));
            }
            result = result
                .checked_mul(10)
                .and_then(|r| r.checked_add((byte - b'0') as usize))
                .ok_or_else(|| ParseError::InvalidInteger("integer overflow".to_string()))?;
        }
        Ok(result)
    }

    fn read_bulk_string(&mut self) -> Result<&'a [u8], ParseError> {
        if self.remaining() < 1 {
            return Err(ParseError::Incomplete);
        }

        if self.get_u8() != b'$' {
            return Err(ParseError::Protocol("expected bulk string".to_string()));
        }

        let len = self.read_integer()?;

        // Check bulk string length limit
        if len > self.max_bulk_string_len {
            return Err(ParseError::BulkStringTooLong {
                len,
                max: self.max_bulk_string_len,
            });
        }

        if self.remaining() < len + 2 {
            return Err(ParseError::Incomplete);
        }

        let data = &self.buffer[self.pos..self.pos + len];
        self.pos += len;

        // Verify CRLF
        if self.remaining() < 2 {
            return Err(ParseError::Incomplete);
        }
        if self.get_u8() != b'\r' || self.get_u8() != b'\n' {
            return Err(ParseError::Protocol(
                "expected CRLF after bulk string".to_string(),
            ));
        }

        Ok(data)
    }

    fn read_line(&mut self) -> Result<&'a [u8], ParseError> {
        let start = self.pos;
        let slice = &self.buffer[start..];

        if let Some(pos) = memchr::memchr(b'\r', slice)
            && pos + 1 < slice.len()
            && slice[pos + 1] == b'\n'
        {
            let end = start + pos;
            let line = &self.buffer[start..end];
            self.pos = end + 2;
            return Ok(line);
        }

        Err(ParseError::Incomplete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ping() {
        let data = b"*1\r\n$4\r\nPING\r\n";
        let (cmd, consumed) = Command::parse(data).unwrap();
        assert_eq!(cmd, Command::Ping);
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn test_parse_get() {
        let data = b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n";
        let (cmd, consumed) = Command::parse(data).unwrap();
        assert_eq!(cmd, Command::Get { key: b"mykey" });
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn test_parse_set() {
        let data = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
        let (cmd, consumed) = Command::parse(data).unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: b"mykey",
                value: b"myvalue",
                ex: None,
                px: None,
                nx: false,
                xx: false,
            }
        );
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn test_parse_set_ex() {
        let data = b"*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n$2\r\nEX\r\n$4\r\n3600\r\n";
        let (cmd, consumed) = Command::parse(data).unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: b"mykey",
                value: b"myvalue",
                ex: Some(3600),
                px: None,
                nx: false,
                xx: false,
            }
        );
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn test_parse_set_px_nx() {
        let data =
            b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n$4\r\n1000\r\n*5\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n$2\r\nNX\r\n";
        let (cmd, _) = Command::parse(data).unwrap();
        if let Command::Set { px, .. } = cmd {
            assert_eq!(px, Some(1000));
        } else {
            panic!("Expected SET command");
        }
    }

    #[test]
    fn test_parse_del() {
        let data = b"*2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n";
        let (cmd, consumed) = Command::parse(data).unwrap();
        assert_eq!(cmd, Command::Del { key: b"mykey" });
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn test_parse_mget() {
        let data = b"*4\r\n$4\r\nMGET\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n";
        let (cmd, consumed) = Command::parse(data).unwrap();
        assert_eq!(
            cmd,
            Command::MGet {
                keys: vec![b"key1" as &[u8], b"key2", b"key3"]
            }
        );
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn test_parse_config() {
        let data = b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nmaxclients\r\n";
        let (cmd, consumed) = Command::parse(data).unwrap();
        assert_eq!(
            cmd,
            Command::Config {
                subcommand: b"GET",
                args: vec![b"maxclients" as &[u8]]
            }
        );
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn test_parse_flushdb() {
        let data = b"*1\r\n$7\r\nFLUSHDB\r\n";
        let (cmd, consumed) = Command::parse(data).unwrap();
        assert_eq!(cmd, Command::FlushDb);
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn test_parse_flushall() {
        let data = b"*1\r\n$8\r\nFLUSHALL\r\n";
        let (cmd, consumed) = Command::parse(data).unwrap();
        assert_eq!(cmd, Command::FlushAll);
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn test_parse_case_insensitive() {
        let data = b"*2\r\n$3\r\nget\r\n$5\r\nmykey\r\n";
        let (cmd, _) = Command::parse(data).unwrap();
        assert_eq!(cmd, Command::Get { key: b"mykey" });

        let data = b"*2\r\n$3\r\nGeT\r\n$5\r\nmykey\r\n";
        let (cmd, _) = Command::parse(data).unwrap();
        assert_eq!(cmd, Command::Get { key: b"mykey" });
    }

    #[test]
    fn test_parse_incomplete() {
        assert!(matches!(
            Command::parse(b"*2\r\n$3\r\nGET"),
            Err(ParseError::Incomplete)
        ));
        assert!(matches!(
            Command::parse(b"*2\r\n"),
            Err(ParseError::Incomplete)
        ));
        assert!(matches!(Command::parse(b""), Err(ParseError::Incomplete)));
    }

    #[test]
    fn test_parse_unknown_command() {
        let data = b"*1\r\n$7\r\nUNKNOWN\r\n";
        assert!(matches!(
            Command::parse(data),
            Err(ParseError::UnknownCommand(_))
        ));
    }

    #[test]
    fn test_parse_wrong_arity() {
        let data = b"*1\r\n$3\r\nGET\r\n"; // GET with no key
        assert!(matches!(
            Command::parse(data),
            Err(ParseError::WrongArity(_))
        ));
    }

    #[test]
    fn test_command_name() {
        assert_eq!(Command::Ping.name(), "PING");
        assert_eq!(Command::Get { key: b"k" }.name(), "GET");
        assert_eq!(
            Command::Set {
                key: b"k",
                value: b"v",
                ex: None,
                px: None,
                nx: false,
                xx: false
            }
            .name(),
            "SET"
        );
        assert_eq!(Command::Del { key: b"k" }.name(), "DEL");
        assert_eq!(Command::MGet { keys: vec![] }.name(), "MGET");
        assert_eq!(
            Command::Config {
                subcommand: b"GET",
                args: vec![]
            }
            .name(),
            "CONFIG"
        );
        assert_eq!(Command::FlushDb.name(), "FLUSHDB");
        assert_eq!(Command::FlushAll.name(), "FLUSHALL");
    }

    #[test]
    fn test_parse_set_xx() {
        let data = b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nXX\r\n";
        let (cmd, consumed) = Command::parse(data).unwrap();
        if let Command::Set { xx, nx, .. } = cmd {
            assert!(xx);
            assert!(!nx);
        } else {
            panic!("Expected SET command");
        }
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn test_parse_set_unknown_option() {
        let data = b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$7\r\nINVALID\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Protocol(_))));
    }

    #[test]
    fn test_parse_set_ex_missing_value() {
        // SET k v EX (missing the expiration value)
        let data = b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Protocol(_))));
    }

    #[test]
    fn test_parse_set_px_missing_value() {
        // SET k v PX (missing the expiration value)
        let data = b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Protocol(_))));
    }

    #[test]
    fn test_parse_set_invalid_ttl() {
        // SET k v EX invalid
        let data = b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$3\r\nabc\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Protocol(_))));
    }

    #[test]
    fn test_parse_ping_wrong_arity() {
        // PING with extra argument
        let data = b"*2\r\n$4\r\nPING\r\n$5\r\nextra\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::WrongArity(_))));
    }

    #[test]
    fn test_parse_del_wrong_arity() {
        // DEL with no key
        let data = b"*1\r\n$3\r\nDEL\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::WrongArity(_))));
    }

    #[test]
    fn test_parse_mget_wrong_arity() {
        // MGET with no keys
        let data = b"*1\r\n$4\r\nMGET\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::WrongArity(_))));
    }

    #[test]
    fn test_parse_config_wrong_arity() {
        // CONFIG with no subcommand
        let data = b"*1\r\n$6\r\nCONFIG\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::WrongArity(_))));
    }

    #[test]
    fn test_parse_set_wrong_arity() {
        // SET with only key
        let data = b"*2\r\n$3\r\nSET\r\n$1\r\nk\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::WrongArity(_))));
    }

    #[test]
    fn test_parse_not_array() {
        // Command not starting with array
        let data = b"+OK\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Protocol(_))));
    }

    #[test]
    fn test_parse_empty_array() {
        // Empty array (0 elements)
        let data = b"*0\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Protocol(_))));
    }

    #[test]
    fn test_command_debug() {
        let cmd = Command::Ping;
        let debug_str = format!("{:?}", cmd);
        assert!(debug_str.contains("Ping"));
    }

    #[test]
    fn test_command_clone() {
        let cmd1 = Command::Get { key: b"mykey" };
        let cmd2 = cmd1.clone();
        assert_eq!(cmd1, cmd2);
    }

    #[test]
    fn test_command_eq() {
        assert_eq!(Command::Ping, Command::Ping);
        assert_ne!(Command::Ping, Command::FlushDb);
        assert_eq!(Command::Get { key: b"a" }, Command::Get { key: b"a" });
        assert_ne!(Command::Get { key: b"a" }, Command::Get { key: b"b" });
    }

    #[test]
    fn test_parse_config_set() {
        let data = b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$10\r\nmaxclients\r\n$3\r\n100\r\n";
        let (cmd, consumed) = Command::parse(data).unwrap();
        if let Command::Config { subcommand, args } = cmd {
            assert_eq!(subcommand, b"SET");
            assert_eq!(args.len(), 2);
        } else {
            panic!("Expected CONFIG command");
        }
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn test_parse_config_no_args() {
        let data = b"*2\r\n$6\r\nCONFIG\r\n$4\r\nINFO\r\n";
        let (cmd, consumed) = Command::parse(data).unwrap();
        if let Command::Config { subcommand, args } = cmd {
            assert_eq!(subcommand, b"INFO");
            assert!(args.is_empty());
        } else {
            panic!("Expected CONFIG command");
        }
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn test_parse_integer_too_large() {
        // Array with length that's too large (>19 digits)
        let data = b"*12345678901234567890123\r\n$4\r\nPING\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::InvalidInteger(_))));
    }

    #[test]
    fn test_parse_integer_non_digit() {
        // Array length with non-digit character
        let data = b"*12a\r\n$4\r\nPING\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::InvalidInteger(_))));
    }

    #[test]
    fn test_parse_integer_overflow() {
        // Array length that causes overflow
        let data = b"*99999999999999999999\r\n$4\r\nPING\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::InvalidInteger(_))));
    }

    #[test]
    fn test_parse_bulk_string_missing_crlf() {
        // Bulk string with wrong trailing bytes
        let data = b"*2\r\n$3\r\nGET\r\n$5\r\nmykeyXX";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Protocol(_))));
    }

    #[test]
    fn test_parse_not_bulk_string() {
        // Command name not a bulk string
        let data = b"*2\r\n+GET\r\n$5\r\nmykey\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Protocol(_))));
    }

    #[test]
    fn test_parse_invalid_utf8_command() {
        // Command name with invalid UTF-8
        let data = b"*1\r\n$2\r\n\xff\xfe\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Protocol(_))));
    }

    #[test]
    fn test_parse_set_invalid_utf8_option() {
        // SET with invalid UTF-8 in option
        let data = b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\n\xff\xfe\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Protocol(_))));
    }

    #[test]
    fn test_parse_set_invalid_utf8_in_ttl() {
        // SET with invalid UTF-8 in TTL value
        let data = b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n\xff\xfe\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Protocol(_))));
    }

    #[test]
    fn test_parse_set_invalid_px_ttl() {
        // SET k v PX invalid
        let data = b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n$3\r\nabc\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Protocol(_))));
    }

    #[test]
    fn test_parse_set_nx() {
        let data = b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nNX\r\n";
        let (cmd, consumed) = Command::parse(data).unwrap();
        if let Command::Set { nx, xx, .. } = cmd {
            assert!(nx);
            assert!(!xx);
        } else {
            panic!("Expected SET command");
        }
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn test_parse_bulk_string_incomplete() {
        // Bulk string with incomplete data
        let data = b"*2\r\n$3\r\nGET\r\n$100\r\nmykey\r\n"; // claims 100 bytes but only 5
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Incomplete)));
    }

    #[test]
    fn test_parse_mget_huge_count_no_oom() {
        // MGET with huge array count should reject as protocol error, not OOM
        let data = b"*1177777777\r\n$4\r\nmGet\r\n";
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Protocol(_))));
    }

    #[test]
    fn test_parse_array_too_large() {
        // Array larger than MAX_ARRAY_LEN (1M) should be rejected
        let data = b"*1048577\r\n$4\r\nPING\r\n"; // 1M + 1
        let result = Command::parse(data);
        assert!(matches!(result, Err(ParseError::Protocol(_))));
    }

    // ========================================================================
    // RESP3 Tests
    // ========================================================================

    #[cfg(feature = "resp3")]
    mod resp3_tests {
        use super::*;

        #[test]
        fn test_parse_hello_no_args() {
            let data = b"*1\r\n$5\r\nHELLO\r\n";
            let (cmd, consumed) = Command::parse(data).unwrap();
            assert_eq!(
                cmd,
                Command::Hello {
                    proto_version: None,
                    auth: None,
                    client_name: None,
                }
            );
            assert_eq!(consumed, data.len());
        }

        #[test]
        fn test_parse_hello_with_version() {
            let data = b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n";
            let (cmd, consumed) = Command::parse(data).unwrap();
            assert_eq!(
                cmd,
                Command::Hello {
                    proto_version: Some(3),
                    auth: None,
                    client_name: None,
                }
            );
            assert_eq!(consumed, data.len());
        }

        #[test]
        fn test_parse_hello_with_auth() {
            // HELLO 3 AUTH username password
            let data =
                b"*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n$4\r\npass\r\n";
            let (cmd, consumed) = Command::parse(data).unwrap();
            assert_eq!(
                cmd,
                Command::Hello {
                    proto_version: Some(3),
                    auth: Some((b"user" as &[u8], b"pass" as &[u8])),
                    client_name: None,
                }
            );
            assert_eq!(consumed, data.len());
        }

        #[test]
        fn test_parse_hello_with_setname() {
            // HELLO 3 SETNAME myapp
            let data = b"*4\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$7\r\nSETNAME\r\n$5\r\nmyapp\r\n";
            let (cmd, consumed) = Command::parse(data).unwrap();
            assert_eq!(
                cmd,
                Command::Hello {
                    proto_version: Some(3),
                    auth: None,
                    client_name: Some(b"myapp" as &[u8]),
                }
            );
            assert_eq!(consumed, data.len());
        }

        #[test]
        fn test_parse_hello_full() {
            // HELLO 3 AUTH user pass SETNAME myapp
            let data = b"*7\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n$4\r\npass\r\n$7\r\nSETNAME\r\n$5\r\nmyapp\r\n";
            let (cmd, consumed) = Command::parse(data).unwrap();
            assert_eq!(
                cmd,
                Command::Hello {
                    proto_version: Some(3),
                    auth: Some((b"user" as &[u8], b"pass" as &[u8])),
                    client_name: Some(b"myapp" as &[u8]),
                }
            );
            assert_eq!(consumed, data.len());
        }

        #[test]
        fn test_hello_command_name() {
            assert_eq!(
                Command::Hello {
                    proto_version: Some(3),
                    auth: None,
                    client_name: None
                }
                .name(),
                "HELLO"
            );
        }

        #[test]
        fn test_parse_hello_auth_missing_args() {
            // HELLO 3 AUTH user (missing password)
            let data = b"*4\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n";
            let result = Command::parse(data);
            assert!(matches!(result, Err(ParseError::Protocol(_))));
        }

        #[test]
        fn test_parse_hello_setname_missing_args() {
            // HELLO 3 SETNAME (missing name)
            let data = b"*3\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$7\r\nSETNAME\r\n";
            let result = Command::parse(data);
            assert!(matches!(result, Err(ParseError::Protocol(_))));
        }

        #[test]
        fn test_parse_hello_unknown_option() {
            // HELLO 3 INVALID
            let data = b"*3\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$7\r\nINVALID\r\n";
            let result = Command::parse(data);
            assert!(matches!(result, Err(ParseError::Protocol(_))));
        }

        #[test]
        fn test_parse_hello_invalid_version() {
            // HELLO abc (invalid version)
            let data = b"*2\r\n$5\r\nHELLO\r\n$3\r\nabc\r\n";
            let result = Command::parse(data);
            assert!(matches!(result, Err(ParseError::Protocol(_))));
        }

        #[test]
        fn test_parse_hello_invalid_utf8_option() {
            // HELLO 3 with invalid UTF-8 option
            let data = b"*3\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$2\r\n\xff\xfe\r\n";
            let result = Command::parse(data);
            assert!(matches!(result, Err(ParseError::Protocol(_))));
        }

        #[test]
        fn test_parse_hello_invalid_utf8_version() {
            // HELLO with invalid UTF-8 version
            let data = b"*2\r\n$5\r\nHELLO\r\n$2\r\n\xff\xfe\r\n";
            let result = Command::parse(data);
            assert!(matches!(result, Err(ParseError::Protocol(_))));
        }
    }
}
