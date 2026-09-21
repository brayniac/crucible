//! Trace file reading for replay-driven benchmarking.
//!
//! Two on-disk layouts are supported, and they answer different questions.
//! The Twitter cluster layout carries an op code and a TTL per record, so it
//! exercises overwrite, delete and TTL expiry — the mechanisms that separate
//! one segment eviction policy from another. libCacheSim's `oracleGeneral`
//! layout carries neither: every record is a GET that inserts on miss. It is
//! kept because its next-access field is what yields a Belady reference curve,
//! not because it can compare eviction policies.
//!
//! The Twitter layout is the one released with the cluster traces at
//! <https://github.com/twitter/cache-trace>, whose own documentation
//! specifies the 20-byte record. Note the public release identifies clusters
//! by number; any internal service naming is not part of that dataset and is
//! deliberately absent here.
//!
//! See `docs/superpowers/specs/2026-09-18-s3fifo-main-pool-experiment-design.md`.

/// Which on-disk layout a trace file carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceFormat {
    /// Twitter cluster binary: 20-byte records with op code and TTL.
    Twitter,
    /// libCacheSim `oracleGeneral` binary: 24-byte records, GET-only.
    OracleGeneral,
    /// libCacheSim `oracleGeneral` CSV text: `time,object,size,next_access`.
    OracleGeneralCsv,
}

/// Key width used for `oracleGeneral` traces, which carry no key length.
///
/// 20 is `u64::MAX`'s decimal width, so every synthesized key is exactly this
/// long. A narrower width would make key length vary with the object id, which
/// would make item size — and therefore how many items fit in a segment — a
/// function of the trace's id distribution rather than a controlled input.
pub const ORACLE_GENERAL_KEY_LEN: u16 = 20;

/// Operation carried by a Twitter cluster record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    Get = 1,
    Gets = 2,
    Set = 3,
    Add = 4,
    Cas = 5,
    Replace = 6,
    Append = 7,
    Prepend = 8,
    Delete = 9,
}

impl Op {
    /// Decode an op code, rejecting anything the trace format does not define.
    ///
    /// Returns the offending byte rather than defaulting to `Get`: a trace
    /// whose op codes we cannot read is a trace we are misinterpreting, and a
    /// silent default would turn that into a plausible-looking hit ratio.
    pub fn from_u8(v: u8) -> Result<Self, u8> {
        match v {
            1 => Ok(Op::Get),
            2 => Ok(Op::Gets),
            3 => Ok(Op::Set),
            4 => Ok(Op::Add),
            5 => Ok(Op::Cas),
            6 => Ok(Op::Replace),
            7 => Ok(Op::Append),
            8 => Ok(Op::Prepend),
            9 => Ok(Op::Delete),
            other => Err(other),
        }
    }
}

/// One replayed request, independent of which layout it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceRecord {
    /// Object identity. Formatted into key bytes by the replay driver.
    pub key_id: u64,
    /// Width to format the key to, in bytes.
    pub key_len: u16,
    /// Value size in bytes.
    pub value_len: u32,
    /// Operation to perform.
    pub op: Op,
    /// TTL in seconds; 0 means no expiry.
    pub ttl_secs: u32,
}

impl TraceRecord {
    /// Decode a Twitter cluster record.
    ///
    /// Layout: `[0..4] timestamp`, `[4..12] key id`, `[12..16]` packs key
    /// length in the upper 10 bits and value length in the lower 22, and
    /// `[16..20]` packs the op code in the upper 8 bits and TTL in the lower 24.
    pub fn from_twitter_bytes(data: &[u8; 20]) -> Result<Self, u8> {
        let key_id = u64::from_le_bytes(data[4..12].try_into().expect("8 bytes"));
        let kv_packed = u32::from_le_bytes(data[12..16].try_into().expect("4 bytes"));
        let op_ttl_packed = u32::from_le_bytes(data[16..20].try_into().expect("4 bytes"));

        Ok(Self {
            key_id,
            key_len: (kv_packed >> 22) as u16,
            value_len: kv_packed & 0x003F_FFFF,
            op: Op::from_u8((op_ttl_packed >> 24) as u8)?,
            ttl_secs: op_ttl_packed & 0x00FF_FFFF,
        })
    }

    /// Decode an `oracleGeneral` record.
    ///
    /// Layout: `[0..4] timestamp`, `[4..12] object id`, `[12..16] size`,
    /// `[16..24] next access virtual time`. The next-access field is not
    /// carried on the record: nothing in the replay path consumes it, and the
    /// Belady curve it feeds is produced by libCacheSim rather than here.
    pub fn from_oracle_general_bytes(data: &[u8; 24]) -> Self {
        Self {
            key_id: u64::from_le_bytes(data[4..12].try_into().expect("8 bytes")),
            key_len: ORACLE_GENERAL_KEY_LEN,
            value_len: u32::from_le_bytes(data[12..16].try_into().expect("4 bytes")),
            op: Op::Get,
            ttl_secs: 0,
        }
    }
}

/// Streaming reader over a trace file.
///
/// Constructed from an arbitrary `BufRead` rather than only a path so the
/// truncation and framing behaviour can be tested against an in-memory buffer;
/// [`TraceReader::open`] is the thin path-taking wrapper over it.
pub struct TraceReader {
    inner: Box<dyn std::io::BufRead>,
    format: TraceFormat,
    records_read: u64,
    line: String,
}

impl TraceReader {
    /// Wrap an already-open reader.
    pub fn new(inner: Box<dyn std::io::BufRead>, format: TraceFormat) -> Self {
        Self {
            inner,
            format,
            records_read: 0,
            line: String::new(),
        }
    }

    /// How many records have been yielded so far.
    ///
    /// The replay driver compares this against the file's expected record
    /// count. A short read that reports a clean end is the failure mode this
    /// exists to catch: it produces a complete-looking run over a fraction of
    /// the trace.
    pub fn records_read(&self) -> u64 {
        self.records_read
    }

    /// Open a trace file, transparently decompressing a `.zst` path.
    ///
    /// Compression is decided by extension rather than by sniffing the magic
    /// bytes: a trace whose name says `.zst` but whose contents are not is a
    /// corrupt input, and reading it as plain binary would decode compressed
    /// bytes into plausible-looking records instead of failing.
    pub fn open(path: &std::path::Path, format: TraceFormat) -> std::io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let raw = std::io::BufReader::with_capacity(1 << 20, file);

        let inner: Box<dyn std::io::BufRead> = if path.extension().is_some_and(|e| e == "zst") {
            Box::new(std::io::BufReader::with_capacity(
                1 << 20,
                zstd::Decoder::new(raw)?,
            ))
        } else {
            Box::new(raw)
        };

        Ok(Self::new(inner, format))
    }

    /// Yield the next record, `None` at a clean end of file.
    ///
    /// A partial trailing record is an error rather than a clean end.
    pub fn next_record(&mut self) -> Option<std::io::Result<TraceRecord>> {
        let result = match self.format {
            TraceFormat::Twitter => self.next_twitter()?,
            TraceFormat::OracleGeneral => self.next_oracle_general()?,
            TraceFormat::OracleGeneralCsv => self.next_oracle_general_csv()?,
        };
        if result.is_ok() {
            self.records_read += 1;
        }
        Some(result)
    }

    fn next_twitter(&mut self) -> Option<std::io::Result<TraceRecord>> {
        let mut buf = [0u8; 20];
        match fill_record(&mut self.inner, &mut buf) {
            Ok(false) => None,
            Ok(true) => Some(TraceRecord::from_twitter_bytes(&buf).map_err(|code| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unknown op code {code} at record {}", self.records_read),
                )
            })),
            Err(e) => Some(Err(e)),
        }
    }

    fn next_oracle_general(&mut self) -> Option<std::io::Result<TraceRecord>> {
        let mut buf = [0u8; 24];
        match fill_record(&mut self.inner, &mut buf) {
            Ok(false) => None,
            Ok(true) => Some(Ok(TraceRecord::from_oracle_general_bytes(&buf))),
            Err(e) => Some(Err(e)),
        }
    }

    fn next_oracle_general_csv(&mut self) -> Option<std::io::Result<TraceRecord>> {
        loop {
            self.line.clear();
            match self.inner.read_line(&mut self.line) {
                Ok(0) => return None,
                Ok(_) => {}
                Err(e) => return Some(Err(e)),
            }

            let trimmed = self.line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            return Some(parse_oracle_general_csv_line(trimmed).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("malformed csv at record {}: {trimmed}", self.records_read),
                )
            }));
        }
    }
}

/// Fill `buf` completely, distinguishing a clean end of file from a short read.
///
/// `read_exact` collapses both into `UnexpectedEof`, which would make a trace
/// that ends exactly on a record boundary indistinguishable from one truncated
/// mid-record. Returns `Ok(false)` only when zero bytes were available.
fn fill_record(reader: &mut dyn std::io::BufRead, buf: &mut [u8]) -> std::io::Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }

    match filled {
        0 => Ok(false),
        n if n == buf.len() => Ok(true),
        n => Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "truncated trace: {n} of {} bytes in final record",
                buf.len()
            ),
        )),
    }
}

/// Parse one `time, object, size, next_access_vtime` line.
fn parse_oracle_general_csv_line(line: &str) -> Option<TraceRecord> {
    let mut fields = line.split(',').map(str::trim);
    let _time = fields.next()?;
    let key_id: u64 = fields.next()?.parse().ok()?;
    let value_len: u32 = fields.next()?.parse().ok()?;

    Some(TraceRecord {
        key_id,
        key_len: ORACLE_GENERAL_KEY_LEN,
        value_len,
        op: Op::Get,
        ttl_secs: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a Twitter record from its unpacked fields, so the tests state the
    /// layout once in the direction the reader does not.
    fn twitter_bytes(key_id: u64, key_len: u16, value_len: u32, op: u8, ttl: u32) -> [u8; 20] {
        let mut out = [0u8; 20];
        out[0..4].copy_from_slice(&7u32.to_le_bytes());
        out[4..12].copy_from_slice(&key_id.to_le_bytes());
        let kv_packed = ((key_len as u32) << 22) | (value_len & 0x003F_FFFF);
        out[12..16].copy_from_slice(&kv_packed.to_le_bytes());
        let op_ttl_packed = ((op as u32) << 24) | (ttl & 0x00FF_FFFF);
        out[16..20].copy_from_slice(&op_ttl_packed.to_le_bytes());
        out
    }

    #[test]
    fn a_twitter_record_unpacks_key_length_from_the_upper_ten_bits() {
        let bytes = twitter_bytes(42, 1023, 1234, Op::Get as u8, 0);
        let record = TraceRecord::from_twitter_bytes(&bytes).unwrap();

        assert_eq!(record.key_id, 42);
        assert_eq!(record.key_len, 1023);
        assert_eq!(record.value_len, 1234);
    }

    #[test]
    fn a_twitter_record_unpacks_ttl_from_the_lower_twenty_four_bits() {
        let bytes = twitter_bytes(1, 16, 100, Op::Set as u8, 0x00FF_FFFF);
        let record = TraceRecord::from_twitter_bytes(&bytes).unwrap();

        assert_eq!(record.op, Op::Set);
        assert_eq!(record.ttl_secs, 0x00FF_FFFF);
    }

    #[test]
    fn an_unknown_twitter_op_code_is_an_error_not_a_silent_get() {
        let bytes = twitter_bytes(1, 16, 100, 200, 0);

        assert_eq!(TraceRecord::from_twitter_bytes(&bytes), Err(200));
    }

    #[test]
    fn an_oracle_general_record_decodes_as_a_get_with_no_ttl() {
        let mut bytes = [0u8; 24];
        bytes[0..4].copy_from_slice(&7u32.to_le_bytes());
        bytes[4..12].copy_from_slice(&99u64.to_le_bytes());
        bytes[12..16].copy_from_slice(&737u32.to_le_bytes());
        bytes[16..24].copy_from_slice(&13i64.to_le_bytes());

        let record = TraceRecord::from_oracle_general_bytes(&bytes);

        assert_eq!(record.key_id, 99);
        assert_eq!(record.value_len, 737);
        assert_eq!(record.op, Op::Get);
        assert_eq!(record.ttl_secs, 0);
        assert_eq!(record.key_len, ORACLE_GENERAL_KEY_LEN);
    }

    fn reader_over(bytes: Vec<u8>, format: TraceFormat) -> TraceReader {
        TraceReader::new(Box::new(std::io::Cursor::new(bytes)), format)
    }

    #[test]
    fn a_reader_yields_every_record_in_a_whole_file() {
        let mut bytes = Vec::new();
        for id in 0..3u64 {
            bytes.extend_from_slice(&twitter_bytes(id, 16, 100, Op::Get as u8, 0));
        }
        let mut reader = reader_over(bytes, TraceFormat::Twitter);

        let ids: Vec<u64> = std::iter::from_fn(|| reader.next_record())
            .map(|r| r.unwrap().key_id)
            .collect();

        assert_eq!(ids, vec![0, 1, 2]);
        assert_eq!(reader.records_read(), 3);
    }

    #[test]
    fn a_trailing_partial_record_is_an_error_not_a_clean_end() {
        let mut bytes = twitter_bytes(0, 16, 100, Op::Get as u8, 0).to_vec();
        bytes.extend_from_slice(&[0u8; 7]);
        let mut reader = reader_over(bytes, TraceFormat::Twitter);

        assert_eq!(reader.next_record().unwrap().unwrap().key_id, 0);

        let tail = reader
            .next_record()
            .expect("a partial record must surface, not read as end of file");
        assert_eq!(
            tail.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof,
            "7 bytes of a 20-byte record is a truncated trace"
        );
    }

    #[test]
    fn an_undecodable_record_stops_the_reader_rather_than_being_skipped() {
        let mut bytes = twitter_bytes(0, 16, 100, Op::Get as u8, 0).to_vec();
        bytes.extend_from_slice(&twitter_bytes(1, 16, 100, 200, 0));
        let mut reader = reader_over(bytes, TraceFormat::Twitter);

        assert_eq!(reader.next_record().unwrap().unwrap().key_id, 0);

        let bad = reader.next_record().expect("a bad op code must surface");
        assert_eq!(bad.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_csv_reader_skips_comment_lines_and_parses_the_rest() {
        let text = "# time, object, size, next_access_vtime\n\
                    0, 13053225291711363978, 737, 13\n\
                    0, 61177148907475485, 248, 6570713\n";
        let mut reader = reader_over(text.as_bytes().to_vec(), TraceFormat::OracleGeneralCsv);

        let first = reader.next_record().unwrap().unwrap();
        let second = reader.next_record().unwrap().unwrap();

        assert_eq!(first.key_id, 13053225291711363978);
        assert_eq!(first.value_len, 737);
        assert_eq!(first.op, Op::Get);
        assert_eq!(second.key_id, 61177148907475485);
        assert!(reader.next_record().is_none());
        assert_eq!(reader.records_read(), 2);
    }

    #[test]
    fn open_reads_a_plain_binary_trace_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.bin");
        std::fs::write(&path, twitter_bytes(5, 16, 100, Op::Get as u8, 0)).unwrap();

        let mut reader = TraceReader::open(&path, TraceFormat::Twitter).unwrap();

        assert_eq!(reader.next_record().unwrap().unwrap().key_id, 5);
        assert!(reader.next_record().is_none());
    }

    #[test]
    fn open_decompresses_a_zst_trace_transparently() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.bin.zst");
        let plain = twitter_bytes(7, 16, 100, Op::Get as u8, 0);
        std::fs::write(&path, zstd::encode_all(&plain[..], 0).unwrap()).unwrap();

        let mut reader = TraceReader::open(&path, TraceFormat::Twitter).unwrap();

        assert_eq!(
            reader.next_record().unwrap().unwrap().key_id,
            7,
            "a .zst trace must decode to the same records as its plain form"
        );
    }
}
