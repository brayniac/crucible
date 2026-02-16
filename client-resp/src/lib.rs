//! Async Redis client backed by Tokio.
//!
//! Each connection is a multiplexed Tokio task with batched writes and
//! in-order RESP response matching. Supports ketama consistent hashing
//! for multi-server deployments.
//!
//! # Example
//!
//! ```no_run
//! use crucible_resp_client::{Client, ClientConfig};
//!
//! # async fn example() -> Result<(), crucible_resp_client::ClientError> {
//! let client = Client::connect(ClientConfig {
//!     servers: vec!["127.0.0.1:6379".to_string()],
//!     ..Default::default()
//! }).await?;
//!
//! client.set(b"hello", b"world").await?;
//! let val = client.get(b"hello").await?;
//! assert_eq!(val.as_deref(), Some(b"world".as_ref()));
//!
//! client.del(b"hello").await?;
//! # Ok(())
//! # }
//! ```

mod config;
mod connection;
mod error;
pub mod pipeline;

pub use config::ClientConfig;
pub use error::ClientError;
pub use pipeline::Pipeline;

use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use protocol_resp::{Request, Value};
use tokio::sync::{mpsc, oneshot};

use connection::{InFlightCommand, spawn_connection};

/// Async Redis client. Clone-able, Send + Sync.
///
/// All clones share the same connection pools.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    ring: ketama::Ring,
    pools: Vec<Pool>,
    command_timeout: Option<std::time::Duration>,
}

struct Pool {
    connections: Vec<mpsc::Sender<InFlightCommand>>,
    next: AtomicUsize,
}

impl Client {
    /// Connect to one or more Redis servers. Resolves addresses, builds
    /// ketama ring, and spawns connection manager tasks.
    pub async fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        let pool_size = config.pool_size.max(1);

        // Resolve server addresses
        let mut server_addrs = Vec::with_capacity(config.servers.len());
        for server in &config.servers {
            let addr = server
                .to_socket_addrs()
                .map_err(ClientError::Io)?
                .next()
                .ok_or_else(|| {
                    ClientError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("cannot resolve address: {server}"),
                    ))
                })?;
            server_addrs.push(addr);
        }

        // Build ketama ring
        let server_ids: Vec<String> = server_addrs.iter().map(|a| a.to_string()).collect();
        let ring = ketama::Ring::build(
            &server_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        );

        // Spawn connection pools
        let mut pools = Vec::with_capacity(server_addrs.len());
        for &addr in &server_addrs {
            let mut connections = Vec::with_capacity(pool_size);
            for _ in 0..pool_size {
                let (tx, rx) = mpsc::channel(config.channel_buffer);
                spawn_connection(addr, config.connect_timeout, config.tcp_nodelay, rx);
                connections.push(tx);
            }
            pools.push(Pool {
                connections,
                next: AtomicUsize::new(0),
            });
        }

        Ok(Client {
            inner: Arc::new(ClientInner {
                ring,
                pools,
                command_timeout: config.command_timeout,
            }),
        })
    }

    /// Execute an encoded command routed by key.
    async fn execute(&self, key: &[u8], encoded: Vec<u8>) -> Result<Value, ClientError> {
        let shard = self.inner.ring.route(key);
        let pool = &self.inner.pools[shard];
        let idx = pool.next.fetch_add(1, Ordering::Relaxed) % pool.connections.len();
        let sender = &pool.connections[idx];

        let (tx, rx) = oneshot::channel();
        let cmd = InFlightCommand { encoded, tx };
        sender.send(cmd).await.map_err(|_| ClientError::PoolClosed)?;

        let result = if let Some(timeout) = self.inner.command_timeout {
            tokio::time::timeout(timeout, rx)
                .await
                .map_err(|_| ClientError::Timeout)?
        } else {
            rx.await
        };

        let value = result.map_err(|_| ClientError::RequestCancelled)??;

        // Convert Redis error responses to ClientError
        if let Value::Error(ref msg) = value {
            return Err(ClientError::Redis(
                String::from_utf8_lossy(msg).into_owned(),
            ));
        }

        Ok(value)
    }

    /// Execute an encoded command not associated with a key (uses first pool).
    async fn execute_unkeyed(&self, encoded: Vec<u8>) -> Result<Value, ClientError> {
        let pool = &self.inner.pools[0];
        let idx = pool.next.fetch_add(1, Ordering::Relaxed) % pool.connections.len();
        let sender = &pool.connections[idx];

        let (tx, rx) = oneshot::channel();
        let cmd = InFlightCommand { encoded, tx };
        sender.send(cmd).await.map_err(|_| ClientError::PoolClosed)?;

        let result = if let Some(timeout) = self.inner.command_timeout {
            tokio::time::timeout(timeout, rx)
                .await
                .map_err(|_| ClientError::Timeout)?
        } else {
            rx.await
        };

        let value = result.map_err(|_| ClientError::RequestCancelled)??;

        if let Value::Error(ref msg) = value {
            return Err(ClientError::Redis(
                String::from_utf8_lossy(msg).into_owned(),
            ));
        }

        Ok(value)
    }

    /// Encode a Request into a Vec<u8>.
    fn encode_request(req: &Request<'_>) -> Vec<u8> {
        let len = req.encoded_len();
        let mut buf = vec![0u8; len];
        req.encode(&mut buf);
        buf
    }

    /// Encode a SetRequest into a Vec<u8>.
    fn encode_set_request(req: &protocol_resp::SetRequest<'_>) -> Vec<u8> {
        let len = req.encoded_len();
        let mut buf = vec![0u8; len];
        req.encode(&mut buf);
        buf
    }

    // ── String commands ─────────────────────────────────────────────────

    /// Get the value of a key.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, ClientError> {
        let encoded = Self::encode_request(&Request::get(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::BulkString(data) => Ok(Some(Bytes::from(data))),
            Value::Null => Ok(None),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Set a key-value pair.
    pub async fn set(&self, key: &[u8], value: impl AsRef<[u8]>) -> Result<(), ClientError> {
        let encoded = Self::encode_set_request(&Request::set(key, value.as_ref()));
        let resp = self.execute(key, encoded).await?;
        match resp {
            Value::SimpleString(_) => Ok(()),
            Value::Null => Ok(()), // SET NX returns Null on failure, but plain SET always OK
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Set a key-value pair with TTL in seconds.
    pub async fn set_ex(
        &self,
        key: &[u8],
        value: impl AsRef<[u8]>,
        ttl_secs: u64,
    ) -> Result<(), ClientError> {
        let encoded = Self::encode_set_request(&Request::set(key, value.as_ref()).ex(ttl_secs));
        let resp = self.execute(key, encoded).await?;
        match resp {
            Value::SimpleString(_) => Ok(()),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Set a key-value pair with TTL in milliseconds.
    pub async fn set_px(
        &self,
        key: &[u8],
        value: impl AsRef<[u8]>,
        ttl_ms: u64,
    ) -> Result<(), ClientError> {
        let encoded = Self::encode_set_request(&Request::set(key, value.as_ref()).px(ttl_ms));
        let resp = self.execute(key, encoded).await?;
        match resp {
            Value::SimpleString(_) => Ok(()),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Set a key only if it does not already exist. Returns true if the key was set.
    pub async fn set_nx(
        &self,
        key: &[u8],
        value: impl AsRef<[u8]>,
    ) -> Result<bool, ClientError> {
        let encoded = Self::encode_set_request(&Request::set(key, value.as_ref()).nx());
        let resp = self.execute(key, encoded).await?;
        match resp {
            Value::SimpleString(_) => Ok(true),
            Value::Null => Ok(false),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Delete a key. Returns the number of keys deleted.
    pub async fn del(&self, key: &[u8]) -> Result<u64, ClientError> {
        let encoded = Self::encode_request(&Request::del(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n as u64),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get values for multiple keys. Keys may route to different servers,
    /// but MGET is sent to the server of the first key.
    pub async fn mget(&self, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>, ClientError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let encoded = Self::encode_request(&Request::mget(keys));
        let value = self.execute(keys[0], encoded).await?;
        match value {
            Value::Array(arr) => {
                let mut result = Vec::with_capacity(arr.len());
                for v in arr {
                    match v {
                        Value::BulkString(data) => result.push(Some(Bytes::from(data))),
                        Value::Null => result.push(None),
                        _ => return Err(ClientError::UnexpectedResponse),
                    }
                }
                Ok(result)
            }
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Increment the integer value of a key by 1.
    pub async fn incr(&self, key: &[u8]) -> Result<i64, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"INCR").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Decrement the integer value of a key by 1.
    pub async fn decr(&self, key: &[u8]) -> Result<i64, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"DECR").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Increment the integer value of a key by a given amount.
    pub async fn incrby(&self, key: &[u8], delta: i64) -> Result<i64, ClientError> {
        let delta_str = delta.to_string();
        let encoded =
            Self::encode_request(&Request::cmd(b"INCRBY").arg(key).arg(delta_str.as_bytes()));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Decrement the integer value of a key by a given amount.
    pub async fn decrby(&self, key: &[u8], delta: i64) -> Result<i64, ClientError> {
        let delta_str = delta.to_string();
        let encoded =
            Self::encode_request(&Request::cmd(b"DECRBY").arg(key).arg(delta_str.as_bytes()));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Append a value to a key. Returns the length of the string after the append.
    pub async fn append(&self, key: &[u8], value: &[u8]) -> Result<i64, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"APPEND").arg(key).arg(value));
        let resp = self.execute(key, encoded).await?;
        match resp {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    // ── Key commands ────────────────────────────────────────────────────

    /// Check if a key exists.
    pub async fn exists(&self, key: &[u8]) -> Result<bool, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"EXISTS").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n > 0),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Set a timeout on a key in seconds.
    pub async fn expire(&self, key: &[u8], seconds: u64) -> Result<bool, ClientError> {
        let secs_str = seconds.to_string();
        let encoded =
            Self::encode_request(&Request::cmd(b"EXPIRE").arg(key).arg(secs_str.as_bytes()));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n == 1),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get the TTL of a key in seconds.
    pub async fn ttl(&self, key: &[u8]) -> Result<i64, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"TTL").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get the TTL of a key in milliseconds.
    pub async fn pttl(&self, key: &[u8]) -> Result<i64, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"PTTL").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Remove the existing timeout on a key.
    pub async fn persist(&self, key: &[u8]) -> Result<bool, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"PERSIST").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n == 1),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get the type of a key.
    pub async fn key_type(&self, key: &[u8]) -> Result<String, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"TYPE").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::SimpleString(data) => Ok(String::from_utf8_lossy(&data).into_owned()),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Rename a key.
    pub async fn rename(&self, key: &[u8], new_key: &[u8]) -> Result<(), ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"RENAME").arg(key).arg(new_key));
        let resp = self.execute(key, encoded).await?;
        match resp {
            Value::SimpleString(_) => Ok(()),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Delete keys without blocking. Returns the number of keys removed.
    pub async fn unlink(&self, key: &[u8]) -> Result<u64, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"UNLINK").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n as u64),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    // ── Hash commands ───────────────────────────────────────────────────

    /// Set a field in a hash. Returns true if the field is new.
    pub async fn hset(
        &self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> Result<bool, ClientError> {
        let encoded =
            Self::encode_request(&Request::cmd(b"HSET").arg(key).arg(field).arg(value));
        let resp = self.execute(key, encoded).await?;
        match resp {
            Value::Integer(n) => Ok(n > 0),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get the value of a hash field.
    pub async fn hget(&self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"HGET").arg(key).arg(field));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::BulkString(data) => Ok(Some(Bytes::from(data))),
            Value::Null => Ok(None),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get all fields and values in a hash.
    pub async fn hgetall(&self, key: &[u8]) -> Result<Vec<(Bytes, Bytes)>, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"HGETALL").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Array(arr) => {
                let mut result = Vec::with_capacity(arr.len() / 2);
                let mut iter = arr.into_iter();
                while let Some(field) = iter.next() {
                    let val = iter.next().ok_or(ClientError::UnexpectedResponse)?;
                    match (field, val) {
                        (Value::BulkString(f), Value::BulkString(v)) => {
                            result.push((Bytes::from(f), Bytes::from(v)));
                        }
                        _ => return Err(ClientError::UnexpectedResponse),
                    }
                }
                Ok(result)
            }
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get values for multiple hash fields.
    pub async fn hmget(
        &self,
        key: &[u8],
        fields: &[&[u8]],
    ) -> Result<Vec<Option<Bytes>>, ClientError> {
        let mut req = Request::cmd(b"HMGET").arg(key);
        for field in fields {
            req = req.arg(field);
        }
        let encoded = Self::encode_request(&req);
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Array(arr) => {
                let mut result = Vec::with_capacity(arr.len());
                for v in arr {
                    match v {
                        Value::BulkString(data) => result.push(Some(Bytes::from(data))),
                        Value::Null => result.push(None),
                        _ => return Err(ClientError::UnexpectedResponse),
                    }
                }
                Ok(result)
            }
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Delete fields from a hash. Returns the number of fields removed.
    pub async fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> Result<i64, ClientError> {
        let mut req = Request::cmd(b"HDEL").arg(key);
        for field in fields {
            req = req.arg(field);
        }
        let encoded = Self::encode_request(&req);
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Check if a field exists in a hash.
    pub async fn hexists(&self, key: &[u8], field: &[u8]) -> Result<bool, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"HEXISTS").arg(key).arg(field));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n == 1),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get the number of fields in a hash.
    pub async fn hlen(&self, key: &[u8]) -> Result<i64, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"HLEN").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get all field names in a hash.
    pub async fn hkeys(&self, key: &[u8]) -> Result<Vec<Bytes>, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"HKEYS").arg(key));
        let value = self.execute(key, encoded).await?;
        Self::parse_bytes_array(value)
    }

    /// Get all values in a hash.
    pub async fn hvals(&self, key: &[u8]) -> Result<Vec<Bytes>, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"HVALS").arg(key));
        let value = self.execute(key, encoded).await?;
        Self::parse_bytes_array(value)
    }

    /// Increment the integer value of a hash field.
    pub async fn hincrby(
        &self,
        key: &[u8],
        field: &[u8],
        delta: i64,
    ) -> Result<i64, ClientError> {
        let delta_str = delta.to_string();
        let encoded = Self::encode_request(
            &Request::cmd(b"HINCRBY")
                .arg(key)
                .arg(field)
                .arg(delta_str.as_bytes()),
        );
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Set a hash field only if it does not exist.
    pub async fn hsetnx(
        &self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> Result<bool, ClientError> {
        let encoded =
            Self::encode_request(&Request::cmd(b"HSETNX").arg(key).arg(field).arg(value));
        let resp = self.execute(key, encoded).await?;
        match resp {
            Value::Integer(n) => Ok(n == 1),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    // ── List commands ───────────────────────────────────────────────────

    /// Push values to the head of a list. Returns the list length.
    pub async fn lpush(&self, key: &[u8], values: &[&[u8]]) -> Result<i64, ClientError> {
        let mut req = Request::cmd(b"LPUSH").arg(key);
        for v in values {
            req = req.arg(v);
        }
        let encoded = Self::encode_request(&req);
        let resp = self.execute(key, encoded).await?;
        match resp {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Push values to the tail of a list. Returns the list length.
    pub async fn rpush(&self, key: &[u8], values: &[&[u8]]) -> Result<i64, ClientError> {
        let mut req = Request::cmd(b"RPUSH").arg(key);
        for v in values {
            req = req.arg(v);
        }
        let encoded = Self::encode_request(&req);
        let resp = self.execute(key, encoded).await?;
        match resp {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Remove and return the first element of a list.
    pub async fn lpop(&self, key: &[u8]) -> Result<Option<Bytes>, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"LPOP").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::BulkString(data) => Ok(Some(Bytes::from(data))),
            Value::Null => Ok(None),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Remove and return the last element of a list.
    pub async fn rpop(&self, key: &[u8]) -> Result<Option<Bytes>, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"RPOP").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::BulkString(data) => Ok(Some(Bytes::from(data))),
            Value::Null => Ok(None),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get the length of a list.
    pub async fn llen(&self, key: &[u8]) -> Result<i64, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"LLEN").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get an element from a list by index.
    pub async fn lindex(&self, key: &[u8], index: i64) -> Result<Option<Bytes>, ClientError> {
        let idx_str = index.to_string();
        let encoded =
            Self::encode_request(&Request::cmd(b"LINDEX").arg(key).arg(idx_str.as_bytes()));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::BulkString(data) => Ok(Some(Bytes::from(data))),
            Value::Null => Ok(None),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get a range of elements from a list.
    pub async fn lrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<Bytes>, ClientError> {
        let start_str = start.to_string();
        let stop_str = stop.to_string();
        let encoded = Self::encode_request(
            &Request::cmd(b"LRANGE")
                .arg(key)
                .arg(start_str.as_bytes())
                .arg(stop_str.as_bytes()),
        );
        let value = self.execute(key, encoded).await?;
        Self::parse_bytes_array(value)
    }

    /// Trim a list to a specified range.
    pub async fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> Result<(), ClientError> {
        let start_str = start.to_string();
        let stop_str = stop.to_string();
        let encoded = Self::encode_request(
            &Request::cmd(b"LTRIM")
                .arg(key)
                .arg(start_str.as_bytes())
                .arg(stop_str.as_bytes()),
        );
        let resp = self.execute(key, encoded).await?;
        match resp {
            Value::SimpleString(_) => Ok(()),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Set the value of an element by index.
    pub async fn lset(&self, key: &[u8], index: i64, value: &[u8]) -> Result<(), ClientError> {
        let idx_str = index.to_string();
        let encoded = Self::encode_request(
            &Request::cmd(b"LSET")
                .arg(key)
                .arg(idx_str.as_bytes())
                .arg(value),
        );
        let resp = self.execute(key, encoded).await?;
        match resp {
            Value::SimpleString(_) => Ok(()),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Push a value to the head of a list only if the list exists. Returns the list length.
    pub async fn lpushx(&self, key: &[u8], value: &[u8]) -> Result<i64, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"LPUSHX").arg(key).arg(value));
        let resp = self.execute(key, encoded).await?;
        match resp {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Push a value to the tail of a list only if the list exists. Returns the list length.
    pub async fn rpushx(&self, key: &[u8], value: &[u8]) -> Result<i64, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"RPUSHX").arg(key).arg(value));
        let resp = self.execute(key, encoded).await?;
        match resp {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    // ── Set commands ────────────────────────────────────────────────────

    /// Add members to a set. Returns the number of members added.
    pub async fn sadd(&self, key: &[u8], members: &[&[u8]]) -> Result<i64, ClientError> {
        let mut req = Request::cmd(b"SADD").arg(key);
        for m in members {
            req = req.arg(m);
        }
        let encoded = Self::encode_request(&req);
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Remove members from a set. Returns the number of members removed.
    pub async fn srem(&self, key: &[u8], members: &[&[u8]]) -> Result<i64, ClientError> {
        let mut req = Request::cmd(b"SREM").arg(key);
        for m in members {
            req = req.arg(m);
        }
        let encoded = Self::encode_request(&req);
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get all members of a set.
    pub async fn smembers(&self, key: &[u8]) -> Result<Vec<Bytes>, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"SMEMBERS").arg(key));
        let value = self.execute(key, encoded).await?;
        Self::parse_bytes_array(value)
    }

    /// Get the number of members in a set.
    pub async fn scard(&self, key: &[u8]) -> Result<i64, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"SCARD").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Check if a member exists in a set.
    pub async fn sismember(&self, key: &[u8], member: &[u8]) -> Result<bool, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"SISMEMBER").arg(key).arg(member));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Integer(n) => Ok(n == 1),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Check if multiple members exist in a set.
    pub async fn smismember(
        &self,
        key: &[u8],
        members: &[&[u8]],
    ) -> Result<Vec<bool>, ClientError> {
        let mut req = Request::cmd(b"SMISMEMBER").arg(key);
        for m in members {
            req = req.arg(m);
        }
        let encoded = Self::encode_request(&req);
        let value = self.execute(key, encoded).await?;
        match value {
            Value::Array(arr) => {
                let mut result = Vec::with_capacity(arr.len());
                for v in arr {
                    match v {
                        Value::Integer(n) => result.push(n == 1),
                        _ => return Err(ClientError::UnexpectedResponse),
                    }
                }
                Ok(result)
            }
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Remove and return a random member from a set.
    pub async fn spop(&self, key: &[u8]) -> Result<Option<Bytes>, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"SPOP").arg(key));
        let value = self.execute(key, encoded).await?;
        match value {
            Value::BulkString(data) => Ok(Some(Bytes::from(data))),
            Value::Null => Ok(None),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get random members from a set.
    pub async fn srandmember(
        &self,
        key: &[u8],
        count: i64,
    ) -> Result<Vec<Bytes>, ClientError> {
        let count_str = count.to_string();
        let encoded = Self::encode_request(
            &Request::cmd(b"SRANDMEMBER")
                .arg(key)
                .arg(count_str.as_bytes()),
        );
        let value = self.execute(key, encoded).await?;
        Self::parse_bytes_array(value)
    }

    // ── Server commands ─────────────────────────────────────────────────

    /// Ping the server.
    pub async fn ping(&self) -> Result<(), ClientError> {
        let encoded = Self::encode_request(&Request::ping());
        let value = self.execute_unkeyed(encoded).await?;
        match value {
            Value::SimpleString(ref s) if s == b"PONG" => Ok(()),
            Value::SimpleString(_) => Ok(()),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Delete all keys in the current database.
    pub async fn flushdb(&self) -> Result<(), ClientError> {
        let encoded = Self::encode_request(&Request::flushdb());
        let value = self.execute_unkeyed(encoded).await?;
        match value {
            Value::SimpleString(_) => Ok(()),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Delete all keys in all databases.
    pub async fn flushall(&self) -> Result<(), ClientError> {
        let encoded = Self::encode_request(&Request::flushall());
        let value = self.execute_unkeyed(encoded).await?;
        match value {
            Value::SimpleString(_) => Ok(()),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get the number of keys in the current database.
    pub async fn dbsize(&self) -> Result<i64, ClientError> {
        let encoded = Self::encode_request(&Request::cmd(b"DBSIZE"));
        let value = self.execute_unkeyed(encoded).await?;
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Get configuration parameter values.
    pub async fn config_get(&self, key: &[u8]) -> Result<Vec<(Bytes, Bytes)>, ClientError> {
        let encoded = Self::encode_request(&Request::config_get(key));
        let value = self.execute_unkeyed(encoded).await?;
        match value {
            Value::Array(arr) => {
                let mut result = Vec::with_capacity(arr.len() / 2);
                let mut iter = arr.into_iter();
                while let Some(k) = iter.next() {
                    let v = iter.next().ok_or(ClientError::UnexpectedResponse)?;
                    match (k, v) {
                        (Value::BulkString(kk), Value::BulkString(vv)) => {
                            result.push((Bytes::from(kk), Bytes::from(vv)));
                        }
                        _ => return Err(ClientError::UnexpectedResponse),
                    }
                }
                Ok(result)
            }
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Set a configuration parameter.
    pub async fn config_set(&self, key: &[u8], value: &[u8]) -> Result<(), ClientError> {
        let encoded = Self::encode_request(&Request::config_set(key, value));
        let resp = self.execute_unkeyed(encoded).await?;
        match resp {
            Value::SimpleString(_) => Ok(()),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    // ── Custom command ──────────────────────────────────────────────────

    /// Execute a custom command. Returns the raw RESP Value.
    pub async fn cmd(&self, key: &[u8], request: &Request<'_>) -> Result<Value, ClientError> {
        let encoded = Self::encode_request(request);
        self.execute(key, encoded).await
    }

    // ── Pipeline ────────────────────────────────────────────────────────

    /// Create a pipeline for batched command execution.
    pub fn pipeline(&self) -> Pipeline {
        Pipeline::new(self.clone())
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn parse_bytes_array(value: Value) -> Result<Vec<Bytes>, ClientError> {
        match value {
            Value::Array(arr) => {
                let mut result = Vec::with_capacity(arr.len());
                for v in arr {
                    match v {
                        Value::BulkString(data) => result.push(Bytes::from(data)),
                        _ => return Err(ClientError::UnexpectedResponse),
                    }
                }
                Ok(result)
            }
            _ => Err(ClientError::UnexpectedResponse),
        }
    }
}
