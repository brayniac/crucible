//! Async proxy worker using ringline's AsyncEventHandler.
//!
//! Each client connection gets a dedicated async task that:
//! 1. Connects to backend(s) on demand via `ringline_redis::Client`
//! 2. Reads commands from the client
//! 3. Checks the local cache for GET hits
//! 4. Forwards cache misses to the typed backend client
//! 5. Caches backend responses and sends them back to the client

use crate::cache::SharedCache;
use crate::config::Config;

use bytes::{Bytes, BytesMut};
use resp_proto::{Command, ParseError, Value};
use ringline::{AsyncEventHandler, ConnCtx, DriverCtx, RinglineBuilder};
use ringline_redis::Client;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, info, warn};

// ── Config channel ──────────────────────────────────────────────────────

/// Per-worker configuration passed through the config channel.
pub struct AsyncProxyWorkerConfig {
    pub worker_id: usize,
    pub config: Config,
    pub cache: Arc<SharedCache>,
    pub shutdown: Arc<AtomicBool>,
    pub cpu_id: Option<usize>,
}

static ASYNC_CONFIG_CHANNEL: Mutex<Option<crossbeam_channel::Receiver<AsyncProxyWorkerConfig>>> =
    Mutex::new(None);

fn init_config_channel(rx: crossbeam_channel::Receiver<AsyncProxyWorkerConfig>) {
    let mut guard = ASYNC_CONFIG_CHANNEL.lock().unwrap();
    *guard = Some(rx);
}

fn recv_config() -> AsyncProxyWorkerConfig {
    let guard = ASYNC_CONFIG_CHANNEL.lock().unwrap();
    let rx = guard.as_ref().expect("config channel not initialized");
    rx.recv().expect("config channel closed")
}

// ── AsyncProxyHandler ───────────────────────────────────────────────────

/// Async proxy handler implementing ringline's AsyncEventHandler.
pub struct AsyncProxyHandler {
    #[allow(dead_code)]
    worker_id: usize,
    shutdown: Arc<AtomicBool>,
    cache: Arc<SharedCache>,
    backends: Vec<SocketAddr>,
    ring: ketama::Ring,
    password: Option<String>,
    username: Option<String>,
    tls_server_name: Option<String>,
    connect_timeout_ms: u64,
}

impl AsyncEventHandler for AsyncProxyHandler {
    fn on_accept(&self, client: ConnCtx) -> impl Future<Output = ()> + 'static {
        let cache = Arc::clone(&self.cache);
        let backends = self.backends.clone();
        let ring = self.ring.clone();
        let password = self.password.clone();
        let username = self.username.clone();
        let tls_server_name = self.tls_server_name.clone();
        let connect_timeout_ms = self.connect_timeout_ms;
        Box::pin(handle_client(
            client,
            ring,
            backends,
            cache,
            password,
            username,
            tls_server_name,
            connect_timeout_ms,
        ))
    }

    fn on_tick(&mut self, ctx: &mut DriverCtx<'_>) {
        if self.shutdown.load(Ordering::Relaxed) {
            ctx.request_shutdown();
        }
    }

    fn create_for_worker(worker_id: usize) -> Self {
        let cfg = recv_config();

        // Pin to CPU if configured.
        if let Some(cpu) = cfg.cpu_id {
            set_cpu_affinity(cpu);
        }

        ::metrics::set_thread_shard(worker_id);

        let backends = cfg.config.backend.nodes.clone();
        assert!(!backends.is_empty(), "at least one backend node required");

        // Build ketama consistent hash ring from backend addresses.
        let server_ids: Vec<String> = backends.iter().map(|a| a.to_string()).collect();
        let ring = ketama::Ring::build(&server_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>());

        info!(
            worker_id = cfg.worker_id,
            backends = ?backends,
            "Async worker starting"
        );

        AsyncProxyHandler {
            worker_id: cfg.worker_id,
            shutdown: cfg.shutdown,
            cache: cfg.cache,
            backends,
            ring,
            password: cfg.config.backend.password.clone(),
            username: cfg.config.backend.username.clone(),
            tls_server_name: cfg.config.backend.tls_server_name.clone(),
            connect_timeout_ms: cfg.config.backend.connect_timeout_ms,
        }
    }
}

// ── Client handler ──────────────────────────────────────────────────────

/// Handle a single client connection: route commands to backends via
/// ketama consistent hashing, establishing backend connections lazily.
#[allow(clippy::too_many_arguments)]
async fn handle_client(
    client: ConnCtx,
    ring: ketama::Ring,
    backends: Vec<SocketAddr>,
    cache: Arc<SharedCache>,
    password: Option<String>,
    username: Option<String>,
    tls_server_name: Option<String>,
    connect_timeout_ms: u64,
) {
    let mut conns: HashMap<usize, Client> = HashMap::new();
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        // Wait for client data.
        let got = client
            .with_data(|data| {
                if data.is_empty() {
                    return ringline::ParseResult::Consumed(0);
                }
                buf.extend_from_slice(data);
                ringline::ParseResult::Consumed(data.len())
            })
            .await;

        if got == 0 {
            // Client disconnected.
            for (_, conn) in conns.drain() {
                conn.conn().close();
            }
            break;
        }

        // Parse and process all complete commands in the buffer.
        loop {
            if buf.is_empty() {
                break;
            }

            match Command::parse(&buf) {
                Ok((cmd, consumed)) => {
                    // Route the command to the appropriate backend.
                    let endpoint_idx = route_command(&cmd, &ring, backends.len());
                    let backend = match get_or_connect(
                        &client,
                        &mut conns,
                        &backends,
                        endpoint_idx,
                        password.as_deref(),
                        username.as_deref(),
                        tls_server_name.as_deref(),
                        connect_timeout_ms,
                    )
                    .await
                    {
                        Some(conn) => conn,
                        None => {
                            let _ = client.send(b"-ERR backend unavailable\r\n");
                            for (_, conn) in conns.drain() {
                                conn.conn().close();
                            }
                            return;
                        }
                    };

                    match process_command(&cmd, &client, backend, &cache).await {
                        CommandResult::Responded => {}
                        CommandResult::ForwardedAndResponded => {}
                        CommandResult::BackendDisconnected => {
                            // Remove the failed backend; next use will reconnect.
                            if let Some(conn) = conns.remove(&endpoint_idx) {
                                conn.conn().close();
                            }
                            let _ = client.send(b"-ERR backend disconnected\r\n");
                        }
                    }
                    let _ = buf.split_to(consumed);
                }
                Err(ParseError::Incomplete) => break,
                Err(e) => {
                    warn!(error = ?e, "Client parse error");
                    let _ = client.send(b"-ERR protocol error\r\n");
                    for (_, conn) in conns.drain() {
                        conn.conn().close();
                    }
                    return;
                }
            }
        }
    }
}

/// Determine the backend endpoint index for a command using the ketama ring.
fn route_command(cmd: &Command<'_>, ring: &ketama::Ring, num_backends: usize) -> usize {
    if num_backends == 1 {
        return 0;
    }
    match cmd {
        // String commands with a key
        Command::Get { key }
        | Command::Set { key, .. }
        | Command::Del { key }
        | Command::Incr { key }
        | Command::Decr { key }
        | Command::IncrBy { key, .. }
        | Command::DecrBy { key, .. }
        | Command::Append { key, .. } => ring.route(key),
        // Hash commands
        Command::HSet { key, .. }
        | Command::HGet { key, .. }
        | Command::HMGet { key, .. }
        | Command::HGetAll { key }
        | Command::HDel { key, .. }
        | Command::HExists { key, .. }
        | Command::HLen { key }
        | Command::HKeys { key }
        | Command::HVals { key }
        | Command::HSetNx { key, .. }
        | Command::HIncrBy { key, .. } => ring.route(key),
        // List commands
        Command::LPush { key, .. }
        | Command::RPush { key, .. }
        | Command::LPop { key, .. }
        | Command::RPop { key, .. }
        | Command::LRange { key, .. }
        | Command::LLen { key }
        | Command::LIndex { key, .. }
        | Command::LSet { key, .. }
        | Command::LTrim { key, .. }
        | Command::LPushX { key, .. }
        | Command::RPushX { key, .. } => ring.route(key),
        // Set commands
        Command::SAdd { key, .. }
        | Command::SRem { key, .. }
        | Command::SMembers { key }
        | Command::SIsMember { key, .. }
        | Command::SMisMember { key, .. }
        | Command::SCard { key }
        | Command::SPop { key, .. }
        | Command::SRandMember { key, .. } => ring.route(key),
        // Key commands
        Command::Type { key } => ring.route(key),
        // MGet: route by first key (may span shards, but simple approach)
        Command::MGet { keys } => {
            if let Some(first) = keys.first() {
                ring.route(first)
            } else {
                0
            }
        }
        // Non-keyed commands go to endpoint 0.
        _ => 0,
    }
}

/// Get an existing backend connection or establish a new one lazily.
#[allow(clippy::too_many_arguments)]
async fn get_or_connect<'a>(
    client: &ConnCtx,
    conns: &'a mut HashMap<usize, Client>,
    backends: &[SocketAddr],
    idx: usize,
    password: Option<&str>,
    username: Option<&str>,
    tls_server_name: Option<&str>,
    connect_timeout_ms: u64,
) -> Option<&'a mut Client> {
    if let std::collections::hash_map::Entry::Vacant(e) = conns.entry(idx) {
        let addr = backends[idx];

        // Submit the connect request (with optional TLS and timeout).
        let connect_fut = match (tls_server_name, connect_timeout_ms > 0) {
            (Some(sni), true) => client.connect_tls_with_timeout(addr, sni, connect_timeout_ms),
            (Some(sni), false) => client.connect_tls(addr, sni),
            (None, true) => client.connect_with_timeout(addr, connect_timeout_ms),
            (None, false) => client.connect(addr),
        };

        let connect_fut = match connect_fut {
            Ok(fut) => fut,
            Err(e) => {
                warn!(error = %e, backend_addr = %addr, "Backend connect submission failed");
                return None;
            }
        };

        let ctx: ConnCtx = match connect_fut.await {
            Ok(ctx) => {
                debug!(
                    client_index = client.index(),
                    backend_index = ctx.index(),
                    backend_addr = %addr,
                    "Backend connected"
                );
                ctx
            }
            Err(e) => {
                warn!(error = %e, backend_addr = %addr, "Backend connect failed");
                return None;
            }
        };

        let mut redis_client = Client::new(ctx);

        // Authenticate if credentials are configured.
        if let Some(pw) = password {
            let auth_result = match username {
                Some(user) => redis_client.auth_username(user, pw).await,
                None => redis_client.auth(pw).await,
            };
            if let Err(e) = auth_result {
                warn!(error = %e, backend_addr = %addr, "Backend auth failed");
                redis_client.conn().close();
                return None;
            }
        }

        e.insert(redis_client);
    }
    conns.get_mut(&idx)
}

/// Result of processing a single command.
enum CommandResult {
    /// Response was sent directly (PING, cache hit).
    Responded,
    /// Command was forwarded to backend and response sent to client.
    ForwardedAndResponded,
    /// Backend connection was lost.
    BackendDisconnected,
}

/// Process a single parsed command.
async fn process_command(
    cmd: &Command<'_>,
    client: &ConnCtx,
    backend: &mut Client,
    cache: &SharedCache,
) -> CommandResult {
    match cmd {
        // ── PING: respond directly ──────────────────────────────────
        Command::Ping => {
            let _ = client.send(Value::PONG);
            CommandResult::Responded
        }

        // ── GET: check cache first, then backend ────────────────────
        Command::Get { key } => {
            // Cache hit: encode the raw value as a RESP bulk string.
            if let Some(data) = cache.with_value(key, Bytes::copy_from_slice) {
                send_bulk_string(client, &data);
                return CommandResult::Responded;
            }

            match backend.get(*key).await {
                Ok(Some(value)) => {
                    // Cache the raw value bytes.
                    cache.set(key, &value);
                    send_bulk_string(client, &value);
                    CommandResult::ForwardedAndResponded
                }
                Ok(None) => {
                    let _ = client.send(Value::NULL_BULK);
                    CommandResult::ForwardedAndResponded
                }
                Err(ringline_redis::Error::ConnectionClosed) => CommandResult::BackendDisconnected,
                Err(e) => {
                    send_error(client, &e.to_string());
                    CommandResult::ForwardedAndResponded
                }
            }
        }

        // ── SET: forward to backend, invalidate cache ───────────────
        Command::Set {
            key,
            value,
            ex,
            px,
            nx,
            xx,
        } => {
            cache.delete(key);

            let result = if *nx {
                backend.set_nx(*key, *value).await.map(|was_set| {
                    if was_set {
                        Value::SimpleString(Bytes::from_static(b"OK"))
                    } else {
                        Value::Null
                    }
                })
            } else if let Some(secs) = ex {
                // SET with XX + EX is not directly supported by typed methods;
                // for simplicity we ignore XX here since it's rare for proxies.
                if *xx {
                    forward_raw_value(backend, cmd).await
                } else {
                    backend
                        .set_ex(*key, *value, *secs)
                        .await
                        .map(|()| Value::SimpleString(Bytes::from_static(b"OK")))
                }
            } else if let Some(ms) = px {
                if *xx {
                    forward_raw_value(backend, cmd).await
                } else {
                    backend
                        .set_px(*key, *value, *ms)
                        .await
                        .map(|()| Value::SimpleString(Bytes::from_static(b"OK")))
                }
            } else if *xx {
                forward_raw_value(backend, cmd).await
            } else {
                backend
                    .set(*key, *value)
                    .await
                    .map(|()| Value::SimpleString(Bytes::from_static(b"OK")))
            };

            send_result(client, result)
        }

        // ── DEL: forward to backend, invalidate cache ───────────────
        Command::Del { key } => {
            cache.delete(key);
            let result = backend.del(*key).await.map(|n| Value::Integer(n as i64));
            send_result(client, result)
        }

        // ── MGET: forward to backend ────────────────────────────────
        Command::MGet { keys } => {
            let key_slices: Vec<&[u8]> = keys.to_vec();
            let result = backend.mget(&key_slices).await.map(|values| {
                let arr: Vec<Value> = values
                    .into_iter()
                    .map(|v| match v {
                        Some(data) => Value::BulkString(data),
                        None => Value::Null,
                    })
                    .collect();
                Value::Array(arr)
            });
            send_result(client, result)
        }

        // ── INCR/DECR/INCRBY/DECRBY ────────────────────────────────
        Command::Incr { key } => {
            cache.delete(key);
            let result = backend.incr(*key).await.map(Value::Integer);
            send_result(client, result)
        }
        Command::Decr { key } => {
            cache.delete(key);
            let result = backend.decr(*key).await.map(Value::Integer);
            send_result(client, result)
        }
        Command::IncrBy { key, delta } => {
            cache.delete(key);
            let result = backend.incrby(*key, *delta).await.map(Value::Integer);
            send_result(client, result)
        }
        Command::DecrBy { key, delta } => {
            cache.delete(key);
            let result = backend.decrby(*key, *delta).await.map(Value::Integer);
            send_result(client, result)
        }
        Command::Append { key, value } => {
            cache.delete(key);
            let result = backend.append(*key, *value).await.map(Value::Integer);
            send_result(client, result)
        }

        // ── Hash commands ───────────────────────────────────────────
        Command::HGet { key, field } => {
            let result = backend.hget(*key, *field).await.map(|v| match v {
                Some(data) => Value::BulkString(data),
                None => Value::Null,
            });
            send_result(client, result)
        }
        Command::HSet { key, fields } => {
            cache.delete(key);
            // The typed client only supports single field hset, so use raw cmd
            // for multi-field HSET to send it in one round trip.
            if fields.len() == 1 {
                let (field, value) = fields[0];
                let result = backend
                    .hset(*key, field, value)
                    .await
                    .map(|is_new| Value::Integer(if is_new { 1 } else { 0 }));
                send_result(client, result)
            } else {
                let result = forward_raw_value(backend, cmd).await;
                send_result(client, result)
            }
        }
        Command::HGetAll { key } => {
            let result = backend.hgetall(*key).await.map(|pairs| {
                let mut arr = Vec::with_capacity(pairs.len() * 2);
                for (f, v) in pairs {
                    arr.push(Value::BulkString(f));
                    arr.push(Value::BulkString(v));
                }
                Value::Array(arr)
            });
            send_result(client, result)
        }
        Command::HMGet { key, fields } => {
            let field_slices: Vec<&[u8]> = fields.to_vec();
            let result = backend.hmget(*key, &field_slices).await.map(|values| {
                let arr: Vec<Value> = values
                    .into_iter()
                    .map(|v| match v {
                        Some(data) => Value::BulkString(data),
                        None => Value::Null,
                    })
                    .collect();
                Value::Array(arr)
            });
            send_result(client, result)
        }
        Command::HDel { key, fields } => {
            cache.delete(key);
            let field_slices: Vec<&[u8]> = fields.to_vec();
            let result = backend.hdel(*key, &field_slices).await.map(Value::Integer);
            send_result(client, result)
        }
        Command::HExists { key, field } => {
            let result = backend
                .hexists(*key, *field)
                .await
                .map(|exists| Value::Integer(if exists { 1 } else { 0 }));
            send_result(client, result)
        }
        Command::HLen { key } => {
            let result = backend.hlen(*key).await.map(Value::Integer);
            send_result(client, result)
        }
        Command::HKeys { key } => {
            let result = backend
                .hkeys(*key)
                .await
                .map(|keys| Value::Array(keys.into_iter().map(Value::BulkString).collect()));
            send_result(client, result)
        }
        Command::HVals { key } => {
            let result = backend
                .hvals(*key)
                .await
                .map(|vals| Value::Array(vals.into_iter().map(Value::BulkString).collect()));
            send_result(client, result)
        }
        Command::HSetNx { key, field, value } => {
            cache.delete(key);
            let result = backend
                .hsetnx(*key, *field, *value)
                .await
                .map(|is_new| Value::Integer(if is_new { 1 } else { 0 }));
            send_result(client, result)
        }
        Command::HIncrBy { key, field, delta } => {
            cache.delete(key);
            let result = backend
                .hincrby(*key, *field, *delta)
                .await
                .map(Value::Integer);
            send_result(client, result)
        }

        // ── List commands ───────────────────────────────────────────
        Command::LPush { key, values } => {
            cache.delete(key);
            let val_slices: Vec<&[u8]> = values.to_vec();
            let result = backend.lpush(*key, &val_slices).await.map(Value::Integer);
            send_result(client, result)
        }
        Command::RPush { key, values } => {
            cache.delete(key);
            let val_slices: Vec<&[u8]> = values.to_vec();
            let result = backend.rpush(*key, &val_slices).await.map(Value::Integer);
            send_result(client, result)
        }
        Command::LPop { key, .. } => {
            cache.delete(key);
            let result = backend.lpop(*key).await.map(|v| match v {
                Some(data) => Value::BulkString(data),
                None => Value::Null,
            });
            send_result(client, result)
        }
        Command::RPop { key, .. } => {
            cache.delete(key);
            let result = backend.rpop(*key).await.map(|v| match v {
                Some(data) => Value::BulkString(data),
                None => Value::Null,
            });
            send_result(client, result)
        }
        Command::LLen { key } => {
            let result = backend.llen(*key).await.map(Value::Integer);
            send_result(client, result)
        }
        Command::LIndex { key, index } => {
            let result = backend.lindex(*key, *index).await.map(|v| match v {
                Some(data) => Value::BulkString(data),
                None => Value::Null,
            });
            send_result(client, result)
        }
        Command::LRange { key, start, stop } => {
            let result = backend
                .lrange(*key, *start, *stop)
                .await
                .map(|items| Value::Array(items.into_iter().map(Value::BulkString).collect()));
            send_result(client, result)
        }
        Command::LTrim { key, start, stop } => {
            cache.delete(key);
            let result = backend
                .ltrim(*key, *start, *stop)
                .await
                .map(|()| Value::SimpleString(Bytes::from_static(b"OK")));
            send_result(client, result)
        }
        Command::LSet { key, index, value } => {
            cache.delete(key);
            let result = backend
                .lset(*key, *index, *value)
                .await
                .map(|()| Value::SimpleString(Bytes::from_static(b"OK")));
            send_result(client, result)
        }
        Command::LPushX { key, values } => {
            cache.delete(key);
            // Client only supports single value lpushx; for multi, use first.
            if let Some(first) = values.first() {
                let result = backend.lpushx(*key, *first).await.map(Value::Integer);
                send_result(client, result)
            } else {
                let _ = client.send(b":0\r\n");
                CommandResult::Responded
            }
        }
        Command::RPushX { key, values } => {
            cache.delete(key);
            if let Some(first) = values.first() {
                let result = backend.rpushx(*key, *first).await.map(Value::Integer);
                send_result(client, result)
            } else {
                let _ = client.send(b":0\r\n");
                CommandResult::Responded
            }
        }

        // ── Set commands ────────────────────────────────────────────
        Command::SAdd { key, members } => {
            cache.delete(key);
            let mem_slices: Vec<&[u8]> = members.to_vec();
            let result = backend.sadd(*key, &mem_slices).await.map(Value::Integer);
            send_result(client, result)
        }
        Command::SRem { key, members } => {
            cache.delete(key);
            let mem_slices: Vec<&[u8]> = members.to_vec();
            let result = backend.srem(*key, &mem_slices).await.map(Value::Integer);
            send_result(client, result)
        }
        Command::SMembers { key } => {
            let result = backend
                .smembers(*key)
                .await
                .map(|members| Value::Array(members.into_iter().map(Value::BulkString).collect()));
            send_result(client, result)
        }
        Command::SCard { key } => {
            let result = backend.scard(*key).await.map(Value::Integer);
            send_result(client, result)
        }
        Command::SIsMember { key, member } => {
            let result = backend
                .sismember(*key, *member)
                .await
                .map(|exists| Value::Integer(if exists { 1 } else { 0 }));
            send_result(client, result)
        }
        Command::SMisMember { key, members } => {
            let mem_slices: Vec<&[u8]> = members.to_vec();
            let result = backend.smismember(*key, &mem_slices).await.map(|bools| {
                Value::Array(
                    bools
                        .into_iter()
                        .map(|b| Value::Integer(if b { 1 } else { 0 }))
                        .collect(),
                )
            });
            send_result(client, result)
        }
        Command::SPop { key, .. } => {
            cache.delete(key);
            let result = backend.spop(*key).await.map(|v| match v {
                Some(data) => Value::BulkString(data),
                None => Value::Null,
            });
            send_result(client, result)
        }
        Command::SRandMember { key, count } => {
            let count = count.unwrap_or(1);
            let result = backend
                .srandmember(*key, count)
                .await
                .map(|members| Value::Array(members.into_iter().map(Value::BulkString).collect()));
            send_result(client, result)
        }

        // ── Key commands ────────────────────────────────────────────
        Command::Type { key } => {
            let result = backend
                .key_type(*key)
                .await
                .map(|t| Value::SimpleString(Bytes::from(t)));
            send_result(client, result)
        }

        // ── Server commands ─────────────────────────────────────────
        Command::FlushDb => {
            let result = backend
                .flushdb()
                .await
                .map(|()| Value::SimpleString(Bytes::from_static(b"OK")));
            send_result(client, result)
        }
        Command::FlushAll => {
            let result = backend
                .flushall()
                .await
                .map(|()| Value::SimpleString(Bytes::from_static(b"OK")));
            send_result(client, result)
        }

        // ── Config ──────────────────────────────────────────────────
        Command::Config { .. } | Command::Cluster { .. } => {
            // Forward raw for complex admin commands.
            let result = forward_raw_value(backend, cmd).await;
            send_result(client, result)
        }

        // ── Unsupported commands ────────────────────────────────────
        _ => {
            send_error(client, "unsupported command");
            CommandResult::Responded
        }
    }
}

// ── Response helpers ────────────────────────────────────────────────────

/// Send a RESP bulk string to the client connection.
fn send_bulk_string(client: &ConnCtx, data: &[u8]) {
    let mut buf = BytesMut::with_capacity(data.len() + 16);
    buf.extend_from_slice(b"$");
    write_usize(&mut buf, data.len());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(data);
    buf.extend_from_slice(b"\r\n");
    let _ = client.send(&buf);
}

/// Encode a `resp_proto::Value` into RESP wire format and send to the client.
fn send_value(client: &ConnCtx, value: &Value) {
    let len = value.encoded_len();
    let mut buf = vec![0u8; len];
    value.encode(&mut buf);
    let _ = client.send(&buf);
}

/// Send an error response to the client.
fn send_error(client: &ConnCtx, msg: &str) {
    let mut buf = BytesMut::with_capacity(msg.len() + 8);
    buf.extend_from_slice(b"-ERR ");
    buf.extend_from_slice(msg.as_bytes());
    buf.extend_from_slice(b"\r\n");
    let _ = client.send(&buf);
}

/// Convert a typed client result to a wire response and send to the client.
fn send_result(client: &ConnCtx, result: Result<Value, ringline_redis::Error>) -> CommandResult {
    match result {
        Ok(value) => {
            send_value(client, &value);
            CommandResult::ForwardedAndResponded
        }
        Err(ringline_redis::Error::ConnectionClosed) => CommandResult::BackendDisconnected,
        Err(e) => {
            send_error(client, &e.to_string());
            CommandResult::ForwardedAndResponded
        }
    }
}

/// Forward a command to the backend using raw `cmd()` and return the raw Value.
/// Used for commands with complex flags not covered by typed methods.
async fn forward_raw_value(
    backend: &mut Client,
    cmd: &Command<'_>,
) -> Result<Value, ringline_redis::Error> {
    // SET with EX/PX needs special handling: the numeric values are formatted
    // into stack buffers here so they live long enough for the request.
    if let Command::Set {
        key,
        value,
        ex,
        px,
        nx,
        xx,
    } = cmd
    {
        let ex_str = ex.map(|s| s.to_string());
        let px_str = px.map(|s| s.to_string());
        let mut req = resp_proto::Request::cmd(b"SET").arg(key).arg(value);
        if let Some(ref s) = ex_str {
            req = req.arg(b"EX" as &[u8]).arg(s.as_bytes());
        }
        if let Some(ref s) = px_str {
            req = req.arg(b"PX" as &[u8]).arg(s.as_bytes());
        }
        if *nx {
            req = req.arg(b"NX" as &[u8]);
        }
        if *xx {
            req = req.arg(b"XX" as &[u8]);
        }
        return backend.cmd(&req).await;
    }

    let req = build_request(cmd);
    backend.cmd(&req).await
}

/// Build a `resp_proto::Request` from a parsed `Command`.
fn build_request<'a>(cmd: &Command<'a>) -> resp_proto::Request<'a> {
    match cmd {
        Command::Get { key } => resp_proto::Request::get(key),
        Command::Set { .. } => unreachable!("SET is handled directly in forward_raw_value"),
        Command::Del { key } => resp_proto::Request::del(key),
        Command::Config { subcommand, args } => {
            let mut req = resp_proto::Request::cmd(b"CONFIG").arg(subcommand);
            for arg in args {
                req = req.arg(arg);
            }
            req
        }
        Command::Cluster { subcommand, args } => {
            let mut req = resp_proto::Request::cmd(b"CLUSTER").arg(subcommand);
            for arg in args {
                req = req.arg(arg);
            }
            req
        }
        // Multi-field HSET
        Command::HSet { key, fields } => {
            let mut req = resp_proto::Request::cmd(b"HSET").arg(key);
            for (field, value) in fields {
                req = req.arg(field).arg(value);
            }
            req
        }
        _ => resp_proto::Request::cmd(b"PING"),
    }
}

#[inline]
fn write_usize(buf: &mut BytesMut, mut n: usize) {
    if n == 0 {
        buf.extend_from_slice(b"0");
        return;
    }
    let mut digits = [0u8; 20];
    let mut i = 20;
    while n > 0 {
        i -= 1;
        digits[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    buf.extend_from_slice(&digits[i..]);
}

// ── Public API ──────────────────────────────────────────────────────────

/// Run the proxy in async mode with the given configuration.
pub fn run_async(
    config: &Config,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_workers = config.threads();
    let cpu_affinity = config.cpu_affinity();
    let listen_addr = config.proxy.listen;

    info!(
        workers = num_workers,
        listen = %listen_addr,
        backend = ?config.backend.nodes,
        mode = "async",
        "Starting async proxy"
    );

    // Create shared cache.
    let cache = Arc::new(SharedCache::new(&config.cache));
    if cache.is_enabled() {
        info!(
            heap_size = config.cache.heap_size,
            segment_size = config.cache.segment_size,
            ttl_ms = config.cache.ttl_ms,
            eviction = ?config.cache.eviction,
            "Shared cache enabled"
        );
    }

    // Set up config channel.
    let (config_tx, config_rx) = crossbeam_channel::bounded::<AsyncProxyWorkerConfig>(num_workers);
    for worker_id in 0..num_workers {
        let cpu_id = cpu_affinity
            .as_ref()
            .map(|cpus| cpus[worker_id % cpus.len()]);

        config_tx
            .send(AsyncProxyWorkerConfig {
                worker_id,
                config: config.clone(),
                cache: Arc::clone(&cache),
                shutdown: Arc::clone(&shutdown),
                cpu_id,
            })
            .expect("failed to queue worker config");
    }
    init_config_channel(config_rx);

    // Build ringline config.
    let krio_config = ringline::Config {
        recv_buffer: ringline::RecvBufferConfig {
            ring_size: config.uring.buffer_count.next_power_of_two(),
            buffer_size: config.uring.buffer_size as u32,
            ..Default::default()
        },
        worker: ringline::WorkerConfig {
            threads: num_workers,
            pin_to_core: false, // We pin in create_for_worker.
            core_offset: 0,
        },
        sq_entries: config.uring.sq_depth,
        tcp_nodelay: true,
        ..Default::default()
    };

    // Launch ringline with async event handler.
    let (shutdown_handle, handles) = RinglineBuilder::new(krio_config)
        .bind(listen_addr)
        .launch::<AsyncProxyHandler>()?;

    // Wait for shutdown signal.
    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }

    info!("Shutdown signal received, stopping async workers...");

    shutdown_handle.shutdown();

    let drain_timeout = Duration::from_secs(config.shutdown.drain_timeout_secs);
    let drain_start = std::time::Instant::now();
    for handle in handles {
        let remaining = drain_timeout.saturating_sub(drain_start.elapsed());
        if remaining.is_zero() {
            warn!("Drain timeout reached");
            break;
        }
        let _ = handle.join();
    }

    info!("Async proxy shutdown complete");
    Ok(())
}

// ── CPU affinity ────────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn set_cpu_affinity(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

#[cfg(not(target_os = "linux"))]
fn set_cpu_affinity(_cpu: usize) {}
