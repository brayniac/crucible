//! Connection pool for krio-redis.
//!
//! `Pool` manages a fixed set of backend connections with round-robin dispatch
//! and lazy reconnection. It is single-threaded (no Arc, no Mutex) — designed
//! for use within a single krio async worker.
//!
//! # Usage
//!
//! ```no_run
//! # use std::net::SocketAddr;
//! # use krio_redis::{Pool, PoolConfig};
//! # async fn example() -> Result<(), krio_redis::Error> {
//! let config = PoolConfig {
//!     addr: "127.0.0.1:6379".parse().unwrap(),
//!     pool_size: 4,
//!     connect_timeout_ms: 0,
//! };
//! let mut pool = Pool::new(config);
//! pool.connect_all().await?;
//! pool.client().await?.set("k", "v").await?;
//! # Ok(())
//! # }
//! ```

use std::net::SocketAddr;

use krio::ConnCtx;

use crate::{Client, Error, Pipeline};

/// Configuration for a connection pool.
pub struct PoolConfig {
    /// Backend address to connect to.
    pub addr: SocketAddr,
    /// Number of connections in the pool.
    pub pool_size: usize,
    /// Connect timeout in milliseconds. 0 means no timeout.
    pub connect_timeout_ms: u64,
}

enum Slot {
    Connected(ConnCtx),
    Disconnected,
}

/// A fixed-size connection pool with round-robin dispatch.
///
/// All slots start disconnected. Call [`connect_all()`](Pool::connect_all) for
/// eager startup, or let [`client()`](Pool::client) lazily reconnect on demand.
pub struct Pool {
    addr: SocketAddr,
    slots: Vec<Slot>,
    next: usize,
    connect_timeout_ms: u64,
}

impl Pool {
    /// Create a new pool. All slots start disconnected.
    pub fn new(config: PoolConfig) -> Self {
        let mut slots = Vec::with_capacity(config.pool_size);
        for _ in 0..config.pool_size {
            slots.push(Slot::Disconnected);
        }
        Pool {
            addr: config.addr,
            slots,
            next: 0,
            connect_timeout_ms: config.connect_timeout_ms,
        }
    }

    /// Eagerly connect all slots. Returns an error if any connection fails.
    pub async fn connect_all(&mut self) -> Result<(), Error> {
        for i in 0..self.slots.len() {
            let conn = self.do_connect().await?;
            self.slots[i] = Slot::Connected(conn);
        }
        Ok(())
    }

    /// Get a [`Client`] bound to the next healthy connection.
    ///
    /// Advances the round-robin cursor and returns a client for a connected
    /// slot. Disconnected slots are lazily reconnected. If all slots fail,
    /// returns [`Error::AllConnectionsFailed`].
    pub async fn client(&mut self) -> Result<Client, Error> {
        let size = self.slots.len();
        for _ in 0..size {
            let idx = self.next;
            self.next = (self.next + 1) % size;

            match &self.slots[idx] {
                Slot::Connected(conn) => return Ok(Client::new(*conn)),
                Slot::Disconnected => {
                    if let Ok(conn) = self.do_connect().await {
                        self.slots[idx] = Slot::Connected(conn);
                        return Ok(Client::new(conn));
                    }
                }
            }
        }
        Err(Error::AllConnectionsFailed)
    }

    /// Get a [`Pipeline`] on the next healthy connection.
    pub async fn pipeline(&mut self) -> Result<Pipeline, Error> {
        let client = self.client().await?;
        Ok(client.pipeline())
    }

    /// Mark a connection as dead after a `ConnectionClosed` error.
    ///
    /// Matches by [`ConnCtx::token()`] and sets the slot to disconnected.
    /// The next [`client()`](Pool::client) call will lazily reconnect.
    pub fn mark_disconnected(&mut self, client: Client) {
        let token = client.conn().token();
        for slot in &mut self.slots {
            if let Slot::Connected(conn) = slot
                && conn.token() == token
            {
                conn.close();
                *slot = Slot::Disconnected;
                return;
            }
        }
    }

    /// Close all connections and reset slots to disconnected.
    pub fn close_all(&mut self) {
        for slot in &mut self.slots {
            if let Slot::Connected(conn) = slot {
                conn.close();
            }
            *slot = Slot::Disconnected;
        }
    }

    /// Number of currently connected slots.
    pub fn connected_count(&self) -> usize {
        self.slots
            .iter()
            .filter(|s| matches!(s, Slot::Connected(_)))
            .count()
    }

    /// Total number of slots in the pool.
    pub fn pool_size(&self) -> usize {
        self.slots.len()
    }

    async fn do_connect(&self) -> Result<ConnCtx, Error> {
        let fut = if self.connect_timeout_ms > 0 {
            krio::connect_with_timeout(self.addr, self.connect_timeout_ms)?
        } else {
            krio::connect(self.addr)?
        };
        Ok(fut.await?)
    }
}
