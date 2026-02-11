//! Backend connection pool.

use super::connection::BackendConnection;
use ahash::AHashMap;
use kompio::ConnToken;
use std::net::SocketAddr;

/// A pool of connections to backend nodes.
pub struct BackendPool {
    /// Backend address (single node).
    backend_addr: SocketAddr,

    /// Connections indexed by ConnToken::index().
    connections: AHashMap<usize, BackendConnection>,

    /// Target pool size.
    pool_size: usize,
}

impl BackendPool {
    /// Create a new backend pool for a single node.
    pub fn new(backend_addr: SocketAddr, pool_size: usize) -> Self {
        Self {
            backend_addr,
            connections: AHashMap::new(),
            pool_size,
        }
    }

    /// Get the backend address.
    pub fn backend_addr(&self) -> SocketAddr {
        self.backend_addr
    }

    /// Get current number of connections.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Check if we need more connections.
    pub fn needs_connections(&self) -> bool {
        self.connections.len() < self.pool_size
    }

    /// Add a connection (in connecting state).
    pub fn add_connection(&mut self, conn: BackendConnection) {
        self.connections.insert(conn.conn.index(), conn);
    }

    /// Get a connection by token.
    pub fn get_connection(&self, conn: ConnToken) -> Option<&BackendConnection> {
        let bc = self.connections.get(&conn.index())?;
        // Verify generation matches
        if bc.conn == conn { Some(bc) } else { None }
    }

    /// Get a mutable connection by token.
    pub fn get_connection_mut(&mut self, conn: ConnToken) -> Option<&mut BackendConnection> {
        let bc = self.connections.get_mut(&conn.index())?;
        if bc.conn == conn { Some(bc) } else { None }
    }

    /// Remove a connection.
    pub fn remove_connection(&mut self, conn: ConnToken) -> Option<BackendConnection> {
        if self.connections.get(&conn.index()).is_some_and(|bc| bc.conn == conn) {
            return self.connections.remove(&conn.index());
        }
        None
    }

    /// Get a usable connection with least in-flight requests (load balancing).
    pub fn get_usable_connection(&mut self) -> Option<&mut BackendConnection> {
        let mut best_index = None;
        let mut best_in_flight = usize::MAX;

        for (index, conn) in &self.connections {
            if !conn.is_usable() {
                continue;
            }
            let in_flight = conn.in_flight.len();
            if in_flight < best_in_flight {
                best_in_flight = in_flight;
                best_index = Some(*index);
                if in_flight == 0 {
                    break;
                }
            }
        }

        best_index.and_then(|idx| self.connections.get_mut(&idx))
    }

    /// Iterate over all connections.
    pub fn connections(&self) -> impl Iterator<Item = &BackendConnection> {
        self.connections.values()
    }

    /// Iterate mutably over all connections.
    pub fn connections_mut(&mut self) -> impl Iterator<Item = &mut BackendConnection> {
        self.connections.values_mut()
    }
}
