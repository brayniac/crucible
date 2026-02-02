//! Backend connection pool.

use super::connection::BackendConnection;
use ahash::AHashMap;
use io_driver::ConnId;
use std::net::SocketAddr;

/// A pool of connections to backend nodes.
///
/// For Phase 1, this is simplified to a single backend node.
/// Phase 3 will add cluster support with multiple nodes and slot routing.
pub struct BackendPool {
    /// Backend address (single node for Phase 1).
    backend_addr: SocketAddr,

    /// Connections indexed by ConnId.
    connections: AHashMap<ConnId, BackendConnection>,

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

    /// Get the target pool size.
    pub fn target_size(&self) -> usize {
        self.pool_size
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
        self.connections.insert(conn.conn_id, conn);
    }

    /// Get a connection by ID.
    pub fn get_connection(&self, conn_id: ConnId) -> Option<&BackendConnection> {
        self.connections.get(&conn_id)
    }

    /// Get a mutable connection by ID.
    pub fn get_connection_mut(&mut self, conn_id: ConnId) -> Option<&mut BackendConnection> {
        self.connections.get_mut(&conn_id)
    }

    /// Remove a connection.
    pub fn remove_connection(&mut self, conn_id: ConnId) -> Option<BackendConnection> {
        self.connections.remove(&conn_id)
    }

    /// Get an idle connection for sending a request.
    pub fn get_idle_connection(&mut self) -> Option<&mut BackendConnection> {
        self.connections.values_mut().find(|c| c.is_idle())
    }

    /// Get a usable connection (idle preferred, or one with capacity for pipelining).
    pub fn get_usable_connection(&mut self) -> Option<&mut BackendConnection> {
        // First try to find an idle connection, then any usable one
        // We need to do this in a single pass to satisfy the borrow checker
        let mut idle_id = None;
        let mut usable_id = None;

        for (conn_id, conn) in &self.connections {
            if conn.is_idle() {
                idle_id = Some(*conn_id);
                break;
            } else if usable_id.is_none() && conn.is_usable() {
                usable_id = Some(*conn_id);
            }
        }

        idle_id
            .or(usable_id)
            .and_then(|id| self.connections.get_mut(&id))
    }

    /// Iterate over all connections.
    pub fn connections(&self) -> impl Iterator<Item = &BackendConnection> {
        self.connections.values()
    }

    /// Iterate mutably over all connections.
    pub fn connections_mut(&mut self) -> impl Iterator<Item = &mut BackendConnection> {
        self.connections.values_mut()
    }

    /// Mark a connection as connected.
    pub fn mark_connected(&mut self, conn_id: ConnId) {
        if let Some(conn) = self.connections.get_mut(&conn_id) {
            conn.mark_connected();
        }
    }
}
