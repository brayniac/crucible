//! Backend connection management.

mod connection;
mod pool;

pub use connection::{BackendConnection, BackendState, InFlightRequest};
pub use pool::BackendPool;
