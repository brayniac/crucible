pub mod acceptor;
pub mod accumulator;
pub mod buffer;
pub(crate) mod chain;
pub mod completion;
pub mod config;
pub mod connection;
pub mod error;
pub mod event_loop;
pub mod guard;
pub mod handler;
pub mod ring;
#[cfg(feature = "tls")]
pub mod tls;
pub mod worker;

// Public API re-exports
pub use buffer::fixed::{MemoryRegion, RegionId};
pub use buffer::send_slab::{MAX_GUARDS, MAX_IOVECS};
pub use completion::{OpTag, UserData};
#[cfg(feature = "tls")]
pub use config::TlsClientConfig;
#[cfg(feature = "tls")]
pub use config::TlsConfig;
pub use config::{Config, RecvBufferConfig, WorkerConfig};
pub use error::Error;
pub use event_loop::EventLoop;
pub use guard::{GuardBox, SendGuard};
pub use handler::{ConnToken, DriverCtx, EventHandler, SendBuilder, SendChainBuilder, ChainPartsBuilder};
#[cfg(feature = "tls")]
pub use tls::TlsInfo;
pub use worker::{KompioBuilder, ShutdownHandle, launch};
