pub mod accumulator;
pub mod acceptor;
pub mod buffer;
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
pub use config::{Config, RecvBufferConfig, WorkerConfig};
#[cfg(feature = "tls")]
pub use config::TlsConfig;
#[cfg(feature = "tls")]
pub use config::TlsClientConfig;
pub use error::Error;
pub use guard::{GuardBox, SendGuard};
pub use handler::{ConnToken, DriverCtx, EventHandler, SendBuilder};
pub use buffer::fixed::{MemoryRegion, RegionId};
pub use buffer::send_slab::{MAX_GUARDS, MAX_IOVECS};
pub use completion::{OpTag, UserData};
pub use event_loop::EventLoop;
pub use worker::{launch, KompioBuilder, ShutdownHandle};
#[cfg(feature = "tls")]
pub use tls::TlsInfo;
