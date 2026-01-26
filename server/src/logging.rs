//! Structured logging initialization.
//!
//! Configures the tracing subscriber for structured logging output.
//! The RUST_LOG environment variable takes precedence over configuration file settings.

use crate::config::{LogFormat, LoggingConfig};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

/// Initialize the logging subsystem.
///
/// The RUST_LOG environment variable takes precedence over the configuration
/// file level setting. If RUST_LOG is not set, the level from config is used.
///
/// # Example
///
/// ```ignore
/// use server::logging;
/// use server::config::LoggingConfig;
///
/// let config = LoggingConfig::default();
/// logging::init(&config);
///
/// tracing::info!("Server starting");
/// ```
pub fn init(config: &LoggingConfig) {
    // Check if RUST_LOG is set; if so, use it instead of config level
    let filter = if std::env::var("RUST_LOG").is_ok() {
        EnvFilter::from_default_env()
    } else {
        EnvFilter::new(config.level.as_str())
    };

    match (config.format, config.timestamps) {
        (LogFormat::Pretty, true) => {
            let subscriber = tracing_subscriber::registry().with(filter).with(
                fmt::layer()
                    .with_ansi(true)
                    .with_target(config.target)
                    .with_thread_names(config.thread_names),
            );
            subscriber.init();
        }
        (LogFormat::Pretty, false) => {
            let subscriber = tracing_subscriber::registry().with(filter).with(
                fmt::layer()
                    .with_ansi(true)
                    .with_target(config.target)
                    .with_thread_names(config.thread_names)
                    .without_time(),
            );
            subscriber.init();
        }
        (LogFormat::Json, true) => {
            let subscriber = tracing_subscriber::registry().with(filter).with(
                fmt::layer()
                    .json()
                    .with_target(config.target)
                    .with_thread_names(config.thread_names),
            );
            subscriber.init();
        }
        (LogFormat::Json, false) => {
            let subscriber = tracing_subscriber::registry().with(filter).with(
                fmt::layer()
                    .json()
                    .with_target(config.target)
                    .with_thread_names(config.thread_names)
                    .without_time(),
            );
            subscriber.init();
        }
        (LogFormat::Compact, true) => {
            let subscriber = tracing_subscriber::registry().with(filter).with(
                fmt::layer()
                    .compact()
                    .with_ansi(true)
                    .with_target(config.target)
                    .with_thread_names(config.thread_names),
            );
            subscriber.init();
        }
        (LogFormat::Compact, false) => {
            let subscriber = tracing_subscriber::registry().with(filter).with(
                fmt::layer()
                    .compact()
                    .with_ansi(true)
                    .with_target(config.target)
                    .with_thread_names(config.thread_names)
                    .without_time(),
            );
            subscriber.init();
        }
    }
}
