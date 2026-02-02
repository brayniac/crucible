//! Crucible Proxy binary.

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::Parser;
use proxy::Config;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "crucible-proxy")]
#[command(about = "High-performance Valkey/Redis proxy")]
struct Args {
    /// Path to configuration file
    config: Option<PathBuf>,

    /// Print default configuration and exit
    #[arg(long)]
    print_config: bool,
}

fn main() {
    let args = Args::parse();

    if args.print_config {
        print_default_config();
        return;
    }

    let config = match &args.config {
        Some(path) => match Config::load(path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to load config: {}", e);
                std::process::exit(1);
            }
        },
        None => {
            eprintln!("No config file specified. Use --config <path> or --print-config");
            std::process::exit(1);
        }
    };

    // Initialize logging
    proxy::logging::init(&config.logging);

    // Install signal handler
    let shutdown = proxy::signal::install_signal_handler();

    // Run the proxy
    if let Err(e) = proxy::run(&config, shutdown) {
        tracing::error!(error = %e, "Proxy error");
        std::process::exit(1);
    }
}

fn print_default_config() {
    let config = r#"# Crucible Proxy Configuration

[proxy]
# Address to listen on for client connections
listen = "0.0.0.0:6379"

[workers]
# Number of worker threads (default: number of CPUs)
# threads = 8

# CPU cores to pin workers to (Linux-style, e.g., "0-3,6-8")
# cpu_affinity = "0-7"

[backend]
# Backend node addresses (seed nodes)
nodes = ["127.0.0.1:6380"]

# Connections per backend node per worker
pool_size = 2

# Connection timeout in milliseconds
connect_timeout_ms = 1000

# Read timeout in milliseconds
read_timeout_ms = 500

[cache]
# Enable local caching of GET responses
enabled = true

# Maximum number of cached items
max_items = 1000000

# TTL for cached items in milliseconds (0 = no expiry)
ttl_ms = 300000

[uring]
# Buffer size for recv operations
buffer_size = 4096

# Number of buffers in the buffer pool
buffer_count = 1024

# Submission queue depth
sq_depth = 1024

# Enable SQPOLL mode (requires CAP_SYS_NICE or root)
sqpoll = false

[metrics]
# Admin server with health checks and Prometheus metrics
address = "0.0.0.0:9090"

[logging]
# Log level: "error", "warn", "info", "debug", "trace"
level = "info"

# Log format: "pretty", "json", or "compact"
format = "pretty"

[shutdown]
# Drain timeout in seconds
drain_timeout_secs = 30
"#;
    print!("{}", config);
}
