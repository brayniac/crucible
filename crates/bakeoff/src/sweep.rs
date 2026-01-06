use clap::ValueEnum;
use serde::{Deserialize, Serialize};

/// Bakeoff suite selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum Suite {
    /// Server comparison: crucible vs valkey with different clients
    #[default]
    Server,
    /// I/O engine comparison: uring vs mio vs tokio
    IoEngine,
}

impl Suite {
    pub fn contexts_file(&self) -> &'static str {
        match self {
            Suite::Server => "contexts.json",
            Suite::IoEngine => "contexts-io.json",
        }
    }
}

// ============================================================================
// Server comparison suite
// ============================================================================

/// The four experiment pairings for server comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Experiment {
    /// crucible-benchmark client -> crucible-segcache server
    CrucibleSegcache,
    /// crucible-benchmark client -> valkey server
    CrucibleValkey,
    /// valkey-benchmark client -> crucible-segcache server
    ValkeySegcache,
    /// valkey-benchmark client -> valkey server
    ValkeyValkey,
}

impl Experiment {
    pub fn all() -> Vec<Experiment> {
        vec![
            Experiment::CrucibleSegcache,
            Experiment::CrucibleValkey,
            Experiment::ValkeySegcache,
            Experiment::ValkeyValkey,
        ]
    }

    pub fn name(&self) -> &'static str {
        match self {
            Experiment::CrucibleSegcache => "crucible-segcache",
            Experiment::CrucibleValkey => "crucible-valkey",
            Experiment::ValkeySegcache => "valkey-segcache",
            Experiment::ValkeyValkey => "valkey-valkey",
        }
    }

    pub fn jsonnet_file(&self) -> &'static str {
        match self {
            Experiment::CrucibleSegcache => "crucible-segcache.jsonnet",
            Experiment::CrucibleValkey => "crucible-valkey.jsonnet",
            Experiment::ValkeySegcache => "valkey-segcache.jsonnet",
            Experiment::ValkeyValkey => "valkey-valkey.jsonnet",
        }
    }
}

// ============================================================================
// I/O Engine comparison suite
// ============================================================================

/// Server engine configuration (runtime + io_engine combination).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerEngine {
    /// Native runtime with io_uring
    Uring,
    /// Native runtime with mio (epoll)
    Mio,
    /// Tokio async runtime
    Tokio,
}

impl ServerEngine {
    pub fn as_str(&self) -> &'static str {
        match self {
            ServerEngine::Uring => "uring",
            ServerEngine::Mio => "mio",
            ServerEngine::Tokio => "tokio",
        }
    }

    /// Returns the runtime config value
    pub fn runtime(&self) -> &'static str {
        match self {
            ServerEngine::Uring | ServerEngine::Mio => "native",
            ServerEngine::Tokio => "tokio",
        }
    }

    /// Returns the io_engine config value (only relevant for native runtime)
    pub fn io_engine(&self) -> &'static str {
        match self {
            ServerEngine::Uring => "uring",
            ServerEngine::Mio => "mio",
            ServerEngine::Tokio => "auto", // not used for tokio runtime
        }
    }
}

/// Client I/O engine configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClientEngine {
    Uring,
    Mio,
}

impl ClientEngine {
    pub fn as_str(&self) -> &'static str {
        match self {
            ClientEngine::Uring => "uring",
            ClientEngine::Mio => "mio",
        }
    }
}

/// I/O engine experiment: server_engine x client_engine combinations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IoExperiment {
    pub server: ServerEngine,
    pub client: ClientEngine,
}

impl IoExperiment {
    pub fn all() -> Vec<IoExperiment> {
        vec![
            IoExperiment {
                server: ServerEngine::Uring,
                client: ClientEngine::Uring,
            },
            IoExperiment {
                server: ServerEngine::Uring,
                client: ClientEngine::Mio,
            },
            IoExperiment {
                server: ServerEngine::Mio,
                client: ClientEngine::Uring,
            },
            IoExperiment {
                server: ServerEngine::Mio,
                client: ClientEngine::Mio,
            },
            IoExperiment {
                server: ServerEngine::Tokio,
                client: ClientEngine::Uring,
            },
            IoExperiment {
                server: ServerEngine::Tokio,
                client: ClientEngine::Mio,
            },
        ]
    }

    pub fn name(&self) -> String {
        format!("{}-{}", self.client.as_str(), self.server.as_str())
    }

    pub fn jsonnet_file(&self) -> &'static str {
        "io-engine.jsonnet"
    }

    pub fn extra_args(&self) -> Vec<String> {
        vec![
            "-p".to_string(),
            format!("server_runtime={}", self.server.runtime()),
            "-p".to_string(),
            format!("server_io_engine={}", self.server.io_engine()),
            "-p".to_string(),
            format!("client_io_engine={}", self.client.as_str()),
        ]
    }
}

/// Parameters for a single experiment run.
#[derive(Debug, Clone)]
pub struct SweepParams {
    pub connections: usize,
    pub pipeline_depth: usize,
}

impl SweepParams {
    /// Generate CLI parameters for systemslab submit.
    pub fn to_args(&self) -> Vec<String> {
        vec![
            "-p".to_string(),
            format!("connections={}", self.connections),
            "-p".to_string(),
            format!("pipeline_depth={}", self.pipeline_depth),
        ]
    }

    pub fn label(&self) -> String {
        format!("c{}_p{}", self.connections, self.pipeline_depth)
    }
}

/// Configuration for parameter sweep.
pub struct SweepConfig {
    pub connections: Vec<usize>,
    pub pipeline_depths: Vec<usize>,
}

impl SweepConfig {
    /// Full sweep configuration for server comparison suite.
    pub fn full() -> Self {
        Self {
            connections: vec![1, 2, 4, 8, 16, 32, 64, 256, 512, 1024, 2048],
            pipeline_depths: vec![1, 8, 16, 32, 64, 128],
        }
    }

    /// Full sweep configuration for io-engine suite.
    /// Focuses on shallow pipelines and varied connection counts
    /// to isolate I/O engine overhead from cache performance.
    pub fn io_engine() -> Self {
        Self {
            connections: vec![8, 16, 32, 64, 128, 256, 512, 1024, 2048],
            pipeline_depths: vec![1, 2, 4, 8, 16, 32, 64, 128, 256],
        }
    }

    /// Limited sweep for testing (2x2 grid).
    pub fn limited() -> Self {
        Self {
            connections: vec![256, 1024],
            pipeline_depths: vec![1, 64],
        }
    }

    pub fn total_combinations(&self) -> usize {
        self.connections.len() * self.pipeline_depths.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = SweepParams> + '_ {
        self.connections.iter().flat_map(move |&connections| {
            self.pipeline_depths
                .iter()
                .map(move |&pipeline_depth| SweepParams {
                    connections,
                    pipeline_depth,
                })
        })
    }
}
