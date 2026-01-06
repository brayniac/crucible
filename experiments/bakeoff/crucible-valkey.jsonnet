local systemslab = import 'systemslab.libsonnet';

local benchmark_config = {
    general: {
        duration: error 'duration must be specified',
        warmup: error 'warmup must be specified',
        threads: error 'threads must be specified',
        io_engine: 'auto',
    },

    target: {
        endpoints: ['SERVER_ADDR:6379'],
        protocol: 'resp',
    },

    connection: {
        connections: error 'connections must be specified',
        pipeline_depth: error 'pipeline_depth must be specified',
        connect_timeout: '5s',
        request_timeout: '1s',
    },

    workload: {
        keyspace: {
            length: error 'key_length must be specified',
            count: error 'key_count must be specified',
            distribution: 'uniform',
        },
        commands: {
            get: error 'get_weight must be specified',
            set: error 'set_weight must be specified',
        },
        values: {
            length: error 'value_length must be specified',
        },
    },

    timestamps: {
        enabled: true,
        mode: 'userspace',
    },
};

function(
    // Git parameters for benchmark client
    repo='https://github.com/brayniac/crucible.git',
    git_ref='main',

    // Valkey parameters
    valkey_version='9.0.1',
    server_threads='8',
    server_cpu_affinity='0-7',
    maxmemory='8gb',

    // Benchmark parameters
    benchmark_threads='24',
    benchmark_cpu_affinity='8-31',
    connections='256',
    pipeline_depth='64',
    key_length='20',
    key_count='1000000',
    value_length='64',
    get_percent='80',
    warmup_duration='30s',
    test_duration='60s',
    rate_limit=''
)
    local args = {
        server_threads: server_threads,
        benchmark_threads: benchmark_threads,
        connections: connections,
        pipeline_depth: pipeline_depth,
        key_length: key_length,
        key_count: key_count,
        value_length: value_length,
        get_percent: get_percent,
    };

    local
        server_threads_int = std.parseInt(args.server_threads),
        benchmark_threads_int = std.parseInt(args.benchmark_threads),
        connections_int = std.parseInt(args.connections),
        pipeline_depth_int = std.parseInt(args.pipeline_depth),
        key_length_int = std.parseInt(args.key_length),
        key_count_int = std.parseInt(args.key_count),
        value_length_int = std.parseInt(args.value_length),
        get_percent_int = std.parseInt(args.get_percent),
        set_percent_int = 100 - get_percent_int;

    assert get_percent_int >= 0 && get_percent_int <= 100 : 'get_percent must be between 0 and 100';

    local
        // Valkey config as command line arguments
        valkey_args = [
            '--port 6379',
            '--bind 0.0.0.0',
            '--protected-mode no',
            '--maxclients 65000',
            '--tcp-backlog 65535',
            '--maxmemory ' + maxmemory,
            '--maxmemory-policy allkeys-random',
            '--io-threads ' + server_threads,
            '--io-threads-do-reads yes',
            '--appendonly no',
            '--save ""',
        ],

        warmup_benchmark_config = benchmark_config {
            general+: {
                duration: warmup_duration,
                warmup: '0s',
                threads: benchmark_threads_int,
                [if benchmark_cpu_affinity != '' then 'cpu_list']: benchmark_cpu_affinity,
            },
            connection+: {
                connections: connections_int,
                pipeline_depth: pipeline_depth_int,
            },
            workload+: {
                [if rate_limit != '' then 'rate_limit']: std.parseInt(rate_limit),
                keyspace+: {
                    length: key_length_int,
                    count: key_count_int,
                },
                commands: {
                    // Warmup is write-heavy to populate the cache
                    get: 10,
                    set: 90,
                },
                values+: {
                    length: value_length_int,
                },
            },
        },

        test_benchmark_config = benchmark_config {
            general+: {
                duration: test_duration,
                warmup: '0s',
                threads: benchmark_threads_int,
                [if benchmark_cpu_affinity != '' then 'cpu_list']: benchmark_cpu_affinity,
            },
            connection+: {
                connections: connections_int,
                pipeline_depth: pipeline_depth_int,
            },
            workload+: {
                [if rate_limit != '' then 'rate_limit']: std.parseInt(rate_limit),
                keyspace+: {
                    length: key_length_int,
                    count: key_count_int,
                },
                commands: {
                    // GET only to match valkey-benchmark -t get
                    get: 100,
                    set: 0,
                },
                values+: {
                    length: value_length_int,
                },
            },
        };

    {
        name: 'valkey_' + valkey_version + '_c' + connections + '_k' + key_length + '_v' + value_length + '_r' + get_percent,
        jobs: {
            server: {
                host: {
                    tags: ['c8g-2xlarge'],
                },

                steps: [
                    // Install valkey from source
                    systemslab.bash(
                        |||
                            export HOME=/tmp/valkey-build
                            mkdir -p $HOME
                            cd $HOME

                            if [ ! -f valkey-server ]; then
                                echo "Downloading and building Valkey %(version)s..."
                                curl -LO https://github.com/valkey-io/valkey/archive/refs/tags/%(version)s.tar.gz
                                tar xzf %(version)s.tar.gz
                                cd valkey-%(version)s
                                make -j$(nproc) BUILD_TLS=no
                                cp src/valkey-server $HOME/
                                cd $HOME
                            fi

                            echo "Valkey server ready"
                            $HOME/valkey-server --version
                        ||| % { version: valkey_version }
                    ),

                    // Server tuning
                    systemslab.bash(
                        |||
                            # Increase TCP backlog
                            sudo sysctl -w net.core.somaxconn=65535
                            sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65535

                            # Disable THP (recommended by Redis/Valkey)
                            echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled || true

                            # Network tuning
                            sudo ethtool -L ens34 combined 8 || true
                        |||
                    ),

                    // Start valkey server in the background
                    systemslab.bash(
                        |||
                            export HOME=/tmp/valkey-build

                            ulimit -n 500000
                            ulimit -a

                            %(cpu_affinity_cmd)s $HOME/valkey-server %(valkey_args)s &
                            echo PID=$! > pid

                            echo "Valkey server started with PID $(cat pid)"
                        ||| % {
                            valkey_args: std.join(' ', valkey_args),
                            cpu_affinity_cmd: if server_cpu_affinity != '' then 'taskset -c ' + server_cpu_affinity else '',
                        },
                        background=true
                    ),

                    // Give the server a moment to start up
                    systemslab.bash('sleep 5'),

                    // Verify the server is listening
                    systemslab.bash(
                        |||
                            nc -z localhost 6379 || { echo "Valkey server failed to start"; exit 1; }
                            echo "Valkey server is ready"
                        |||
                    ),

                    // Signal that the cache is ready
                    systemslab.barrier('cache-start'),

                    // Wait for warmup to complete
                    systemslab.barrier('warmup-complete'),

                    // Verify the server is still running after warmup
                    systemslab.bash(
                        |||
                            nc -z localhost 6379 || { echo "Valkey server died during warmup"; exit 1; }
                        |||
                    ),

                    // Wait for the benchmark to finish
                    systemslab.barrier('test-finish'),

                    // Get valkey stats before shutdown
                    systemslab.bash(
                        |||
                            echo "=== Valkey INFO stats ==="
                            echo "INFO" | nc localhost 6379 | head -100
                        |||
                    ),

                    // Shutdown the server
                    systemslab.bash(
                        |||
                            source ./pid
                            kill $PID
                        |||
                    ),
                ],
            },

            client: {
                local warmup = std.manifestTomlEx(warmup_benchmark_config, ''),
                local loadgen = std.manifestTomlEx(test_benchmark_config, ''),

                host: {
                    tags: ['c8g-8xlarge'],
                },

                steps: [
                    // Set up persistent build environment with Rust toolchain
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build
                            mkdir -p $HOME

                            if [ ! -f $HOME/.cargo/bin/cargo ]; then
                                curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
                            fi
                        |||
                    ),

                    // Clone or update the repository
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build
                            source $HOME/.cargo/env

                            cd $HOME
                            if [ -d crucible ]; then
                                cd crucible
                                git fetch origin
                                git checkout %(git_ref)s
                                git pull origin %(git_ref)s || true
                            else
                                git clone %(repo)s crucible
                                cd crucible
                                git checkout %(git_ref)s
                            fi
                        ||| % { repo: repo, git_ref: git_ref }
                    ),

                    // Build the benchmark binary
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build
                            source $HOME/.cargo/env

                            cd $HOME/crucible
                            cargo build --release -p benchmark
                        |||
                    ),

                    // Write out the toml configs
                    systemslab.write_file('warmup.toml', warmup),
                    systemslab.write_file('loadgen.toml', loadgen),

                    // Replace SERVER_ADDR placeholder with actual server address
                    systemslab.bash(
                        |||
                            sed -i "s/SERVER_ADDR/$SERVER_ADDR/g" warmup.toml
                            sed -i "s/SERVER_ADDR/$SERVER_ADDR/g" loadgen.toml
                        |||
                    ),

                    // Wait for the cache to start
                    systemslab.barrier('cache-start'),

                    // Run the warmup workload
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build

                            ulimit -n 500000
                            sudo prlimit --memlock=unlimited --pid $$
                            ulimit -a

                            export RUST_LOG=benchmark=info
                            $HOME/crucible/target/release/crucible-benchmark warmup.toml
                        |||
                    ),

                    // Wait for connections to clean up
                    systemslab.bash('sleep 10'),

                    // Signal warmup is complete
                    systemslab.barrier('warmup-complete'),

                    // Run the benchmark
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build

                            ulimit -n 500000
                            sudo prlimit --memlock=unlimited --pid $$
                            ulimit -a

                            export RUST_LOG=benchmark=info
                            $HOME/crucible/target/release/crucible-benchmark loadgen.toml
                        |||
                    ),

                    // Signal that the test is complete
                    systemslab.barrier('test-finish'),

                    // Upload the artifacts
                    systemslab.upload_artifact('loadgen.toml', tags=['benchmark-config']),
                ],
            },
        },
    }
