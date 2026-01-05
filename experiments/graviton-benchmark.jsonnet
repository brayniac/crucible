local systemslab = import 'systemslab.libsonnet';

local server_config = {
    // Runtime selection: "native" (io_uring/mio) or "tokio"
    runtime: 'native',

    workers: {
        threads: error 'threads must be specified',
    },

    cache: {
        backend: error 'backend must be specified',
        heap_size: error 'heap_size must be specified',
        segment_size: '1MB',
        hashtable_power: 20,
        hugepage: '2MB',
    },

    listener: [
        {
            protocol: 'resp',
            address: '0.0.0.0:6379',
        },
    ],

    metrics: {
        address: '0.0.0.0:9090',
    },

    uring: {
        sqpoll: false,
        sqpoll_idle_ms: 1000,
        buffer_count: 1024,
        buffer_size: 4096,
        sq_depth: 1024,
    },
};

local benchmark_config = {
    general: {
        duration: error 'duration must be specified',
        warmup: error 'warmup must be specified',
        threads: error 'threads must be specified',
        io_engine: 'auto',
    },

    target: {
        // Server address will be replaced by sed
        endpoints: ['172.31.31.218:6379'],
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
    // Git parameters
    repo='https://github.com/brayniac/crucible.git',
    git_ref='main',

    // Server parameters
    cache_backend='segcache',
    heap_size='8GB',
    segment_size='1MB',
    hashtable_power='20',
    server_threads='8',
    server_cpu_affinity='0-7',
    runtime='native',

    // Benchmark parameters
    benchmark_threads='24',
    benchmark_cpu_affinity='8-32',
    connections='256',
    pipeline_depth='64',
    key_length='16',
    key_count='1000000',
    value_length='64',
    get_percent='80',
    warmup_duration='30s',
    test_duration='300s',
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
        hashtable_power: hashtable_power,
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
        hashtable_power_int = std.parseInt(args.hashtable_power),
        set_percent_int = 100 - get_percent_int;

    assert get_percent_int >= 0 && get_percent_int <= 100 : 'get_percent must be between 0 and 100';
    assert cache_backend == 'segcache' || cache_backend == 's3fifo' : 'cache_backend must be segcache or s3fifo';
    assert runtime == 'native' || runtime == 'tokio' : 'runtime must be native or tokio';

    local
        cache_config = server_config {
            runtime: runtime,
            workers+: {
                threads: server_threads_int,
                [if server_cpu_affinity != '' then 'cpu_affinity']: server_cpu_affinity,
            },
            cache+: {
                backend: cache_backend,
                heap_size: heap_size,
                segment_size: segment_size,
                hashtable_power: hashtable_power_int,
            },
        },

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
                    get: get_percent_int,
                    set: set_percent_int,
                },
                values+: {
                    length: value_length_int,
                },
            },
        };

    {
        name: 'crucible_' + cache_backend + '_c' + connections + '_k' + key_length + '_v' + value_length + '_r' + get_percent,
        jobs: {
            server: {
                local config = std.manifestTomlEx(cache_config, ''),

                host: {
                    tags: ['c8g.2xl'],
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

                    // Build the server binary
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build
                            source $HOME/.cargo/env

                            cd $HOME/crucible
                            cargo build --release -p server
                        |||
                    ),

                    // Write out the server config
                    systemslab.write_file('server.toml', config),

                    // Server tuning
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build
                            mkdir -p $HOME

                            sudo ethtool -L ens34 combined 8
                        |||
                    ),

                    // Start the cache server in the background
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build

                            ulimit -n 500000
                            sudo prlimit --memlock=unlimited --pid $$
                            ulimit -a
                            export CRUCIBLE_DIAGNOSTICS=1
                            $HOME/crucible/target/release/crucible-server server.toml &
                            echo PID=$! > pid
                        |||,
                        background=true
                    ),

                    // Give the server a moment to start up
                    systemslab.bash('sleep 10'),

                    // Verify the server is listening
                    systemslab.bash(
                        |||
                            nc -z localhost 6379 || { echo "Cache server failed to start"; exit 1; }
                        |||
                    ),

                    // Signal that the cache is ready
                    systemslab.barrier('cache-start'),

                    // Wait for warmup to complete
                    systemslab.barrier('warmup-complete'),

                    // Verify the server is still running after warmup
                    systemslab.bash(
                        |||
                            nc -z localhost 6379 || { echo "Cache server died during warmup"; exit 1; }
                        |||
                    ),

                    // Wait for the benchmark to finish
                    systemslab.barrier('test-finish'),

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
                    tags: ['c8g.8xl'],
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

                            export CRUCIBLE_DIAGNOSTICS=1 RUST_LOG=benchmark=debug
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

                            export CRUCIBLE_DIAGNOSTICS=1 RUST_LOG=benchmark=debug
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
