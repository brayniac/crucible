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
        warmup: '0s',
        threads: error 'threads must be specified',
        io_engine: 'mio',
    },

    target: {
        endpoints: ['127.0.0.1:6379'],
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
    server_cpu_affinity='4-7,20-23',
    runtime='native',

    // Benchmark parameters
    benchmark_threads='16',
    benchmark_cpu_affinity='8-15,24-31',
    connections='256',
    pipeline_depth='16',
    key_length='16',
    key_count='1000000',
    value_length='64',
    get_percent='80',

    // Flamegraph parameters
    warmup_duration='30s',
    record_duration='30',
    perf_frequency='99'
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
        record_duration: record_duration,
        perf_frequency: perf_frequency,
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

        // Warmup config - write-heavy to populate cache
        warmup_benchmark_config = benchmark_config {
            general+: {
                duration: warmup_duration,
                threads: benchmark_threads_int,
                [if benchmark_cpu_affinity != '' then 'cpu_list']: benchmark_cpu_affinity,
            },
            connection+: {
                connections: connections_int,
                pipeline_depth: pipeline_depth_int,
            },
            workload+: {
                keyspace+: {
                    length: key_length_int,
                    count: key_count_int,
                },
                commands: {
                    get: 10,
                    set: 90,
                },
                values+: {
                    length: value_length_int,
                },
            },
        },

        // Load config for profiling - normal read-heavy workload
        load_benchmark_config = benchmark_config {
            general+: {
                // Run longer than record duration to ensure we capture full profile
                duration: std.toString(std.parseInt(args.record_duration) + 60) + 's',
                threads: benchmark_threads_int,
            },
            connection+: {
                connections: connections_int,
                pipeline_depth: pipeline_depth_int,
            },
            workload+: {
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
        name: 'flamegraph_' + cache_backend + '_' + runtime + '_t' + server_threads,
        jobs: {
            profiler: {
                local config = std.manifestTomlEx(cache_config, ''),
                local warmup = std.manifestTomlEx(warmup_benchmark_config, ''),
                local loadgen = std.manifestTomlEx(load_benchmark_config, ''),

                host: {
                    tags: ['baremetal', 'server'],
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

                            # Install inferno for flamegraph generation
                            source $HOME/.cargo/env
                            if ! command -v inferno-flamegraph &> /dev/null; then
                                cargo install inferno
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

                    // Build the server and benchmark binaries
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build
                            source $HOME/.cargo/env

                            cd $HOME/crucible
                            cargo build --release -p server -p benchmark
                        |||
                    ),

                    // Write out configs
                    systemslab.write_file('server.toml', config),
                    systemslab.write_file('warmup.toml', warmup),
                    systemslab.write_file('loadgen.toml', loadgen),

                    // Start the cache server in the background
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build

                            ulimit -n 500000
                            sudo prlimit --memlock=unlimited --pid $$
                            ulimit -a
                            $HOME/crucible/target/release/crucible-server server.toml &
                            SERVER_PID=$!
                            echo "SERVER_PID=$SERVER_PID" > pids.env
                        |||,
                        background=true
                    ),

                    // Give the server a moment to start up
                    systemslab.bash('sleep 5'),

                    // Verify the server is listening
                    systemslab.bash(
                        |||
                            nc -z localhost 6379 || { echo "Cache server failed to start"; exit 1; }
                        |||
                    ),

                    // Run warmup workload to populate cache
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build

                            echo "Running warmup workload..."
                            ulimit -n 500000
                            sudo prlimit --memlock=unlimited --pid $$
                            $HOME/crucible/target/release/crucible-benchmark warmup.toml
                            echo "Warmup complete"

                            # Brief pause to let connections settle
                            sleep 5
                        |||
                    ),

                    // Start load generation in background and record with perf
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build
                            source pids.env

                            echo "Starting load generation..."
                            ulimit -n 500000
                            sudo prlimit --memlock=unlimited --pid $$
                            $HOME/crucible/target/release/crucible-benchmark loadgen.toml &
                            BENCHMARK_PID=$!
                            echo "BENCHMARK_PID=$BENCHMARK_PID" >> pids.env

                            # Wait for benchmark to ramp up
                            sleep 10

                            echo "Recording with perf for %(record_duration)s seconds..."
                            sudo perf record \
                                --call-graph dwarf \
                                --freq %(perf_frequency)s \
                                -p $SERVER_PID \
                                -o perf.data \
                                -- sleep %(record_duration)s

                            echo "Perf recording complete"
                        ||| % { record_duration: args.record_duration, perf_frequency: args.perf_frequency }
                    ),

                    // Generate flamegraph
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build
                            source $HOME/.cargo/env

                            echo "Generating flamegraph..."
                            sudo perf script -i perf.data | \
                                $HOME/.cargo/bin/inferno-collapse-perf | \
                                $HOME/.cargo/bin/inferno-flamegraph > flamegraph.svg

                            echo "Flamegraph generated: flamegraph.svg"
                            ls -la flamegraph.svg
                        |||
                    ),

                    // Shutdown processes
                    systemslab.bash(
                        |||
                            source pids.env

                            # Stop benchmark if still running
                            if [ -n "$BENCHMARK_PID" ]; then
                                kill $BENCHMARK_PID 2>/dev/null || true
                            fi

                            # Stop server
                            if [ -n "$SERVER_PID" ]; then
                                kill $SERVER_PID 2>/dev/null || true
                            fi

                            echo "Cleanup complete"
                        |||
                    ),

                    // Upload artifacts
                    systemslab.upload_artifact('flamegraph.svg', tags=['flamegraph', runtime, cache_backend]),
                    systemslab.upload_artifact('server.toml', tags=['config']),
                    systemslab.upload_artifact('loadgen.toml', tags=['config']),
                ],
            },
        },
    }
