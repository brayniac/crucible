local systemslab = import 'systemslab.libsonnet';

local server_config = {
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

    // Server parameters - these define what configuration to optimize for
    cache_backend='segcache',
    heap_size='8GB',
    segment_size='1MB',
    hashtable_power='20',
    server_threads='16',
    server_cpu_affinity='0-15',
    runtime='native',

    // Benchmark parameters for profiling workload
    benchmark_threads='16',
    connections='256',
    pipeline_depth='16',
    key_length='16',
    key_count='1000000',
    value_length='64',

    // PGO training workload - should be representative of production
    // Run multiple phases to capture different access patterns
    warmup_duration='60s',
    read_heavy_duration='120s',
    write_heavy_duration='60s',
    mixed_duration='120s'
)
    local args = {
        server_threads: server_threads,
        benchmark_threads: benchmark_threads,
        connections: connections,
        pipeline_depth: pipeline_depth,
        key_length: key_length,
        key_count: key_count,
        value_length: value_length,
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
        hashtable_power_int = std.parseInt(args.hashtable_power);

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

        base_benchmark = benchmark_config {
            general+: {
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
                values+: {
                    length: value_length_int,
                },
            },
        },

        // Warmup: write-heavy to populate cache
        warmup_config = base_benchmark {
            general+: { duration: warmup_duration },
            workload+: { commands: { get: 10, set: 90 } },
        },

        // Read-heavy workload (typical production pattern)
        read_heavy_config = base_benchmark {
            general+: { duration: read_heavy_duration },
            workload+: { commands: { get: 95, set: 5 } },
        },

        // Write-heavy workload
        write_heavy_config = base_benchmark {
            general+: { duration: write_heavy_duration },
            workload+: { commands: { get: 20, set: 80 } },
        },

        // Mixed workload
        mixed_config = base_benchmark {
            general+: { duration: mixed_duration },
            workload+: { commands: { get: 50, set: 50 } },
        };

    {
        name: 'pgo_' + cache_backend + '_' + runtime,
        jobs: {
            pgo_build: {
                local config = std.manifestTomlEx(cache_config, ''),
                local warmup = std.manifestTomlEx(warmup_config, ''),
                local read_heavy = std.manifestTomlEx(read_heavy_config, ''),
                local write_heavy = std.manifestTomlEx(write_heavy_config, ''),
                local mixed = std.manifestTomlEx(mixed_config, ''),

                host: {
                    tags: ['baremetal', 'server'],
                },

                steps: [
                    // Set up Rust toolchain
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-pgo
                            mkdir -p $HOME

                            if [ ! -f $HOME/.cargo/bin/cargo ]; then
                                curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
                            fi

                            # Ensure we have the llvm-tools for profile merging
                            source $HOME/.cargo/env
                            rustup component add llvm-tools-preview
                        |||
                    ),

                    // Clone or update the repository
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-pgo
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

                    // Phase 1: Build instrumented binaries for profile generation
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-pgo
                            source $HOME/.cargo/env

                            cd $HOME/crucible

                            # Clean previous builds
                            cargo clean

                            # Create directory for profile data
                            mkdir -p $HOME/pgo-profiles

                            # Build with profile generation instrumentation
                            RUSTFLAGS="-Cprofile-generate=$HOME/pgo-profiles" \
                                cargo build --release -p server -p benchmark

                            echo "Instrumented build complete"
                        |||
                    ),

                    // Write config files
                    systemslab.write_file('server.toml', config),
                    systemslab.write_file('warmup.toml', warmup),
                    systemslab.write_file('read_heavy.toml', read_heavy),
                    systemslab.write_file('write_heavy.toml', write_heavy),
                    systemslab.write_file('mixed.toml', mixed),

                    // Phase 2: Run instrumented server with training workloads
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-pgo

                            echo "Starting instrumented server for profile collection..."
                            ulimit -n 500000

                            $HOME/crucible/target/release/crucible-server server.toml &
                            SERVER_PID=$!
                            echo "SERVER_PID=$SERVER_PID" > pids.env

                            sleep 5

                            # Verify server started
                            nc -z localhost 6379 || { echo "Server failed to start"; kill $SERVER_PID; exit 1; }

                            echo "Server running with PID $SERVER_PID"
                        |||,
                        background=true
                    ),

                    systemslab.bash('sleep 5'),

                    // Run warmup workload
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-pgo
                            ulimit -n 500000

                            echo "=== Running warmup workload ==="
                            $HOME/crucible/target/release/crucible-benchmark warmup.toml
                            sleep 2
                        |||
                    ),

                    // Run read-heavy workload (most common production pattern)
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-pgo
                            ulimit -n 500000

                            echo "=== Running read-heavy workload ==="
                            $HOME/crucible/target/release/crucible-benchmark read_heavy.toml
                            sleep 2
                        |||
                    ),

                    // Run write-heavy workload
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-pgo
                            ulimit -n 500000

                            echo "=== Running write-heavy workload ==="
                            $HOME/crucible/target/release/crucible-benchmark write_heavy.toml
                            sleep 2
                        |||
                    ),

                    // Run mixed workload
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-pgo
                            ulimit -n 500000

                            echo "=== Running mixed workload ==="
                            $HOME/crucible/target/release/crucible-benchmark mixed.toml
                            sleep 2
                        |||
                    ),

                    // Stop the instrumented server to flush profile data
                    systemslab.bash(
                        |||
                            source pids.env
                            echo "Stopping instrumented server to flush profiles..."
                            kill $SERVER_PID
                            sleep 5

                            echo "Profile files collected:"
                            ls -la $HOME/pgo-profiles/
                        |||
                    ),

                    // Phase 3: Merge profile data
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-pgo
                            source $HOME/.cargo/env

                            # Find llvm-profdata
                            LLVM_PROFDATA=$(find $HOME/.rustup -name 'llvm-profdata' -type f | head -1)

                            if [ -z "$LLVM_PROFDATA" ]; then
                                echo "ERROR: llvm-profdata not found"
                                exit 1
                            fi

                            echo "Using: $LLVM_PROFDATA"

                            # Merge all profile data
                            $LLVM_PROFDATA merge \
                                -o $HOME/pgo-profiles/merged.profdata \
                                $HOME/pgo-profiles/*.profraw

                            echo "Merged profile data:"
                            ls -la $HOME/pgo-profiles/merged.profdata
                        |||
                    ),

                    // Phase 4: Build optimized binary using profile data
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-pgo
                            source $HOME/.cargo/env

                            cd $HOME/crucible

                            # Clean and rebuild with profile data
                            cargo clean

                            RUSTFLAGS="-Cprofile-use=$HOME/pgo-profiles/merged.profdata -Cllvm-args=-pgo-warn-missing-function" \
                                cargo build --release -p server

                            echo "PGO-optimized build complete"
                            ls -la target/release/crucible-server

                            # Copy to output location
                            cp target/release/crucible-server $HOME/crucible-server-pgo
                        |||
                    ),

                    // Upload the PGO-optimized binary
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-pgo
                            cp $HOME/crucible-server-pgo ./crucible-server-pgo

                            # Also create a tarball with the binary
                            tar -czvf crucible-server-pgo.tar.gz crucible-server-pgo

                            echo "PGO build artifacts ready for upload"
                            ls -la crucible-server-pgo*
                        |||
                    ),

                    systemslab.upload_artifact('crucible-server-pgo', tags=['pgo', 'binary', runtime, cache_backend]),
                    systemslab.upload_artifact('crucible-server-pgo.tar.gz', tags=['pgo', 'archive', runtime, cache_backend]),
                    systemslab.upload_artifact('server.toml', tags=['config']),
                ],
            },
        },
    }
