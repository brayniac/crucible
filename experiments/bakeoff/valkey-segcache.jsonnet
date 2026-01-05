local systemslab = import 'systemslab.libsonnet';

// Benchmark Crucible server using valkey-benchmark (native Redis benchmark tool)
// for fair comparison with valkey-native-benchmark.jsonnet

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
        hugepage: 'disabled',
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

function(
    // Git parameters for crucible
    repo='https://github.com/brayniac/crucible.git',
    git_ref='main',

    // Crucible server parameters
    cache_backend='segcache',
    heap_size='8GB',
    segment_size='1MB',
    hashtable_power='20',
    server_threads='8',
    server_cpu_affinity='0-7',
    runtime='native',

    // Valkey benchmark parameters
    valkey_version='9.0.1',
    benchmark_threads='24',
    benchmark_cpu_affinity='8-31',
    connections='256',
    pipeline_depth='64',
    key_count='1000000',
    value_length='64',
    get_percent='80',
    warmup_requests='10000000',
    test_requests='50000000',
)
    local
        server_threads_int = std.parseInt(server_threads),
        benchmark_threads_int = std.parseInt(benchmark_threads),
        connections_int = std.parseInt(connections),
        pipeline_depth_int = std.parseInt(pipeline_depth),
        key_count_int = std.parseInt(key_count),
        value_length_int = std.parseInt(value_length),
        get_percent_int = std.parseInt(get_percent),
        hashtable_power_int = std.parseInt(hashtable_power),
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
        };

    {
        name: 'crucible_vbench_' + cache_backend + '_c' + connections + '_v' + value_length + '_r' + get_percent,
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
                            # Increase TCP backlog
                            sudo sysctl -w net.core.somaxconn=65535
                            sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65535

                            # Disable THP (for fair comparison with Valkey)
                            echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled || true

                            # Network tuning
                            sudo ethtool -L ens34 combined 8 || true
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

                            echo "Crucible server started with PID $(cat pid)"
                        |||,
                        background=true
                    ),

                    // Give the server a moment to start up
                    systemslab.bash('sleep 10'),

                    // Verify the server is listening
                    systemslab.bash(
                        |||
                            nc -z localhost 6379 || { echo "Crucible server failed to start"; exit 1; }
                            echo "Crucible server is ready"
                        |||
                    ),

                    // Signal that the cache is ready
                    systemslab.barrier('cache-start'),

                    // Wait for warmup to complete
                    systemslab.barrier('warmup-complete'),

                    // Verify the server is still running after warmup
                    systemslab.bash(
                        |||
                            nc -z localhost 6379 || { echo "Crucible server died during warmup"; exit 1; }
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
                host: {
                    tags: ['c8g.8xl'],
                },

                steps: [
                    // Install valkey-benchmark from source
                    systemslab.bash(
                        |||
                            export HOME=/tmp/valkey-build
                            mkdir -p $HOME
                            cd $HOME

                            if [ ! -f valkey-benchmark ]; then
                                echo "Downloading and building Valkey %(version)s..."
                                curl -LO https://github.com/valkey-io/valkey/archive/refs/tags/%(version)s.tar.gz
                                tar xzf %(version)s.tar.gz
                                cd valkey-%(version)s
                                make -j$(nproc) BUILD_TLS=no
                                cp src/valkey-benchmark $HOME/
                                cd $HOME
                            fi

                            echo "valkey-benchmark ready"
                            $HOME/valkey-benchmark --version
                        ||| % { version: valkey_version }
                    ),

                    // Wait for the cache to start
                    systemslab.barrier('cache-start'),

                    // Run warmup workload (write-heavy to populate cache)
                    systemslab.bash(
                        |||
                            export HOME=/tmp/valkey-build

                            ulimit -n 500000
                            sudo prlimit --memlock=unlimited --pid $$
                            ulimit -a

                            echo "=== Running warmup (SET-heavy to populate cache) ==="
                            %(cpu_affinity_cmd)s $HOME/valkey-benchmark \
                                -h 172.31.31.218 \
                                -p 6379 \
                                -c %(connections)s \
                                -n %(warmup_requests)s \
                                -d %(value_length)s \
                                -r %(key_count)s \
                                -P %(pipeline_depth)s \
                                --threads %(threads)s \
                                -t set \
                                -q

                            echo "Warmup complete"
                        ||| % {
                            connections: connections,
                            warmup_requests: warmup_requests,
                            value_length: value_length,
                            key_count: key_count,
                            pipeline_depth: pipeline_depth,
                            threads: benchmark_threads,
                            cpu_affinity_cmd: if benchmark_cpu_affinity != '' then 'taskset -c ' + benchmark_cpu_affinity else '',
                        }
                    ),

                    // Wait for connections to clean up
                    systemslab.bash('sleep 5'),

                    // Signal warmup is complete
                    systemslab.barrier('warmup-complete'),

                    // Run the GET benchmark
                    systemslab.bash(
                        |||
                            export HOME=/tmp/valkey-build

                            ulimit -n 500000
                            sudo prlimit --memlock=unlimited --pid $$
                            ulimit -a

                            echo "=== Running GET benchmark ==="
                            %(cpu_affinity_cmd)s $HOME/valkey-benchmark \
                                -h 172.31.31.218 \
                                -p 6379 \
                                -c %(connections)s \
                                -n %(test_requests)s \
                                -d %(value_length)s \
                                -r %(key_count)s \
                                -P %(pipeline_depth)s \
                                --threads %(threads)s \
                                -t get
                        ||| % {
                            connections: connections,
                            test_requests: test_requests,
                            value_length: value_length,
                            key_count: key_count,
                            pipeline_depth: pipeline_depth,
                            threads: benchmark_threads,
                            cpu_affinity_cmd: if benchmark_cpu_affinity != '' then 'taskset -c ' + benchmark_cpu_affinity else '',
                        }
                    ),

                    // Signal that the test is complete
                    systemslab.barrier('test-finish'),
                ],
            },
        },
    }
