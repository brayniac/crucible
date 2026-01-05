local systemslab = import 'systemslab.libsonnet';

// Benchmark using valkey-benchmark (native Redis/Valkey benchmark tool)
// for standardized comparison

function(
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
        ];

    {
        name: 'valkey_native_' + valkey_version + '_c' + connections + '_v' + value_length + '_r' + get_percent,
        jobs: {
            server: {
                host: {
                    tags: ['c8g.2xl'],
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
                                cp src/valkey-benchmark $HOME/
                                cd $HOME
                            fi

                            echo "Valkey ready"
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
