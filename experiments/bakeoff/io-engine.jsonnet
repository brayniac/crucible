local systemslab = import 'systemslab.libsonnet';

// IO Engine comparison experiment
// Tests different combinations of server and client IO engines

local server_config = {
    runtime: 'native',

    workers: {
        threads: error 'threads must be specified',
    },

    cache: {
        backend: 'segcache',
        heap_size: error 'heap_size must be specified',
        segment_size: '1MB',
        hashtable_power: 20,
        hugepage: 'none',
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
    heap_size='8GB',
    server_threads='8',
    server_cpu_affinity='0-7',
    server_io_engine='auto',  // auto, mio, or uring

    // Benchmark parameters
    benchmark_threads='24',
    benchmark_cpu_affinity='8-32',
    client_io_engine='auto',  // auto, mio, or uring
    connections='256',
    pipeline_depth='64',
    key_length='20',
    key_count='1000000',
    value_length='64',
    get_percent='80',
    warmup_duration='30s',
    test_duration='60s',
)
    local
        server_threads_int = std.parseInt(server_threads),
        benchmark_threads_int = std.parseInt(benchmark_threads),
        connections_int = std.parseInt(connections),
        pipeline_depth_int = std.parseInt(pipeline_depth),
        key_length_int = std.parseInt(key_length),
        key_count_int = std.parseInt(key_count),
        value_length_int = std.parseInt(value_length),
        get_percent_int = std.parseInt(get_percent),
        set_percent_int = 100 - get_percent_int;

    assert get_percent_int >= 0 && get_percent_int <= 100 : 'get_percent must be between 0 and 100';
    assert server_io_engine == 'auto' || server_io_engine == 'mio' || server_io_engine == 'uring' : 'server_io_engine must be auto, mio, or uring';
    assert client_io_engine == 'auto' || client_io_engine == 'mio' || client_io_engine == 'uring' : 'client_io_engine must be auto, mio, or uring';

    local
        cache_config = server_config {
            io_engine: server_io_engine,
            workers+: {
                threads: server_threads_int,
                [if server_cpu_affinity != '' then 'cpu_affinity']: server_cpu_affinity,
            },
            cache+: {
                heap_size: heap_size,
            },
        },

        warmup_benchmark_config = benchmark_config {
            general+: {
                duration: warmup_duration,
                warmup: '0s',
                threads: benchmark_threads_int,
                io_engine: client_io_engine,
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

        test_benchmark_config = benchmark_config {
            general+: {
                duration: test_duration,
                warmup: '0s',
                threads: benchmark_threads_int,
                io_engine: client_io_engine,
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
                    get: 100,
                    set: 0,
                },
                values+: {
                    length: value_length_int,
                },
            },
        };

    {
        name: 'io_engine_s' + server_io_engine + '_c' + client_io_engine + '_c' + connections + '_p' + pipeline_depth,
        jobs: {
            server: {
                local config = std.manifestTomlEx(cache_config, ''),

                host: {
                    tags: ['c8g.2xl'],
                },

                steps: [
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build
                            mkdir -p $HOME

                            if [ ! -f $HOME/.cargo/bin/cargo ]; then
                                curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
                            fi
                        |||
                    ),

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

                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build
                            source $HOME/.cargo/env

                            cd $HOME/crucible
                            cargo build --release -p server
                        |||
                    ),

                    systemslab.write_file('server.toml', config),

                    systemslab.bash(
                        |||
                            sudo sysctl -w net.core.somaxconn=65535
                            sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65535
                            echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled || true
                            sudo ethtool -L ens34 combined 8 || true
                        |||
                    ),

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

                    systemslab.bash('sleep 10'),

                    systemslab.bash(
                        |||
                            nc -z localhost 6379 || { echo "Cache server failed to start"; exit 1; }
                        |||
                    ),

                    systemslab.barrier('cache-start'),
                    systemslab.barrier('warmup-complete'),

                    systemslab.bash(
                        |||
                            nc -z localhost 6379 || { echo "Cache server died during warmup"; exit 1; }
                        |||
                    ),

                    systemslab.barrier('test-finish'),

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
                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build
                            mkdir -p $HOME

                            if [ ! -f $HOME/.cargo/bin/cargo ]; then
                                curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
                            fi
                        |||
                    ),

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

                    systemslab.bash(
                        |||
                            export HOME=/tmp/crucible-build
                            source $HOME/.cargo/env

                            cd $HOME/crucible
                            cargo build --release -p benchmark
                        |||
                    ),

                    systemslab.write_file('warmup.toml', warmup),
                    systemslab.write_file('loadgen.toml', loadgen),

                    systemslab.barrier('cache-start'),

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

                    systemslab.bash('sleep 10'),

                    systemslab.barrier('warmup-complete'),

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

                    systemslab.barrier('test-finish'),

                    systemslab.upload_artifact('loadgen.toml', tags=['benchmark-config']),
                ],
            },
        },
    }
