use clap::{Parser, Subcommand};
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(name = "xtask")]
#[command(about = "Development tasks for the crucible workspace")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run all fuzz tests in the workspace
    FuzzAll {
        /// How long to run each fuzz target in seconds
        #[arg(short, long, default_value = "60")]
        duration: u64,

        /// Number of fuzz targets to run in parallel
        #[arg(short, long, default_value = "1")]
        jobs: usize,

        /// Number of libFuzzer workers per target (fork mode)
        #[arg(short, long, default_value = "1")]
        fork: u32,
    },

    /// Generate a flamegraph of the server under load
    Flamegraph {
        /// Duration to record in seconds
        #[arg(short, long, default_value = "10")]
        duration: u64,

        /// Output file for the flamegraph SVG
        #[arg(short, long, default_value = "flamegraph.svg")]
        output: PathBuf,

        /// Server config file (uses default if not specified)
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Benchmark config file (uses default if not specified)
        #[arg(short, long)]
        bench_config: Option<PathBuf>,

        /// Skip running the benchmark (just profile idle server)
        #[arg(long)]
        no_load: bool,

        /// Server runtime: "native" or "tokio"
        #[arg(long, default_value = "native")]
        runtime: String,
    },
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::FuzzAll {
            duration,
            jobs,
            fork,
        } => fuzz_all(duration, jobs, fork),
        Commands::Flamegraph {
            duration,
            output,
            config,
            bench_config,
            no_load,
            runtime,
        } => flamegraph(duration, output, config, bench_config, no_load, runtime),
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

#[derive(Debug)]
struct FuzzTarget {
    /// Relative path to fuzz directory (e.g., "cache/core/fuzz")
    fuzz_dir: PathBuf,
    /// Target name (e.g., "fuzz_basic_header")
    name: String,
}

impl FuzzTarget {
    fn display_name(&self) -> String {
        format!("{}:{}", self.fuzz_dir.display(), self.name)
    }
}

#[derive(Debug)]
enum FuzzResult {
    Pass { elapsed: Duration },
    Fail { elapsed: Duration, reason: String },
}

fn discover_fuzz_targets(workspace_root: &Path) -> Result<Vec<FuzzTarget>, String> {
    let mut targets = Vec::new();

    // Find all fuzz/Cargo.toml files
    fn find_fuzz_dirs(dir: &Path, fuzz_dirs: &mut Vec<PathBuf>) {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if path.file_name().map(|n| n == "fuzz").unwrap_or(false) {
                        if path.join("Cargo.toml").exists() {
                            fuzz_dirs.push(path);
                        }
                    } else if path.file_name().map(|n| n != "target").unwrap_or(true) {
                        find_fuzz_dirs(&path, fuzz_dirs);
                    }
                }
            }
        }
    }

    let mut fuzz_dirs = Vec::new();
    find_fuzz_dirs(workspace_root, &mut fuzz_dirs);
    fuzz_dirs.sort();

    for fuzz_dir in fuzz_dirs {
        let targets_dir = fuzz_dir.join("fuzz_targets");
        if !targets_dir.exists() {
            continue;
        }

        if let Ok(entries) = fs::read_dir(&targets_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map(|e| e == "rs").unwrap_or(false)
                    && let Some(stem) = path.file_stem()
                {
                    let rel_path = fuzz_dir
                        .strip_prefix(workspace_root)
                        .unwrap_or(&fuzz_dir)
                        .to_path_buf();
                    targets.push(FuzzTarget {
                        fuzz_dir: rel_path,
                        name: stem.to_string_lossy().to_string(),
                    });
                }
            }
        }
    }

    // Sort for deterministic ordering
    targets.sort_by_key(|t| t.display_name());

    Ok(targets)
}

fn run_fuzz_target(
    workspace_root: &Path,
    target: &FuzzTarget,
    duration: u64,
    fork: u32,
) -> FuzzResult {
    let fuzz_dir = workspace_root.join(&target.fuzz_dir);
    let start = Instant::now();

    let mut cmd = Command::new("cargo");
    cmd.arg("+nightly")
        .arg("fuzz")
        .arg("run")
        .arg(&target.name)
        .arg("--")
        .arg(format!("-max_total_time={duration}"));

    if fork > 1 {
        cmd.arg(format!("-fork={fork}"));
    }

    cmd.current_dir(&fuzz_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            return FuzzResult::Fail {
                elapsed: start.elapsed(),
                reason: format!("failed to spawn: {e}"),
            };
        }
    };

    let output = match child.wait_with_output() {
        Ok(output) => output,
        Err(e) => {
            return FuzzResult::Fail {
                elapsed: start.elapsed(),
                reason: format!("failed to wait: {e}"),
            };
        }
    };

    let elapsed = start.elapsed();
    let exit_code = output.status.code().unwrap_or(-1);

    // Check exit code first
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let last_lines: Vec<&str> = stderr.lines().rev().take(10).collect();
        let context: String = last_lines.into_iter().rev().collect::<Vec<_>>().join("\n");
        return FuzzResult::Fail {
            elapsed,
            reason: format!("exit code {exit_code}\n{context}"),
        };
    }

    // Check if it ran for the expected duration (80% minimum)
    let min_duration = Duration::from_secs(duration * 80 / 100);
    if elapsed < min_duration {
        return FuzzResult::Fail {
            elapsed,
            reason: format!(
                "exit code {exit_code}, exited early: {:.1}s < {:.1}s minimum",
                elapsed.as_secs_f64(),
                min_duration.as_secs_f64()
            ),
        };
    }

    FuzzResult::Pass { elapsed }
}

fn fuzz_all(duration: u64, jobs: usize, fork: u32) -> Result<(), String> {
    let workspace_root = find_workspace_root()?;

    println!("Running all fuzz tests for {duration}s each (jobs={jobs}, fork={fork})");
    println!("=============================================");
    println!();

    let targets = discover_fuzz_targets(&workspace_root)?;

    if targets.is_empty() {
        return Err("No fuzz targets found".to_string());
    }

    let total = targets.len();
    println!("Found {total} fuzz targets");
    println!();

    // Work queue: targets waiting to be processed
    let work_queue: Arc<Mutex<Vec<FuzzTarget>>> = Arc::new(Mutex::new(targets));

    // Results channel
    let (result_tx, result_rx) = mpsc::channel();

    // Spawn worker threads
    let handles: Vec<_> = (0..jobs)
        .map(|_| {
            let queue = Arc::clone(&work_queue);
            let tx = result_tx.clone();
            let workspace = workspace_root.clone();

            thread::spawn(move || {
                loop {
                    // Get next work item
                    let target = {
                        let mut q = queue.lock().unwrap();
                        q.pop()
                    };

                    let target = match target {
                        Some(t) => t,
                        None => break, // No more work
                    };

                    println!("Starting: {}", target.display_name());

                    let result = run_fuzz_target(&workspace, &target, duration, fork);

                    match &result {
                        FuzzResult::Pass { elapsed } => {
                            println!(
                                "[PASS] {} ({:.1}s)",
                                target.display_name(),
                                elapsed.as_secs_f64()
                            );
                        }
                        FuzzResult::Fail { elapsed, reason } => {
                            println!(
                                "[FAIL] {} ({:.1}s)",
                                target.display_name(),
                                elapsed.as_secs_f64()
                            );
                            for line in reason.lines() {
                                println!("       {line}");
                            }
                            println!();
                        }
                    }

                    tx.send((target, result)).unwrap();
                }
            })
        })
        .collect();

    // Drop the sender so result_rx knows when all workers are done
    drop(result_tx);

    // Collect results
    let mut results: BTreeMap<String, (FuzzTarget, FuzzResult)> = BTreeMap::new();
    for (target, result) in result_rx {
        let name = target.display_name();
        results.insert(name, (target, result));
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    // Summary
    let passed = results
        .values()
        .filter(|(_, r)| matches!(r, FuzzResult::Pass { .. }))
        .count();
    let failed = results
        .values()
        .filter(|(_, r)| matches!(r, FuzzResult::Fail { .. }))
        .count();

    println!();
    println!("=============================================");
    println!("Summary: {passed}/{total} passed, {failed} failed");

    if failed > 0 {
        println!();
        println!("Failed targets:");
        for (name, (_, result)) in &results {
            if matches!(result, FuzzResult::Fail { .. }) {
                println!("  - {name}");
            }
        }
        std::process::exit(1);
    }

    Ok(())
}

fn find_workspace_root() -> Result<PathBuf, String> {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version=1", "--no-deps"])
        .output()
        .map_err(|e| format!("failed to run cargo metadata: {e}"))?;

    if !output.status.success() {
        return Err("cargo metadata failed".to_string());
    }

    // Simple JSON parsing - find "workspace_root"
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in BufReader::new(stdout.as_bytes())
        .lines()
        .map_while(Result::ok)
    {
        if let Some(idx) = line.find("\"workspace_root\"") {
            // Extract the path value
            let after = &line[idx..];
            if let Some(start) = after.find(':') {
                let value_part = &after[start + 1..];
                let value_part = value_part.trim();
                if let Some(stripped) = value_part.strip_prefix('"')
                    && let Some(end) = stripped.find('"')
                {
                    let path = &stripped[..end];
                    return Ok(PathBuf::from(path));
                }
            }
        }
    }

    Err("could not find workspace_root in cargo metadata".to_string())
}

// =============================================================================
// Flamegraph command
// =============================================================================

fn flamegraph(
    duration: u64,
    output: PathBuf,
    config: Option<PathBuf>,
    bench_config: Option<PathBuf>,
    no_load: bool,
    runtime: String,
) -> Result<(), String> {
    let workspace_root = find_workspace_root()?;

    // Check for required tools
    check_tool("perf", "Install with: apt install linux-perf (Linux only)")?;
    check_tool(
        "inferno-collapse-perf",
        "Install with: cargo install inferno",
    )?;
    check_tool("inferno-flamegraph", "Install with: cargo install inferno")?;

    println!("Flamegraph generation");
    println!("=====================");
    println!("Duration: {}s", duration);
    println!("Output: {}", output.display());
    println!("Runtime: {}", runtime);
    println!("Load: {}", if no_load { "none" } else { "benchmark" });
    println!();

    // Build release binaries
    println!("Building release binaries...");
    let status = Command::new("cargo")
        .args(["build", "--release", "-p", "server", "-p", "benchmark"])
        .current_dir(&workspace_root)
        .status()
        .map_err(|e| format!("failed to run cargo build: {e}"))?;

    if !status.success() {
        return Err("cargo build failed".to_string());
    }

    // Create a temporary server config if not provided
    let server_config = match config {
        Some(path) => path,
        None => {
            let config_content = format!(
                r#"
runtime = "{runtime}"

[workers]
threads = 4

[cache]
backend = "segcache"
heap_size = "64MB"
segment_size = "1MB"
hashtable_power = 16

[[listener]]
protocol = "resp"
address = "127.0.0.1:6379"

[metrics]
address = "127.0.0.1:9090"

[uring]
sqpoll = false
buffer_count = 1024
buffer_size = 4096
sq_depth = 1024
"#
            );
            let config_path = workspace_root.join("target/flamegraph-server.toml");
            fs::write(&config_path, config_content)
                .map_err(|e| format!("failed to write temp config: {e}"))?;
            config_path
        }
    };

    // Create a temporary benchmark config if not provided
    let benchmark_config = match bench_config {
        Some(path) => path,
        None => {
            let config_content = r#"
[general]
duration = "300s"
warmup = "2s"
threads = 2

[target]
endpoints = ["127.0.0.1:6379"]
protocol = "resp"

[connection]
pool_size = 32
pipeline_depth = 16
connect_timeout = "5s"
request_timeout = "1s"

[workload]

[workload.keyspace]
length = 16
count = 100000
distribution = "uniform"

[workload.commands]
get = 80
set = 20

[workload.values]
length = 64
"#;
            let config_path = workspace_root.join("target/flamegraph-bench.toml");
            fs::write(&config_path, config_content)
                .map_err(|e| format!("failed to write temp config: {e}"))?;
            config_path
        }
    };

    // Start the server
    println!("Starting server...");
    let server_bin = workspace_root.join("target/release/crucible-server");
    let mut server = Command::new(&server_bin)
        .arg(&server_config)
        .current_dir(&workspace_root)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("failed to start server: {e}"))?;

    let server_pid = server.id();
    println!("Server started with PID: {}", server_pid);

    // Wait for server to be ready
    println!("Waiting for server to be ready...");
    if !wait_for_server("127.0.0.1:6379", Duration::from_secs(10)) {
        let _ = server.kill();
        return Err("Server failed to start within timeout".to_string());
    }
    println!("Server is ready");

    // Start benchmark if requested
    let mut benchmark: Option<Child> = None;
    if !no_load {
        println!("Starting benchmark...");
        let bench_bin = workspace_root.join("target/release/crucible-benchmark");
        let child = Command::new(&bench_bin)
            .arg(&benchmark_config)
            .current_dir(&workspace_root)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("failed to start benchmark: {e}"))?;
        println!("Benchmark started with PID: {}", child.id());
        benchmark = Some(child);

        // Give benchmark time to warm up
        println!("Warming up for 3 seconds...");
        thread::sleep(Duration::from_secs(3));
    }

    // Run perf record
    println!("Recording perf data for {}s...", duration);
    let perf_data = workspace_root.join("target/perf.data");

    let perf_status = Command::new("sudo")
        .args([
            "perf",
            "record",
            "--call-graph",
            "dwarf",
            "-p",
            &server_pid.to_string(),
            "-o",
            perf_data.to_str().unwrap(),
            "--",
            "sleep",
            &duration.to_string(),
        ])
        .current_dir(&workspace_root)
        .status()
        .map_err(|e| format!("failed to run perf record: {e}"))?;

    if !perf_status.success() {
        let _ = server.kill();
        if let Some(mut b) = benchmark {
            let _ = b.kill();
        }
        return Err("perf record failed".to_string());
    }

    // Stop benchmark
    if let Some(mut b) = benchmark {
        println!("Stopping benchmark...");
        let _ = b.kill();
        let _ = b.wait();
    }

    // Stop server
    println!("Stopping server...");
    let _ = server.kill();
    let _ = server.wait();

    // Generate flamegraph
    println!("Generating flamegraph...");

    // perf script | inferno-collapse-perf | inferno-flamegraph > output.svg
    let perf_script = Command::new("sudo")
        .args(["perf", "script", "-i", perf_data.to_str().unwrap()])
        .current_dir(&workspace_root)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("failed to run perf script: {e}"))?;

    let collapse = Command::new("inferno-collapse-perf")
        .stdin(perf_script.stdout.unwrap())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("failed to run inferno-collapse-perf: {e}"))?;

    let flamegraph_output = Command::new("inferno-flamegraph")
        .stdin(collapse.stdout.unwrap())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("failed to run inferno-flamegraph: {e}"))?;

    if !flamegraph_output.status.success() {
        return Err("flamegraph generation failed".to_string());
    }

    // Write output
    let output_path = if output.is_absolute() {
        output
    } else {
        workspace_root.join(output)
    };

    fs::write(&output_path, &flamegraph_output.stdout)
        .map_err(|e| format!("failed to write flamegraph: {e}"))?;

    // Clean up perf data
    let _ = Command::new("sudo")
        .args(["rm", "-f", perf_data.to_str().unwrap()])
        .status();

    println!();
    println!("Flamegraph generated: {}", output_path.display());
    println!("Open in a browser to view the interactive flamegraph.");

    Ok(())
}

fn check_tool(name: &str, install_hint: &str) -> Result<(), String> {
    let result = Command::new("which")
        .arg(name)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    match result {
        Ok(status) if status.success() => Ok(()),
        _ => Err(format!(
            "Required tool '{}' not found. {}",
            name, install_hint
        )),
    }
}

fn wait_for_server(addr: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(mut stream) = TcpStream::connect(addr) {
            // Try a simple PING command
            let _ = stream.write_all(b"*1\r\n$4\r\nPING\r\n");
            let _ = stream.flush();
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}
