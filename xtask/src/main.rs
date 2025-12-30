use clap::{Parser, Subcommand};
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
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
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::FuzzAll {
            duration,
            jobs,
            fork,
        } => fuzz_all(duration, jobs, fork),
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
