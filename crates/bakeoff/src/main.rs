use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod context;
mod results;
mod runner;
mod sweep;

use context::ContextManager;
use runner::{RunResult, Runner};
use sweep::{Experiment, IoExperiment, Suite, SweepConfig};

#[derive(Parser)]
#[command(name = "bakeoff")]
#[command(about = "Experiment driver for cache server benchmarking")]
struct Cli {
    /// Which evaluation suite to run
    #[arg(long, short, default_value = "server")]
    suite: Suite,

    /// Path to the bakeoff experiments directory
    #[arg(long, default_value = "experiments/bakeoff")]
    experiments_dir: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

impl Cli {
    fn contexts_file(&self) -> PathBuf {
        self.experiments_dir.join(self.suite.contexts_file())
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize contexts for all experiment pairings
    Init {
        /// Create new contexts even if they already exist
        #[arg(long)]
        force: bool,
    },

    /// Show current contexts
    Status,

    /// Run experiments with parameter sweep
    Run {
        /// Only print commands, don't execute
        #[arg(long)]
        dry_run: bool,

        /// Run only a subset for testing (2x2 grid)
        #[arg(long)]
        limited: bool,

        /// Force re-submission of already submitted experiments
        #[arg(long)]
        force: bool,
    },

    /// Collect results from completed experiments
    Results {
        /// Output format (table, json, html)
        #[arg(long, default_value = "table")]
        format: String,

        /// Output file for html format (default: stdout)
        #[arg(long, short)]
        output: Option<std::path::PathBuf>,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let contexts_file = cli.contexts_file();
    let mut ctx_manager = ContextManager::new(&contexts_file);

    match cli.command {
        Commands::Init { force } => {
            // Load existing contexts first
            let _ = ctx_manager.load();

            println!(
                "Initializing contexts for {} suite...\n",
                match cli.suite {
                    Suite::Server => "server comparison",
                    Suite::IoEngine => "io-engine comparison",
                }
            );

            // Generate timestamp suffix for unique context names (YYYYMMDD-HHMM)
            let timestamp = chrono::Local::now().format("%Y%m%d-%H%M");

            match cli.suite {
                Suite::Server => {
                    for exp in Experiment::all() {
                        let exp_name = exp.name();
                        if !force && ctx_manager.get_by_name(exp_name).is_some() {
                            println!("  {} already has context, skipping", exp_name);
                            continue;
                        }

                        // Clear submitted tracking for this experiment when re-initializing
                        if force {
                            ctx_manager.clear_submitted_for_name(exp_name);
                        }

                        let name = format!("bakeoff-{}-{}", exp_name, timestamp);
                        println!("  Creating context: {}", name);

                        match context::create_context(&name) {
                            Ok(uuid) => {
                                println!("    -> {}", uuid);
                                ctx_manager.set_by_name(exp_name, uuid);
                            }
                            Err(e) => {
                                eprintln!("    -> ERROR: {}", e);
                            }
                        }
                    }
                }
                Suite::IoEngine => {
                    for exp in IoExperiment::all() {
                        let exp_name = exp.name();
                        if !force && ctx_manager.get_by_name(&exp_name).is_some() {
                            println!("  {} already has context, skipping", exp_name);
                            continue;
                        }

                        // Clear submitted tracking for this experiment when re-initializing
                        if force {
                            ctx_manager.clear_submitted_for_name(&exp_name);
                        }

                        let name = format!("bakeoff-io-{}-{}", exp_name, timestamp);
                        println!("  Creating context: {}", name);

                        match context::create_context(&name) {
                            Ok(uuid) => {
                                println!("    -> {}", uuid);
                                ctx_manager.set_by_name(&exp_name, uuid);
                            }
                            Err(e) => {
                                eprintln!("    -> ERROR: {}", e);
                            }
                        }
                    }
                }
            }

            ctx_manager.save()?;
            println!("\nContexts saved to {:?}", contexts_file);
        }

        Commands::Status => {
            ctx_manager.load()?;

            println!(
                "{} Contexts:\n",
                match cli.suite {
                    Suite::Server => "Server Suite",
                    Suite::IoEngine => "IO Engine Suite",
                }
            );

            let sweep = match cli.suite {
                Suite::Server => SweepConfig::full(),
                Suite::IoEngine => SweepConfig::io_engine(),
            };

            match cli.suite {
                Suite::Server => {
                    for exp in Experiment::all() {
                        let name = exp.name();
                        match ctx_manager.get_by_name(name) {
                            Some(uuid) => println!("  {:<20} {}", name, uuid),
                            None => println!("  {:<20} (not initialized)", name),
                        }
                    }

                    println!("\nSubmitted Experiments:\n");
                    let mut total_submitted = 0;
                    for exp in Experiment::all() {
                        let mut count = 0;
                        for params in sweep.iter() {
                            let name = format!("{}_{}", exp.name(), params.label());
                            if ctx_manager.is_submitted(&name) {
                                count += 1;
                            }
                        }
                        total_submitted += count;
                        println!(
                            "  {:<20} {}/{}",
                            exp.name(),
                            count,
                            sweep.total_combinations()
                        );
                    }
                    println!(
                        "\n  Total: {}/{}",
                        total_submitted,
                        Experiment::all().len() * sweep.total_combinations()
                    );
                }
                Suite::IoEngine => {
                    for exp in IoExperiment::all() {
                        let name = exp.name();
                        match ctx_manager.get_by_name(&name) {
                            Some(uuid) => println!("  {:<20} {}", name, uuid),
                            None => println!("  {:<20} (not initialized)", name),
                        }
                    }

                    println!("\nSubmitted Experiments:\n");
                    let mut total_submitted = 0;
                    for exp in IoExperiment::all() {
                        let mut count = 0;
                        for params in sweep.iter() {
                            let name = format!("{}_{}", exp.name(), params.label());
                            if ctx_manager.is_submitted(&name) {
                                count += 1;
                            }
                        }
                        total_submitted += count;
                        println!(
                            "  {:<20} {}/{}",
                            exp.name(),
                            count,
                            sweep.total_combinations()
                        );
                    }
                    println!(
                        "\n  Total: {}/{}",
                        total_submitted,
                        IoExperiment::all().len() * sweep.total_combinations()
                    );
                }
            }
        }

        Commands::Run {
            dry_run,
            limited,
            force,
        } => {
            ctx_manager.load()?;

            let sweep = match cli.suite {
                Suite::Server if limited => SweepConfig::limited(),
                Suite::Server => SweepConfig::full(),
                Suite::IoEngine if limited => SweepConfig::limited(),
                Suite::IoEngine => SweepConfig::io_engine(),
            };

            let runner = Runner::new(&cli.experiments_dir, dry_run, force);

            match cli.suite {
                Suite::Server => {
                    let experiments = Experiment::all();

                    println!(
                        "Running {} server experiments with {} parameter combinations{}",
                        experiments.len(),
                        sweep.total_combinations(),
                        if dry_run { " (DRY RUN)" } else { "" }
                    );
                    println!();

                    let mut submitted_count = 0;
                    let mut skipped_count = 0;

                    for exp in experiments {
                        let context_id = match ctx_manager.get_by_name(exp.name()) {
                            Some(id) => id.clone(),
                            None if dry_run => "<CONTEXT_ID>".to_string(),
                            None => {
                                return Err(format!(
                                    "No context for {}. Run 'bakeoff init' first.",
                                    exp.name()
                                )
                                .into());
                            }
                        };

                        let display_id = if context_id.len() >= 8 {
                            &context_id[..8]
                        } else {
                            &context_id
                        };
                        println!("=== {} (context: {}) ===", exp.name(), display_id);

                        for params in sweep.iter() {
                            let name = format!("{}_{}", exp.name(), params.label());
                            let already_submitted = ctx_manager.is_submitted(&name);

                            let (name, result) = runner.run(
                                exp.name(),
                                exp.jsonnet_file(),
                                &context_id,
                                &params,
                                &[],
                                already_submitted,
                            )?;

                            match result {
                                RunResult::Submitted => {
                                    ctx_manager.mark_submitted(name);
                                    ctx_manager.save()?;
                                    submitted_count += 1;
                                }
                                RunResult::Skipped => {
                                    skipped_count += 1;
                                }
                                RunResult::DryRun => {}
                            }
                        }

                        println!();
                    }

                    if !dry_run {
                        println!(
                            "Summary: {} submitted, {} skipped",
                            submitted_count, skipped_count
                        );
                    }
                }
                Suite::IoEngine => {
                    let experiments = IoExperiment::all();

                    println!(
                        "Running {} io-engine experiments with {} parameter combinations{}",
                        experiments.len(),
                        sweep.total_combinations(),
                        if dry_run { " (DRY RUN)" } else { "" }
                    );
                    println!();

                    let mut submitted_count = 0;
                    let mut skipped_count = 0;

                    for exp in experiments {
                        let exp_name = exp.name();
                        let context_id = match ctx_manager.get_by_name(&exp_name) {
                            Some(id) => id.clone(),
                            None if dry_run => "<CONTEXT_ID>".to_string(),
                            None => {
                                return Err(format!(
                                    "No context for {}. Run 'bakeoff -s io-engine init' first.",
                                    exp_name
                                )
                                .into());
                            }
                        };

                        let display_id = if context_id.len() >= 8 {
                            &context_id[..8]
                        } else {
                            &context_id
                        };
                        println!("=== {} (context: {}) ===", exp_name, display_id);

                        let extra_args = exp.extra_args();

                        for params in sweep.iter() {
                            let name = format!("{}_{}", exp_name, params.label());
                            let already_submitted = ctx_manager.is_submitted(&name);

                            let (name, result) = runner.run(
                                &exp_name,
                                exp.jsonnet_file(),
                                &context_id,
                                &params,
                                &extra_args,
                                already_submitted,
                            )?;

                            match result {
                                RunResult::Submitted => {
                                    ctx_manager.mark_submitted(name);
                                    ctx_manager.save()?;
                                    submitted_count += 1;
                                }
                                RunResult::Skipped => {
                                    skipped_count += 1;
                                }
                                RunResult::DryRun => {}
                            }
                        }

                        println!();
                    }

                    if !dry_run {
                        println!(
                            "Summary: {} submitted, {} skipped",
                            submitted_count, skipped_count
                        );
                    }
                }
            }
        }

        Commands::Results { format, output } => {
            ctx_manager.load()?;

            // Collect all results
            let mut all_results = std::collections::HashMap::new();

            match cli.suite {
                Suite::Server => {
                    for exp in Experiment::all() {
                        let context_id = match ctx_manager.get_by_name(exp.name()) {
                            Some(id) => id,
                            None => {
                                if format == "table" {
                                    println!("{}: no context initialized\n", exp.name());
                                }
                                continue;
                            }
                        };

                        match results::fetch_context_results(context_id) {
                            Ok(exp_results) => {
                                all_results.insert(exp.name().to_string(), exp_results);
                            }
                            Err(e) => {
                                if format == "table" {
                                    println!("{}: error fetching results: {}\n", exp.name(), e);
                                } else {
                                    eprintln!("{}: error fetching results: {}", exp.name(), e);
                                }
                            }
                        }
                    }
                }
                Suite::IoEngine => {
                    for exp in IoExperiment::all() {
                        let exp_name = exp.name();
                        let context_id = match ctx_manager.get_by_name(&exp_name) {
                            Some(id) => id,
                            None => {
                                if format == "table" {
                                    println!("{}: no context initialized\n", exp_name);
                                }
                                continue;
                            }
                        };

                        match results::fetch_context_results(context_id) {
                            Ok(exp_results) => {
                                all_results.insert(exp_name, exp_results);
                            }
                            Err(e) => {
                                if format == "table" {
                                    println!("{}: error fetching results: {}\n", exp_name, e);
                                } else {
                                    eprintln!("{}: error fetching results: {}", exp_name, e);
                                }
                            }
                        }
                    }
                }
            }

            let bakeoff_results = results::BakeoffResults {
                experiments: all_results,
            };

            match format.as_str() {
                "json" => {
                    results::print_results_json(&bakeoff_results);
                }
                "html" => {
                    let html = results::generate_html_report(&bakeoff_results);
                    if let Some(path) = output {
                        std::fs::write(&path, &html)?;
                        println!("HTML report written to {:?}", path);
                    } else {
                        println!("{}", html);
                    }
                }
                _ => {
                    let experiments: Vec<String> = match cli.suite {
                        Suite::Server => Experiment::all()
                            .iter()
                            .map(|e| e.name().to_string())
                            .collect(),
                        Suite::IoEngine => IoExperiment::all().iter().map(|e| e.name()).collect(),
                    };
                    for exp_name in &experiments {
                        if let Some(exp_results) = bakeoff_results.experiments.get(exp_name) {
                            results::print_results_table(exp_name, exp_results);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
