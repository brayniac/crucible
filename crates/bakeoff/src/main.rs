use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod context;
mod results;
mod runner;
mod sweep;

use context::ContextManager;
use runner::{RunResult, Runner};
use sweep::{Experiment, IoExperiment, NativeExperiment, NativeSweepConfig, Suite, SweepConfig};

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

    /// Retry failed experiments
    Retry {
        /// Only show what would be retried, don't actually retry
        #[arg(long)]
        dry_run: bool,

        /// Only clear failed experiments from tracking, don't resubmit
        #[arg(long)]
        clear_only: bool,
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
                    Suite::IoNative => "native io-engine comparison",
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
                Suite::IoNative => {
                    for exp in NativeExperiment::all() {
                        let exp_name = exp.name();
                        if !force && ctx_manager.get_by_name(&exp_name).is_some() {
                            println!("  {} already has context, skipping", exp_name);
                            continue;
                        }

                        // Clear submitted tracking for this experiment when re-initializing
                        if force {
                            ctx_manager.clear_submitted_for_name(&exp_name);
                        }

                        let name = format!("bakeoff-native-{}-{}", exp_name, timestamp);
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
                    Suite::IoNative => "Native IO Engine Suite",
                }
            );

            // Handle IoNative separately since it uses different sweep config
            if cli.suite == Suite::IoNative {
                let sweep = NativeSweepConfig::full();

                for exp in NativeExperiment::all() {
                    let name = exp.name();
                    match ctx_manager.get_by_name(&name) {
                        Some(uuid) => println!("  {:<20} {}", name, uuid),
                        None => println!("  {:<20} (not initialized)", name),
                    }
                }

                println!("\nSubmitted Experiments:\n");
                let mut total_submitted = 0;
                for exp in NativeExperiment::all() {
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
                    NativeExperiment::all().len() * sweep.total_combinations()
                );
                return Ok(());
            }

            let sweep = match cli.suite {
                Suite::Server => SweepConfig::full(),
                Suite::IoEngine => SweepConfig::io_engine(),
                Suite::IoNative => unreachable!(), // handled above
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
                Suite::IoNative => {
                    // Already handled and returned early above
                    unreachable!()
                }
            }
        }

        Commands::Run {
            dry_run,
            limited,
            force,
        } => {
            ctx_manager.load()?;

            let runner = Runner::new(&cli.experiments_dir, dry_run, force);

            // Handle IoNative separately since it uses NativeSweepConfig
            if cli.suite == Suite::IoNative {
                let sweep = if limited {
                    NativeSweepConfig::limited()
                } else {
                    NativeSweepConfig::full()
                };

                let experiments = NativeExperiment::all();

                println!(
                    "Running {} native io experiments with {} parameter combinations{}",
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
                                "No context for {}. Run 'bakeoff -s io-native init' first.",
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

                        let (name, result) = runner.run_native(
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

                return Ok(());
            }

            let sweep = match cli.suite {
                Suite::Server if limited => SweepConfig::limited(),
                Suite::Server => SweepConfig::full(),
                Suite::IoEngine if limited => SweepConfig::limited(),
                Suite::IoEngine => SweepConfig::io_engine(),
                Suite::IoNative => unreachable!(), // handled above
            };

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
                Suite::IoNative => {
                    // Already handled and returned early above
                    unreachable!()
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
                Suite::IoNative => {
                    for exp in NativeExperiment::all() {
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

                        match results::fetch_native_context_results(context_id) {
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
                        Suite::IoNative => {
                            NativeExperiment::all().iter().map(|e| e.name()).collect()
                        }
                    };
                    for exp_name in &experiments {
                        if let Some(exp_results) = bakeoff_results.experiments.get(exp_name) {
                            results::print_results_table(exp_name, exp_results);
                        }
                    }
                }
            }
        }

        Commands::Retry {
            dry_run,
            clear_only,
        } => {
            ctx_manager.load()?;

            println!(
                "Checking for failed experiments in {} suite{}...\n",
                match cli.suite {
                    Suite::Server => "server",
                    Suite::IoEngine => "io-engine",
                    Suite::IoNative => "native io-engine",
                },
                if dry_run { " (DRY RUN)" } else { "" }
            );

            let mut total_failed = 0;
            let mut total_cleared = 0;
            let mut failed_experiments: Vec<(String, String, usize, usize)> = Vec::new(); // (exp_name, label, conns, pipe)

            // Collect all failed experiments
            match cli.suite {
                Suite::Server => {
                    for exp in Experiment::all() {
                        let context_id = match ctx_manager.get_by_name(exp.name()) {
                            Some(id) => id,
                            None => {
                                println!("  {}: no context initialized, skipping", exp.name());
                                continue;
                            }
                        };

                        match results::fetch_failed_experiments(context_id, exp.name()) {
                            Ok(failed) => {
                                if !failed.is_empty() {
                                    println!(
                                        "  {}: {} failed experiments",
                                        exp.name(),
                                        failed.len()
                                    );
                                    for f in &failed {
                                        println!("    - c{}/p{}", f.connections, f.pipeline_depth);
                                        failed_experiments.push((
                                            exp.name().to_string(),
                                            f.label.clone(),
                                            f.connections,
                                            f.pipeline_depth,
                                        ));
                                    }
                                    total_failed += failed.len();
                                } else {
                                    println!("  {}: no failures", exp.name());
                                }
                            }
                            Err(e) => {
                                eprintln!("  {}: error fetching results: {}", exp.name(), e);
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
                                println!("  {}: no context initialized, skipping", exp_name);
                                continue;
                            }
                        };

                        match results::fetch_failed_experiments(context_id, &exp_name) {
                            Ok(failed) => {
                                if !failed.is_empty() {
                                    println!("  {}: {} failed experiments", exp_name, failed.len());
                                    for f in &failed {
                                        println!("    - c{}/p{}", f.connections, f.pipeline_depth);
                                        failed_experiments.push((
                                            exp_name.clone(),
                                            f.label.clone(),
                                            f.connections,
                                            f.pipeline_depth,
                                        ));
                                    }
                                    total_failed += failed.len();
                                } else {
                                    println!("  {}: no failures", exp_name);
                                }
                            }
                            Err(e) => {
                                eprintln!("  {}: error fetching results: {}", exp_name, e);
                            }
                        }
                    }
                }
                Suite::IoNative => {
                    // IoNative retry not yet implemented - requires value_size tracking
                    println!("  Retry not yet implemented for io-native suite.");
                    println!(
                        "  Use 'bakeoff -s io-native run --force' to resubmit all experiments."
                    );
                    return Ok(());
                }
            }

            if total_failed == 0 {
                println!("\nNo failed experiments found.");
                return Ok(());
            }

            println!("\nTotal failed: {}", total_failed);

            if dry_run {
                println!("\nDry run - no changes made.");
                return Ok(());
            }

            // Clear failed experiments from submitted tracking
            println!("\nClearing failed experiments from tracking...");
            for (_, label, _, _) in &failed_experiments {
                if ctx_manager.clear_submitted(label) {
                    total_cleared += 1;
                }
            }
            ctx_manager.save()?;
            println!(
                "  Cleared {} experiments from submitted tracking",
                total_cleared
            );

            if clear_only {
                println!("\nCleared tracking only. Run 'bakeoff run' to resubmit.");
                return Ok(());
            }

            // Resubmit failed experiments
            println!("\nResubmitting failed experiments...");

            let runner = Runner::new(&cli.experiments_dir, false, true);
            let mut submitted_count = 0;

            match cli.suite {
                Suite::Server => {
                    for (exp_name, _, connections, pipeline_depth) in &failed_experiments {
                        let exp = Experiment::all()
                            .into_iter()
                            .find(|e| e.name() == exp_name)
                            .unwrap();

                        let context_id = ctx_manager.get_by_name(exp.name()).unwrap().clone();
                        let params = sweep::SweepParams {
                            connections: *connections,
                            pipeline_depth: *pipeline_depth,
                        };

                        let (name, result) = runner.run(
                            exp.name(),
                            exp.jsonnet_file(),
                            &context_id,
                            &params,
                            &[],
                            false, // not already submitted (we just cleared it)
                        )?;

                        match result {
                            RunResult::Submitted => {
                                println!("  [OK] {}", name);
                                ctx_manager.mark_submitted(name);
                                ctx_manager.save()?;
                                submitted_count += 1;
                            }
                            RunResult::Skipped => {
                                println!("  [SKIP] {}", name);
                            }
                            RunResult::DryRun => {}
                        }
                    }
                }
                Suite::IoEngine => {
                    for (exp_name, _, connections, pipeline_depth) in &failed_experiments {
                        let exp = IoExperiment::all()
                            .into_iter()
                            .find(|e| e.name() == *exp_name)
                            .unwrap();

                        let context_id = ctx_manager.get_by_name(&exp.name()).unwrap().clone();
                        let params = sweep::SweepParams {
                            connections: *connections,
                            pipeline_depth: *pipeline_depth,
                        };
                        let extra_args = exp.extra_args();

                        let (name, result) = runner.run(
                            &exp.name(),
                            exp.jsonnet_file(),
                            &context_id,
                            &params,
                            &extra_args,
                            false,
                        )?;

                        match result {
                            RunResult::Submitted => {
                                println!("  [OK] {}", name);
                                ctx_manager.mark_submitted(name);
                                ctx_manager.save()?;
                                submitted_count += 1;
                            }
                            RunResult::Skipped => {
                                println!("  [SKIP] {}", name);
                            }
                            RunResult::DryRun => {}
                        }
                    }
                }
                Suite::IoNative => {
                    // Already returned early above
                    unreachable!()
                }
            }

            println!(
                "\nRetry complete: {} experiments resubmitted",
                submitted_count
            );
        }
    }

    Ok(())
}
