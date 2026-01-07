use crate::sweep::{NativeSweepParams, SweepParams};
use std::path::{Path, PathBuf};
use std::process::Command;

pub enum RunResult {
    Submitted,
    Skipped,
    DryRun,
}

pub struct Runner {
    experiments_dir: PathBuf,
    dry_run: bool,
    force: bool,
}

impl Runner {
    pub fn new(experiments_dir: &Path, dry_run: bool, force: bool) -> Self {
        Self {
            experiments_dir: experiments_dir.to_path_buf(),
            dry_run,
            force,
        }
    }

    /// Run an experiment with the given parameters.
    ///
    /// - `exp_name`: The experiment name (used for labeling)
    /// - `jsonnet_file`: The jsonnet spec file name
    /// - `context_id`: The systemslab context ID
    /// - `params`: The sweep parameters
    /// - `extra_args`: Additional CLI args (e.g., for io_engine selection)
    /// - `already_submitted`: Whether this experiment was already submitted
    pub fn run(
        &self,
        exp_name: &str,
        jsonnet_file: &str,
        context_id: &str,
        params: &SweepParams,
        extra_args: &[String],
        already_submitted: bool,
    ) -> Result<(String, RunResult), Box<dyn std::error::Error>> {
        let spec_path = self.experiments_dir.join(jsonnet_file);
        let name = format!("{}_{}", exp_name, params.label());

        if already_submitted && !self.force {
            println!("  [SKIP] {} (already submitted)", name);
            return Ok((name, RunResult::Skipped));
        }

        let mut args = vec![
            "submit".to_string(),
            spec_path.to_string_lossy().to_string(),
            "--context".to_string(),
            context_id.to_string(),
            "--name".to_string(),
            name.clone(),
        ];

        // Add parameter arguments
        for param_arg in params.to_args() {
            args.push(param_arg);
        }

        // Add extra arguments (e.g., io_engine selection)
        for arg in extra_args {
            args.push(arg.clone());
        }

        if self.dry_run {
            println!("  [DRY RUN] systemslab {}", args.join(" "));
            return Ok((name, RunResult::DryRun));
        }

        println!("  Submitting: {} ...", name);

        let output = Command::new("systemslab").args(&args).output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("    ERROR: {}", stderr.trim());
            return Err(format!("systemslab submit failed for {}", name).into());
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        // Try to extract experiment ID from output
        if let Some(line) = stdout
            .lines()
            .find(|l| l.contains("experiment") || l.len() == 36)
        {
            println!("    -> {}", line.trim());
        } else {
            println!("    -> submitted");
        }

        Ok((name, RunResult::Submitted))
    }

    /// Run a native I/O experiment with the given parameters (includes value_size).
    pub fn run_native(
        &self,
        exp_name: &str,
        jsonnet_file: &str,
        context_id: &str,
        params: &NativeSweepParams,
        extra_args: &[String],
        already_submitted: bool,
    ) -> Result<(String, RunResult), Box<dyn std::error::Error>> {
        let spec_path = self.experiments_dir.join(jsonnet_file);
        let name = format!("{}_{}", exp_name, params.label());

        if already_submitted && !self.force {
            println!("  [SKIP] {} (already submitted)", name);
            return Ok((name, RunResult::Skipped));
        }

        let mut args = vec![
            "submit".to_string(),
            spec_path.to_string_lossy().to_string(),
            "--context".to_string(),
            context_id.to_string(),
            "--name".to_string(),
            name.clone(),
        ];

        // Add parameter arguments
        for param_arg in params.to_args() {
            args.push(param_arg);
        }

        // Add extra arguments (e.g., io_engine selection)
        for arg in extra_args {
            args.push(arg.clone());
        }

        if self.dry_run {
            println!("  [DRY RUN] systemslab {}", args.join(" "));
            return Ok((name, RunResult::DryRun));
        }

        println!("  Submitting: {} ...", name);

        let output = Command::new("systemslab").args(&args).output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("    ERROR: {}", stderr.trim());
            return Err(format!("systemslab submit failed for {}", name).into());
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        // Try to extract experiment ID from output
        if let Some(line) = stdout
            .lines()
            .find(|l| l.contains("experiment") || l.len() == 36)
        {
            println!("    -> {}", line.trim());
        } else {
            println!("    -> submitted");
        }

        Ok((name, RunResult::Submitted))
    }
}
