use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Contexts {
    #[serde(default)]
    pub contexts: HashMap<String, String>,
    #[serde(default)]
    pub submitted: HashSet<String>,
}

pub struct ContextManager {
    path: PathBuf,
    contexts: Contexts,
}

impl ContextManager {
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            contexts: Contexts::default(),
        }
    }

    pub fn load(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.path.exists() {
            let contents = fs::read_to_string(&self.path)?;
            self.contexts = serde_json::from_str(&contents)?;
        }
        Ok(())
    }

    pub fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let contents = serde_json::to_string_pretty(&self.contexts)?;
        fs::write(&self.path, contents)?;
        Ok(())
    }

    pub fn get_by_name(&self, name: &str) -> Option<&String> {
        self.contexts.contexts.get(name)
    }

    pub fn set_by_name(&mut self, name: &str, uuid: String) {
        self.contexts.contexts.insert(name.to_string(), uuid);
    }

    pub fn is_submitted(&self, name: &str) -> bool {
        self.contexts.submitted.contains(name)
    }

    pub fn mark_submitted(&mut self, name: String) {
        self.contexts.submitted.insert(name);
    }

    pub fn clear_submitted_for_name(&mut self, name: &str) {
        let prefix = format!("{}_", name);
        self.contexts.submitted.retain(|n| !n.starts_with(&prefix));
    }
}

/// Create a new context via systemslab CLI and return the UUID.
pub fn create_context(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("systemslab")
        .args(["context", "new", "--name", name])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("systemslab context new failed: {}", stderr).into());
    }

    // Parse UUID from output - systemslab outputs a URL like http://systemslab/context/<uuid>
    let stdout = String::from_utf8_lossy(&output.stdout);
    let uuid = stdout
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            // Extract UUID from URL pattern
            if let Some(idx) = line.rfind('/') {
                let potential_uuid = &line[idx + 1..];
                if potential_uuid.len() == 36 && potential_uuid.contains('-') {
                    return Some(potential_uuid.to_string());
                }
            }
            // Or check if line itself is a UUID
            if line.len() == 36 && line.contains('-') && !line.contains('/') {
                return Some(line.to_string());
            }
            None
        })
        .next()
        .ok_or("Could not parse UUID from systemslab output")?;

    Ok(uuid)
}
