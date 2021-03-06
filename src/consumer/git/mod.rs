use consumer::forced_string;
use std::path::Path;
use std::process::Command;

// clone the repo into a build folder within the path given
pub fn clone_repo(path: &Path, name: &str, url: &str) -> Result<(), &'static str> {
    info!("clone repo: {}", name);
    let output = Command::new("git")
        .arg("clone")
        .arg(url)
        .arg("build")
        .current_dir(path)
        .output()
        .expect("failed to run git");

    debug!("stdout:\n{}", forced_string(output.stdout));
    debug!("stderr:\n{}", forced_string(output.stderr));

    if output.status.success() {
        info!("git clone: complete");
        Ok(())
    } else {
        error!("git clone: failed");
        Err("git clone failed")
    }
}

// fetch the pull request with the given number
pub fn fetch_pull(path: &Path, number: &u64) -> Result<(), &'static str> {
    info!("git fetch: pr #{}", number);
    let pr_ref = format!("pull/{}/head:pr-{}", number, number);
    let output = Command::new("git")
        .arg("fetch")
        .arg("origin")
        .arg(pr_ref)
        .current_dir(path)
        .output()
        .expect("failed to run git");

    debug!("stdout:\n{}", forced_string(output.stdout));
    debug!("stderr:\n{}", forced_string(output.stderr));

    if output.status.success() {
        info!("git fetch: pr #{}: complete", number);
        Ok(())
    } else {
        info!("git fetch: pr #{}: failed", number);
        Err("git fetch failed to sync the ref")
    }
}

// checkout the given pr after fetch
pub fn checkout_pr(path: &Path, number: &u64) -> Result<(), &'static str> {
    info!("git checkout: pr #{}", number);
    let branch = format!("pr-{}", number);
    let output = Command::new("git")
        .arg("checkout")
        .arg(branch)
        .current_dir(path)
        .output()
        .expect("failed to run git");

    debug!("stdout:\n{}", forced_string(output.stdout));
    debug!("stderr:\n{}", forced_string(output.stderr));

    if output.status.success() {
        info!("git checkout: pr #{}: complete", number);
        Ok(())
    } else {
        info!("git checkout: pr #{}: failed", number);
        Err("git checkout failed")
    }
}

// checkout the given sha
pub fn checkout_sha(path: &Path, sha: &str) -> Result<(), &'static str> {
    info!("git checkout: sha {}", sha);
    let output = Command::new("git")
        .arg("checkout")
        .arg(sha)
        .current_dir(path)
        .output()
        .expect("failed to run git");

    debug!("stdout:\n{}", forced_string(output.stdout));
    debug!("stderr:\n{}", forced_string(output.stderr));

    if output.status.success() {
        info!("git checkout: sha {}: complete", sha);
        Ok(())
    } else {
        info!("git checkout: sha {}: failed", sha);
        Err("git checkout failed")
    }
}
