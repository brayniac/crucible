use consumer::forced_string;
use std::{fs, path, process};

// save the cache directories
pub fn save(build: &path::Path, cache: &path::Path) -> Result<(), ()> {
    info!("cache save: start");

    let paths = vec![
        "target/debug",
        "target/release",
        "target/benchcmp/target",
        "fuzz/target",
        "fuzz/corpus",
    ];

    for path in &paths {
        let _ = rsync(path, build, cache);
    }

    Ok(())
}

// load the cache into the build dir
pub fn load(build: &path::Path, cache: &path::Path) -> Result<(), ()> {
    info!("cache load: start");

    let paths = vec![
        "target/debug",
        "target/release",
        "target/benchcmp/target",
        "fuzz/target",
        "fuzz/corpus",
    ];

    for path in &paths {
        let _ = rsync(path, cache, build);
    }

    Ok(())
}

// call rsync for subfolder path
fn rsync(path: &str, source: &path::Path, destination: &path::Path) -> Result<(), ()> {
    let mut dst = destination.to_path_buf();
    dst.push(path);
    let _ = fs::create_dir_all(dst.as_path());
    dst.pop();
    if let Ok(output) = process::Command::new("rsync")
        .arg("-aPc")
        .arg("--delete")
        .arg(path)
        .arg(dst.as_path().to_str().unwrap())
        .current_dir(source)
        .output()
    {
        debug!("stdout:\n{}", forced_string(output.stdout));
        debug!("stderr:\n{}", forced_string(output.stderr));

        if output.status.success() {
            info!("rsync {}: ok", path);
            Ok(())
        } else {
            info!("rsync {}: fail", path);
            Err(())
        }
    } else {
        error!("failed calling rsync");
        Err(())
    }
}
