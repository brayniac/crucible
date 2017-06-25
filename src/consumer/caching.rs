use consumer::cargo::Triple;
use consumer::forced_string;
use std::{fs, path, process};

#[derive(Clone)]
pub struct Cache {
    build: path::PathBuf,
    cache: path::PathBuf,
    folders: Vec<String>,
}

impl Cache {
    pub fn new(build: path::PathBuf, cache: path::PathBuf) -> Cache {
        let folders = vec![
            "target/debug",
            "target/release",
            "target/aarch64-unknown-linux-gnu",
            "target/arm-unknown-linux-gnueabi",
            "target/arm-unknown-linux-gnueabihf",
            "target/armv7-unknown-linux-gnueabihf",
            "target/i686-unknown-linux-gnu",
            "target/i686-unknown-linux-musl",
            "target/x86_64-unknown-linux-gnu",
            "target/x86_64-unknwon-linux-musl",
            "target/benchcmp/target",
            "fuzz/target",
            "fuzz/corpus",
        ];
        let folders = folders.iter().map(|&x| x.to_owned()).collect();
        Cache {
            build: build,
            cache: cache,
            folders: folders,
        }
    }

    pub fn save(&self) -> Result<(), ()> {
        save(
            self.build.as_path(),
            self.cache.as_path(),
            self.folders.clone(),
        )
    }

    pub fn load(&self) -> Result<(), ()> {
        load(
            self.build.as_path(),
            self.cache.as_path(),
            self.folders.clone(),
        )
    }

    pub fn set_cache(&mut self, path: path::PathBuf) {
        self.cache = path;
    }

    pub fn set_build(&mut self, path: path::PathBuf) {
        self.build = path;
    }
}

// save the cache directories
fn save(build: &path::Path, cache: &path::Path, folders: Vec<String>) -> Result<(), ()> {
    info!("cache save: start");

    for folder in &folders {
        let _ = rsync(&folder, build, cache);
    }

    Ok(())
}

// load the cache into the build dir
fn load(build: &path::Path, cache: &path::Path, folders: Vec<String>) -> Result<(), ()> {
    info!("cache load: start");

    for folder in &folders {
        let _ = rsync(&folder, cache, build);
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
        trace!("stdout:\n{}", forced_string(output.stdout));
        trace!("stderr:\n{}", forced_string(output.stderr));

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
