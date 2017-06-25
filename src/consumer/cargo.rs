use consumer::caching::Cache;
use consumer::forced_string;
use regex::Regex;
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Channel {
    Stable,
    Beta,
    Nightly,
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Channel::Stable => write!(f, "stable"),
            Channel::Beta => write!(f, "beta"),
            Channel::Nightly => write!(f, "nightly"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Triple {
    Native,
    Aarch64LinuxGnu,
    ArmLinuxGnueabi,
    ArmLinuxGnueabihf,
    Armv7LinuxGnueabihf,
    I686LinuxGnu,
    I686LinuxMusl,
    X86_64LinuxGnu,
    X86_64LinuxMusl,
}

impl fmt::Display for Triple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Triple::Native => write!(f, "native"),
            Triple::Aarch64LinuxGnu => write!(f, "aarch64-unknown-linux-gnu"),
            Triple::ArmLinuxGnueabi => write!(f, "arm-unknown-linux-gnueabi"),
            Triple::ArmLinuxGnueabihf => write!(f, "arm-unknown-linux-gnueabihf"),
            Triple::Armv7LinuxGnueabihf => write!(f, "armv7-unknown-linux-gnueabihf"),
            Triple::I686LinuxGnu => write!(f, "i686-unknown-linux-gnu"),
            Triple::I686LinuxMusl => write!(f, "i686-unknown-linux-musl"),
            Triple::X86_64LinuxGnu => write!(f, "x86_64-unknown-linux-gnu"),
            Triple::X86_64LinuxMusl => write!(f, "x86_64-unknown-linux-musl"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    Debug,
    Release,
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Profile::Debug => write!(f, "debug"),
            Profile::Release => write!(f, "release"),
        }
    }
}

pub struct Cargo {
    cache: Option<Cache>,
    cache_base: Option<PathBuf>,
    dirty_cache: bool,
    channel: Channel,
    triple: Triple,
    profile: Profile,
    path: PathBuf,
    release: bool,
    fuzz_seconds: usize,
    fuzz_cores: usize,
    fuzz_len: usize,
}

impl Cargo {
    pub fn new(path: PathBuf) -> Cargo {
        Cargo {
            cache: None,
            dirty_cache: false,
            cache_base: None,
            channel: Channel::Stable,
            triple: Triple::Native,
            profile: Profile::Debug,
            path: path,
            release: false,
            fuzz_seconds: 60,
            fuzz_cores: 1,
            fuzz_len: 64,
        }
    }

    fn label_maker(&self) -> String {
        format!(
            "cargo ({}-{}) [{}]:",
            self.channel,
            self.triple,
            self.profile
        )
    }

    pub fn build(&mut self) -> Result<(), ()> {
        self.dirty_cache = true;
        info!("{} build: starting", self.label_maker());
        let mut command = Command::new("cargo");
        command.arg("build").arg("--verbose");
        if self.profile == Profile::Release {
            command.arg("--release");
        }
        if self.triple != Triple::Native {
            command.arg("--target");
            command.arg(format!("{}", self.triple));
        }
        let output = command.current_dir(self.path.as_path()).output().expect(
            "failed to run cargo build",
        );

        debug!(
            "{} build: stdout:\n{}",
            self.label_maker(),
            forced_string(output.stdout)
        );
        debug!(
            "{} build: stderr:\n{}",
            self.label_maker(),
            forced_string(output.stderr)
        );

        if output.status.success() {
            info!("{} build: passed", self.label_maker());
            Ok(())
        } else {
            info!("{} build: failed", self.label_maker());
            Err(())
        }
    }

    pub fn clean(&mut self) -> Result<(), ()> {
        self.dirty_cache = false;
        clean(Path::new(&self.path))
    }

    pub fn clippy(&mut self) -> Result<(), ()> {
        self.dirty_cache = true;
        clippy(Path::new(&self.path))
    }

    pub fn test(&mut self) -> Result<(), ()> {
        self.dirty_cache = true;
        info!("{} test: starting", self.label_maker());
        let mut command = Command::new("cargo");
        command.arg("test").arg("--verbose");
        if self.profile == Profile::Release {
            command.arg("--release");
        }
        if self.triple != Triple::Native {
            command.arg("--target");
            command.arg(format!("{}", self.triple));
        }
        let output = command.current_dir(self.path.as_path()).output().expect(
            "failed to run cargo test",
        );

        debug!(
            "{} test: stdout:\n{}",
            self.label_maker(),
            forced_string(output.stdout)
        );
        debug!(
            "{} test: stderr:\n{}",
            self.label_maker(),
            forced_string(output.stderr)
        );

        if output.status.success() {
            info!("{} test: passed", self.label_maker());
            Ok(())
        } else {
            info!("{} test: failed", self.label_maker());
            Err(())
        }
    }

    pub fn fmt(&mut self) -> Result<(), ()> {
        self.dirty_cache = true;
        fmt(Path::new(&self.path))
    }

    pub fn fuzz_all(&mut self) -> Result<(), ()> {
        self.dirty_cache = true;
        fuzz_all(
            Path::new(&self.path),
            self.fuzz_seconds,
            self.fuzz_cores,
            self.fuzz_len,
        )
    }

    pub fn set_profile(&mut self, profile: Profile) {
        self.profile = profile;
    }

    pub fn set_triple(&mut self, triple: Triple) {
        self.triple = triple;
    }

    pub fn set_channel(&mut self, channel: Channel) {
        self.flush_cache();
        if let Some(ref mut cache) = self.cache {
            let mut path = self.cache_base.clone().unwrap();
            path.push(format!("{}", channel));
            cache.set_cache(path);
        }
        self.channel = channel;
    }

    pub fn set_fuzz_seconds(&mut self, seconds: usize) {
        self.fuzz_seconds = seconds;
    }

    pub fn fuzz_seconds(&self) -> usize {
        self.fuzz_seconds
    }

    pub fn set_fuzz_cores(&mut self, cores: usize) {
        self.fuzz_cores = cores;
    }

    pub fn fuzz_cores(&self) -> usize {
        self.fuzz_cores
    }

    pub fn set_fuzz_len(&mut self, bytes: usize) {
        self.fuzz_len = bytes;
    }

    pub fn fuzz_len(&self) -> usize {
        self.fuzz_len
    }

    pub fn set_cache(&mut self, path: Option<PathBuf>) {
        self.flush_cache();
        self.clean();
        match path {
            Some(path) => {
                self.cache_base = Some(path.clone());
                let mut cache_path = path.clone();
                cache_path.push(format!("{}", self.channel));
                let cache = Cache::new(PathBuf::from(self.path.clone()), cache_path);
                let _ = cache.load();
                self.cache = Some(cache);
            }
            None => {
                self.cache = None;
            }
        }
    }

    pub fn flush_cache(&mut self) {
        if self.dirty_cache {
            if let Some(cache) = self.cache.clone() {
                let _ = cache.save();
                self.dirty_cache = false;
            }
        }
    }
}

// run cargo clean in the given path
fn clean(path: &Path) -> Result<(), ()> {
    info!("cargo clean: starting");
    let output = Command::new("cargo")
        .arg("clean")
        .current_dir(path)
        .output()
        .expect("failed to run cargo clean");

    debug!("stdout:\n{}", forced_string(output.stdout));
    debug!("stderr:\n{}", forced_string(output.stderr));

    if output.status.success() {
        info!("cargo clean: ok");
        Ok(())
    } else {
        info!("cargo clean: error");
        Err(())
    }
}

// run cargo clippy
// toolchain: nightly
fn clippy(path: &Path) -> Result<(), ()> {
    info!("cargo clippy: started");
    let output = Command::new("cargo")
        .arg("+nightly")
        .arg("clippy")
        .current_dir(path)
        .output()
        .expect("failed to run cargo test");

    debug!("stdout:\n{}", forced_string(output.stdout));
    debug!("stderr:\n{}", forced_string(output.stderr));

    if output.status.success() {
        info!("cargo clippy: passed");
        Ok(())
    } else {
        info!("cargo clippy: failed");
        Err(())
    }
}

// run cargo fmt detecting differences
// toolchain: stable
fn fmt(path: &Path) -> Result<(), ()> {
    info!("cargo fmt: started");
    let output = Command::new("cargo")
        .arg("fmt")
        .arg("--")
        .arg("--write-mode=diff")
        .current_dir(path)
        .output()
        .expect("failed to run cargo fmt");

    debug!("stdout:\n{}", forced_string(output.stdout));
    debug!("stderr:\n{}", forced_string(output.stderr));

    if output.status.success() {
        info!("cargo fmt: passed");
        Ok(())
    } else {
        info!("cargo fmt: failed");
        Err(())
    }
}

// run all of the fuzzers defined within the given path
// toolchain: nightly
fn fuzz_all(path: &Path, seconds: usize, cores: usize, len: usize) -> Result<(), ()> {
    info!("cargo fuzz: started");
    if let Ok(targets) = fuzz_list(path) {
        for t in targets {
            if fuzz_run(path, &t, seconds, cores, len).is_err() {
                debug!("stop fuzzing after failure: {}", t);
                info!("cargo fuzz: error");
                return Err(());
            }
        }
        info!("cargo fuzz: passed");
        return Ok(());
    } else {
        info!("no targets");
    }
    Err(())
}

// lists the available fuzzers
// toolchain: nightly
fn fuzz_list(path: &Path) -> Result<Vec<String>, ()> {
    let output = Command::new("cargo")
        .arg("+nightly")
        .arg("fuzz")
        .arg("list")
        .current_dir(path)
        .output()
        .expect("failed to run cargo fuzz list");
    if output.status.success() {
        fuzz_list_parse(output.stdout)
    } else {
        info!("cargo fuzz: failed to list fuzz targets");
        Err(())
    }
}

// run the named fuzzer in the given path
fn fuzz_run(path: &Path, fuzzer: &str, seconds: usize, cores: usize, len: usize) -> Result<(), ()> {
    let label = format!("cargo fuzz {}:", fuzzer);
    info!("{} started", label);
    let output = Command::new("cargo")
        .arg("+nightly")
        .arg("fuzz")
        .arg("run")
        .arg(fuzzer)
        .arg("--")
        .arg(format!("-max_total_time={}", seconds))
        .arg(format!("-timeout={}", seconds))
        .arg(format!("-jobs={}", cores))
        .arg(format!("-workers={}", cores))
        .arg(format!("-max_len={}", len))
        .current_dir(path)
        .output()
        .expect("failed to run cargo fuzz run");

    debug!("stdout:\n{}", forced_string(output.stdout));
    debug!("stderr:\n{}", forced_string(output.stderr));

    if output.status.success() {
        info!("cargo fuzz {}: passed", fuzzer);
        Ok(())
    } else {
        info!("cargo fuzz {}: failed", fuzzer);
        Err(())
    }
}

// parse the output of "cargo fuzz list" to get list of fuzz test names
// note: marked pub for fuzz testing
pub fn fuzz_list_parse(stdout: Vec<u8>) -> Result<Vec<String>, ()> {
    if let Ok(stdout) = String::from_utf8(stdout) {
        let re = Regex::new(r"(fuzz_\w+)\n").unwrap();
        let mut result = Vec::<String>::new();
        for cap in re.captures_iter(&stdout) {
            result.push(cap[1].to_owned());
        }
        return Ok(result);
    }
    Err(())
}

mod tests {
    #[test]
    fn test_fuzz_list_parse() {
        use super::*;
        let data = r#"[38;5;2mfuzz_1
[m(B[38;5;2mfuzz_2
[m(B[38;5;2mfuzz_3
[m(B"#;
        let data = data.as_bytes();
        let expected = vec![
            "fuzz_1".to_owned(),
            "fuzz_2".to_owned(),
            "fuzz_3".to_owned(),
        ];
        let result = fuzz_list_parse(data.to_vec()).unwrap();
        assert_eq!(expected, result);
    }
}
