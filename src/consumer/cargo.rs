use consumer::forced_string;
use regex::Regex;
use std::path::Path;
use std::process::Command;

pub struct Cargo {
    path: String,
    release: bool,
    target: Option<String>,
}

impl Cargo {
    pub fn new(path: String) -> Cargo {
        Cargo {
            path: path,
            release: false,
            target: None,
        }
    }

    pub fn build(&self) -> Result<(), ()> {
        build(Path::new(&self.path), self.release, self.target.clone())
    }

    pub fn clean(&self) -> Result<(), ()> {
        clean(Path::new(&self.path))
    }

    pub fn clippy(&self) -> Result<(), ()> {
        clippy(Path::new(&self.path))
    }

    pub fn test(&self) -> Result<(), ()> {
        test(Path::new(&self.path), self.release, self.target.clone())
    }

    pub fn fmt(&self) -> Result<(), ()> {
        fmt(Path::new(&self.path))
    }

    pub fn fuzz_all(&self, seconds: usize, cores: usize) -> Result<(), ()> {
        fuzz_all(Path::new(&self.path), seconds, cores)
    }

    pub fn set_release(&mut self, enable: bool) {
        self.release = enable;
    }

    pub fn set_target(&mut self, target: String) {
        self.target = Some(target);
    }
}

// run cargo build in given path in either debug or release mode
fn build(path: &Path, release: bool, target: Option<String>) -> Result<(), ()> {
    let label = if release {
        "cargo build --release:"
    } else {
        "cargo build:"
    };

    info!("{} starting", label);
    let mut command = Command::new("cargo");
    command.arg("build").arg("--verbose");
    if release {
        command.arg("--release");
    }
    if let Some(t) = target {
        command.arg("--target");
        command.arg(t);
    }
    let output = command.current_dir(path).output().expect(
        "failed to run cargo build",
    );

    debug!("{} stdout:\n{}", label, forced_string(output.stdout));
    debug!("{} stderr:\n{}", label, forced_string(output.stderr));

    if output.status.success() {
        info!("{} passed", label);
        Ok(())
    } else {
        info!("{} failed", label);
        Err(())
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

fn test(path: &Path, release: bool, target: Option<String>) -> Result<(), ()> {
    let label = if release {
        "cargo test --release:"
    } else {
        "cargo test:"
    };

    info!("{} starting", label);
    let mut command = Command::new("cargo");
    command.arg("test").arg("--verbose");
    if release {
        command.arg("--release");
    }
    if let Some(t) = target {
        command.arg("--target");
        command.arg(t);
    }
    let output = command.current_dir(path).output().expect(
        "failed to run cargo test",
    );

    debug!("{} stdout:\n{}", label, forced_string(output.stdout));
    debug!("{} stderr:\n{}", label, forced_string(output.stderr));

    if output.status.success() {
        info!("{} passed", label);
        Ok(())
    } else {
        info!("{} failed", label);
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
fn fuzz_all(path: &Path, seconds: usize, cores: usize) -> Result<(), ()> {
    info!("cargo fuzz: started");
    if let Ok(targets) = fuzz_list(path) {
        for t in targets {
            if fuzz_run(path, &t, seconds, cores).is_err() {
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
fn fuzz_run(path: &Path, fuzzer: &str, seconds: usize, cores: usize) -> Result<(), ()> {
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
