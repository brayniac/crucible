use consumer::forced_string;
use regex::Regex;
use std::path::Path;
use std::process::Command;

pub fn test(path: &Path) -> Result<(), ()> {
    info!("cargo test: starting");
    let output = Command::new("cargo")
        .arg("test")
        .current_dir(path)
        .output()
        .expect("failed to run cargo test");

    debug!("stdout:\n{}", forced_string(output.stdout));
    debug!("stderr:\n{}", forced_string(output.stderr));

    if output.status.success() {
        info!("cargo test: passed");
        Ok(())
    } else {
        info!("cargo test: failed");
        Err(())
    }
}

pub fn clippy(path: &Path) -> Result<(), ()> {
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

pub fn fmt(path: &Path) -> Result<(), ()> {
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

pub fn fuzz_all(path: &Path) -> Result<(), ()> {
    info!("cargo fuzz: started");
    if let Ok(targets) = fuzz_list(path) {
        for t in targets {
            if fuzz_run(path, &t).is_err() {
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

pub fn fuzz_list(path: &Path) -> Result<Vec<String>, ()> {
    let output = Command::new("cargo")
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

pub fn fuzz_run(path: &Path, fuzzer: &str) -> Result<(), ()> {
    info!("cargo fuzz {}: started", fuzzer);
    let output = Command::new("cargo")
        .arg("+nightly")
        .arg("fuzz")
        .arg("run")
        .arg(fuzzer)
        .arg("--")
        .arg("-max_total_time=60")
        .arg("-timeout=60")
        .arg("-jobs=8")
        .arg("-workers=8")
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

// this should be fuzz tested
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
