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
pub enum SubCommand {
    Build,
    Test,
    Fmt,
    Fuzz,
    Clippy,
    Clean,
}

impl fmt::Display for SubCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SubCommand::Build => write!(f, "build"),
            SubCommand::Clean => write!(f, "clean"),
            SubCommand::Clippy => write!(f, "clippy"),
            SubCommand::Fmt => write!(f, "fmt"),
            SubCommand::Fuzz => write!(f, "fuzz"),
            SubCommand::Test => write!(f, "test"),
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

    fn command_builder(&self, sub_command: SubCommand) -> Command {
        let mut command = Command::new("cargo");
        command.current_dir(self.path.as_path());
        command.arg(format!("+{}", self.channel));
        command.arg(format!("{}", sub_command));
        match sub_command {
            SubCommand::Build | SubCommand::Test => {
                command.arg("--verbose");
                if self.profile == Profile::Release {
                    command.arg("--release");
                }
                if self.triple != Triple::Native {
                    command.arg("--target");
                    command.arg(format!("{}", self.triple));
                }
            }
            SubCommand::Fmt => {
                command.arg("--").arg("--write-mode=diff");
            }
            _ => {}
        }
        command
    }

    fn command_runner(&self, mut command: Command, sub_command: SubCommand) -> Result<(), ()> {
        let output = command.output().expect(
            "failed to run cargo",
        );
        debug!(
            "{} {}: stdout:\n{}",
            self.label_maker(),
            sub_command,
            forced_string(output.stdout)
        );
        debug!(
            "{} {}: stderr:\n{}",
            self.label_maker(),
            sub_command,
            forced_string(output.stderr)
        );
        if output.status.success() {
            info!("{} {}: passed", self.label_maker(), sub_command);
            Ok(())
        } else {
            info!("{} {}: failed", self.label_maker(), sub_command);
            Err(())
        }
    }

    pub fn build(&mut self) -> Result<(), ()> {
        self.dirty_cache = true;
        let sub_command = SubCommand::Build;
        info!("{} {}: starting", self.label_maker(), sub_command);
        let mut command = self.command_builder(sub_command);
        self.command_runner(command, sub_command)
    }

    pub fn clean(&mut self) -> Result<(), ()> {
        self.dirty_cache = false;
        let sub_command = SubCommand::Clean;
        info!("{} {}: starting", self.label_maker(), sub_command);
        let mut command = self.command_builder(sub_command);
        self.command_runner(command, sub_command)
    }

    pub fn clippy(&mut self) -> Result<(), ()> {
        let channel = self.channel;
        self.channel(Channel::Nightly);
        self.dirty_cache = true;
        let sub_command = SubCommand::Clippy;
        info!("{} {}: starting", self.label_maker(), sub_command);
        let mut command = self.command_builder(sub_command);
        let result = self.command_runner(command, sub_command);
        self.channel(channel);
        result
    }

    pub fn fmt(&mut self) -> Result<(), ()> {
        self.dirty_cache = true;
        let sub_command = SubCommand::Fmt;
        info!("{} {}: starting", self.label_maker(), sub_command);
        let mut command = self.command_builder(sub_command);
        self.command_runner(command, sub_command)
    }

    // run the named fuzzer in the given path
    fn fuzz_run(&mut self, fuzzer: &str) -> Result<(), ()> {
        let channel = self.channel;
        self.channel(Channel::Nightly);
        self.dirty_cache = true;
        let sub_command = SubCommand::Clippy;
        info!("{} {}: starting", self.label_maker(), sub_command);
        let mut command = self.command_builder(sub_command);
        command.arg("run")
            .arg(fuzzer)
            .arg("--")
            .arg(format!("-max_total_time={}", self.fuzz_seconds))
            .arg(format!("-timeout={}", self.fuzz_seconds))
            .arg(format!("-jobs={}", self.fuzz_cores))
            .arg(format!("-workers={}", self.fuzz_cores))
            .arg(format!("-max_len={}", self.fuzz_len));
        let result = self.command_runner(command, sub_command);
        self.channel(channel);
        result
    }

    // run the named fuzzer in the given path
    fn fuzz_list(&mut self) -> Result<Vec<String>, ()> {
        let channel = self.channel;
        self.channel(Channel::Nightly);
        let sub_command = SubCommand::Fuzz;
        info!("{} {}: starting", self.label_maker(), sub_command);
        let mut command = self.command_builder(sub_command);
        command.arg("list");
        let output = command.output().expect("Failed to run cargo fuzz list");
        let result = if output.status.success() {
            fuzz_list_parse(output.stdout)
        } else {
            info!("cargo fuzz: failed to list fuzz targets");
            Err(())
        };
        self.channel(channel);
        result
    }

    pub fn test(&mut self) -> Result<(), ()> {
        self.dirty_cache = true;
        let sub_command = SubCommand::Test;
        info!("{} {}: starting", self.label_maker(), sub_command);
        let mut command = self.command_builder(sub_command);
        self.command_runner(command, sub_command)
    }

    pub fn profile(&mut self, profile: Profile) {
        self.profile = profile;
    }

    pub fn triple(&mut self, triple: Triple) {
        self.triple = triple;
    }

    pub fn channel(&mut self, channel: Channel) {
        self.flush_cache();
        if let Some(ref mut cache) = self.cache {
            let mut path = self.cache_base.clone().unwrap();
            path.push(format!("{}", channel));
            cache.set_cache(path);
        }
        self.channel = channel;
    }

    pub fn fuzz_seconds(&mut self, seconds: usize) {
        self.fuzz_seconds = seconds;
    }

    pub fn fuzz_cores(&mut self, cores: usize) {
        self.fuzz_cores = cores;
    }

    pub fn fuzz_max_len(&mut self, bytes: usize) {
        self.fuzz_len = bytes;
    }

    pub fn cache(&mut self, path: Option<PathBuf>) {
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

    // run all of the fuzzers defined within the given path
    pub fn fuzz_all(&mut self) -> Result<(), ()> {
        info!("cargo fuzz: started");
        if let Ok(targets) = self.fuzz_list() {
            for t in targets {
                if self.fuzz_run(&t).is_err() {
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
