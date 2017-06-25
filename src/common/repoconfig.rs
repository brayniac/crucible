use std::collections::BTreeMap;
use std::default::Default;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use toml::{Parser, Value};
use toml::Value::Table;

pub struct Config {
    fuzz: bool,
    fuzz_cores: usize,
    fuzz_seconds: usize,
    fuzz_max_len: usize,
    cross: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            fuzz: true,
            fuzz_cores: 1,
            fuzz_seconds: 60,
            fuzz_max_len: 64,
            cross: true,
        }
    }
}

impl Config {
    pub fn set_fuzz_cores(&mut self, cores: usize) -> &mut Self {
        self.fuzz_cores = cores;
        self
    }

    #[allow(dead_code)]
    pub fn fuzz_cores(&self) -> usize {
        self.fuzz_cores
    }

    pub fn set_fuzz_seconds(&mut self, seconds: usize) -> &mut Self {
        self.fuzz_seconds = seconds;
        self
    }

    #[allow(dead_code)]
    pub fn fuzz_seconds(&self) -> usize {
        self.fuzz_seconds
    }

    pub fn set_fuzz_max_len(&mut self, bytes: usize) -> &mut Self {
        self.fuzz_max_len = bytes;
        self
    }

    #[allow(dead_code)]
    pub fn fuzz_max_len(&self) -> usize {
        self.fuzz_max_len
    }

    pub fn set_fuzz(&mut self, enable: bool) -> &mut Self {
        self.fuzz = enable;
        self
    }

    pub fn fuzz(&self) -> bool {
        self.fuzz
    }

    pub fn set_cross(&mut self, enable: bool) -> &mut Self {
        self.cross = enable;
        self
    }

    pub fn cross(&self) -> bool {
        self.cross
    }
}

pub fn load_config(path: &Path) -> Result<Config, String> {
    let cfg_txt = match File::open(path) {
        Ok(mut f) => {
            let mut cfg_txt = String::new();
            f.read_to_string(&mut cfg_txt).unwrap();
            cfg_txt
        }
        Err(e) => return Err(format!("Error opening config: {}", e)),
    };

    let mut p = Parser::new(&cfg_txt);

    match p.parse() {
        Some(table) => {
            debug!("toml parsed successfully. creating config");
            load_config_table(&table)
        }

        None => {
            for err in &p.errors {
                let (loline, locol) = p.to_linecol(err.lo);
                let (hiline, hicol) = p.to_linecol(err.hi);
                println!(
                    "{:?}:{}:{}-{}:{} error: {}",
                    path,
                    loline,
                    locol,
                    hiline,
                    hicol,
                    err.desc
                );
            }
            Err("failed to load config".to_owned())
        }
    }
}

fn load_config_table(table: &BTreeMap<String, Value>) -> Result<Config, String> {

    let mut config = Config::default();

    if let Some(&Table(ref general)) = table.get("fuzz") {
        if let Some(v) = general.get("enable").and_then(|k| k.as_bool()) {
            config.set_fuzz(v as bool);
        }
    }

    if let Some(&Table(ref general)) = table.get("fuzz") {
        if let Some(v) = general.get("cores").and_then(|k| k.as_integer()) {
            config.set_fuzz_cores(v as usize);
        }
    }

    if let Some(&Table(ref general)) = table.get("fuzz") {
        if let Some(v) = general.get("seconds").and_then(|k| k.as_integer()) {
            config.set_fuzz_seconds(v as usize);
        }
    }

    if let Some(&Table(ref general)) = table.get("fuzz") {
        if let Some(v) = general.get("length").and_then(|k| k.as_integer()) {
            config.set_fuzz_max_len(v as usize);
        }
    }

    if let Some(&Table(ref general)) = table.get("cross") {
        if let Some(v) = general.get("enable").and_then(|k| k.as_bool()) {
            config.set_cross(v as bool);
        }
    }

    Ok(config)
}
