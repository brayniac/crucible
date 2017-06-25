use getopts::Matches;
use std::collections::BTreeMap;
use std::default::Default;
use std::fs::File;
use std::io::Read;
use toml::{Parser, Value};
use toml::Value::Table;

pub struct Config {
    fuzz_max_len: usize,
    fuzz_cores: usize,
    fuzz_seconds: usize,
    workers: usize,
    stats: String,
    http: String,
    token: Option<String>,
    secret: Option<String>,
    repo: Option<String>,
    author: Option<String>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            stats: "localhost:42024".to_owned(),
            http: "localhost:4567".to_owned(),
            fuzz_max_len: 64,
            fuzz_cores: 1,
            fuzz_seconds: 60,
            workers: 1,
            token: None,
            secret: None,
            repo: None,
            author: None,
        }
    }
}

impl Config {
    pub fn set_token(&mut self, token: String) -> &mut Self {
        self.token = Some(token);
        self
    }

    pub fn token(&self) -> Option<String> {
        self.token.clone()
    }

    pub fn set_http(&mut self, address: String) -> &mut Self {
        self.http = address;
        self
    }

    pub fn http(&self) -> String {
        self.http.clone()
    }

    pub fn set_stats(&mut self, address: String) -> &mut Self {
        self.stats = address;
        self
    }

    pub fn stats(&self) -> String {
        self.stats.clone()
    }

    pub fn set_repo(&mut self, name: String) -> &mut Self {
        self.repo = Some(name);
        self
    }

    pub fn repo(&self) -> Option<String> {
        self.repo.clone()
    }

    pub fn set_author(&mut self, name: String) -> &mut Self {
        self.author = Some(name);
        self
    }

    pub fn author(&self) -> Option<String> {
        self.author.clone()
    }

    pub fn set_secret(&mut self, secret: String) -> &mut Self {
        self.secret = Some(secret);
        self
    }

    pub fn secret(&self) -> Option<String> {
        self.secret.clone()
    }

    pub fn set_fuzz_cores(&mut self, cores: usize) -> &mut Self {
        self.fuzz_cores = cores;
        self
    }

    pub fn fuzz_cores(&self) -> usize {
        self.fuzz_cores
    }

    pub fn set_fuzz_seconds(&mut self, seconds: usize) -> &mut Self {
        self.fuzz_seconds = seconds;
        self
    }

    pub fn fuzz_seconds(&self) -> usize {
        self.fuzz_seconds
    }

    pub fn set_fuzz_max_len(&mut self, bytes: usize) -> &mut Self {
        self.fuzz_max_len = bytes;
        self
    }

    pub fn fuzz_max_len(&self) -> usize {
        self.fuzz_max_len
    }

    pub fn set_workers(&mut self, count: usize) -> &mut Self {
        self.workers = count;
        self
    }

    pub fn workers(&self) -> usize {
        self.workers
    }
}

pub fn load_config(matches: &Matches) -> Result<Config, String> {
    if let Some(toml) = matches.opt_str("config") {
        let cfg_txt = match File::open(&toml) {
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
                        "{}:{}:{}-{}:{} error: {}",
                        toml,
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
    } else {
        Err("config file not specified".to_owned())
    }
}

fn load_config_table(table: &BTreeMap<String, Value>) -> Result<Config, String> {

    let mut config = Config::default();

    if let Some(&Table(ref general)) = table.get("general") {
        if let Some(v) = general.get("token").and_then(|k| k.as_str()).map(|k| {
            k.to_owned()
        })
        {
            config.set_token(v);
        }
    }

    if let Some(&Table(ref general)) = table.get("general") {
        if let Some(v) = general.get("secret").and_then(|k| k.as_str()).map(|k| {
            k.to_owned()
        })
        {
            config.set_secret(v);
        }
    }

    if let Some(&Table(ref general)) = table.get("general") {
        if let Some(v) = general.get("http").and_then(|k| k.as_str()).map(
            |k| k.to_owned(),
        )
        {
            config.set_http(v);
        }
    }

    if let Some(&Table(ref general)) = table.get("general") {
        if let Some(v) = general.get("stats").and_then(|k| k.as_str()).map(|k| {
            k.to_owned()
        })
        {
            config.set_stats(v);
        }
    }

    if let Some(&Table(ref general)) = table.get("general") {
        if let Some(v) = general.get("repo").and_then(|k| k.as_str()).map(
            |k| k.to_owned(),
        )
        {
            config.set_repo(v);
        }
    }

    if let Some(&Table(ref general)) = table.get("general") {
        if let Some(v) = general.get("author").and_then(|k| k.as_str()).map(|k| {
            k.to_owned()
        })
        {
            config.set_author(v);
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
        if let Some(v) = general.get("max-length").and_then(|k| k.as_integer()) {
            config.set_fuzz_max_len(v as usize);
        }
    }

    if let Some(&Table(ref general)) = table.get("general") {
        if let Some(v) = general.get("workers").and_then(|k| k.as_integer()) {
            config.set_workers(v as usize);
        }
    }

    Ok(config)
}
