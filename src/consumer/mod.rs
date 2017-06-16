use common::metrics::Metric;
use curl::easy::{Easy, List};
use mktemp::Temp;
use mpmc::Queue;
use publisher;
use publisher::Status;
use regex::Regex;
use shuteye::sleep;
use std::default::Default;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use tic::{Clocksource, Sample, Sender};
use webhook::event::*;

mod config;

pub use self::config::Config;

pub struct Consumer {
    clock: Clocksource,
    stats: Sender<Metric>,
    events: Queue<Event>,
    publisher: Queue<publisher::Event>,
    repo: Option<String>,
    author: Option<String>,
}

impl Consumer {
    pub fn configure() -> Config {
        Default::default()
    }

    pub fn configured(config: Config) -> Result<Consumer, &'static str> {
        let events = config.events.clone();
        let clock = config.clock.clone();
        let stats = config.stats.clone();
        let publisher = config.publisher.clone();
        let repo = config.repo.clone();
        let author = config.author.clone();

        if events.is_none() {
            return Err("need events queue");
        }
        if clock.is_none() {
            return Err("need clock");
        }
        if stats.is_none() {
            return Err("need stats");
        }
        if publisher.is_none() {
            return Err("need publisher");
        }
        Ok(Consumer {
            events: events.unwrap(),
            clock: clock.unwrap(),
            stats: stats.unwrap(),
            publisher: publisher.unwrap(),
            repo: repo,
            author: author,
        })
    }

    fn time(&self) -> u64 {
        self.clock.counter()
    }

    pub fn run(&mut self) {
        if let Some(event) = self.events.pop() {
            let t0 = self.time();
            trace!("consume event: {:?}", event);
            // do processing
            self.handle_event(event);
            let t1 = self.time();
            let _ = self.stats.send(Sample::new(t0, t1, Metric::Processed));
        }
        // call sleep
        sleep(Duration::new(0, 1_000_000));
    }

    fn send_status(
        &self,
        repo: &str,
        sha: &str,
        state: &str,
        context: &str,
        description: &str,
        url: &str,
    ) {
        let status = Status {
            repo: repo.to_owned(),
            sha: sha.to_owned(),
            state: state.to_owned(),
            context: context.to_owned(),
            description: description.to_owned(),
            url: url.to_owned(),
        };
        info!("sending publisher Event");
        let event = publisher::Event::Status(status);

        self.publisher.push(event).unwrap();
    }

    fn should_handle(&mut self, event: &Event) -> bool {
        // events must have a repo
        if event.repo().is_none() {
            return false;
        }

        // skip events with this sha, happens when branch deleted
        if event.sha() == Some("0000000000000000000000000000000000000000".to_owned()) {
            debug!("skip event with zero-SHA");
            return false;
        }

        // repository whitelist
        if let Some(ref whitelist) = self.repo {
            if event.repo().unwrap() != *whitelist {
                debug!("repo: {} not in whitelist", event.repo().unwrap());
                debug!("whitelist: {:?}", *whitelist);
                return false;
            }
        }

        // author whitelist
        if let Some(ref whitelist) = self.author {
            if event.author().unwrap() != *whitelist {
                debug!("author: {} not in whitelist", event.author().unwrap());
                debug!("whitelist: {:?}", *whitelist);
                return false;
            }
        }

        // skip pull requests that aren't either opened or edited
        // this avoids retesting a closed pull request
        if let Event::PullRequest(pr) = event.clone() {
            let action = pr.action();
            match action.as_str() {
                "opened" | "edited" | "synchronize" => {}
                _ => {
                    return false;
                }
            }
        }

        true
    }

    fn handle_event(&mut self, event: Event) {
        if !self.should_handle(&event) {
            return;
        }

        let repo = event.repo().unwrap();

        let temp_dir = Temp::new_dir_in(Path::new("/mnt/scratch/")).unwrap();
        let path = temp_dir.to_path_buf();

        let description = match event {
            Event::Push(_) => "continuous-integration/crucible/push",
            Event::PullRequest(_) => "continuous-integration/crucible/pr",
            _ => unreachable!(),
        };

        let sha = event.sha().expect("event is missing a sha");
        let url = event.url().expect("event is missing a url");

        // inform github we're running a test
        self.send_status(
            &repo,
            &sha,
            "pending",
            description,
            "pending...",
            "https://oxidize.io",
        );

        if clone_repo(path.as_path(), &repo, &url).is_err() {
            self.send_status(
                &repo,
                &sha,
                "error",
                description,
                "failed to clone repo",
                "https://oxidize.io",
            );
            return;
        }

        let mut build_path = path.clone();
        info!("build path: {:?}", build_path);
        build_path.push("build");
        info!("build path: {:?}", build_path);

        let status = match event {
            Event::PullRequest(pr) => {
                let fetch = fetch_pull(build_path.as_path(), &pr.number());
                match fetch {
                    Ok(_) => checkout_pr(build_path.as_path(), &pr.number()),
                    Err(_) => Err(()),
                }
            }
            Event::Push(push) => checkout_sha(build_path.as_path(), &push.sha()),
            _ => {
                unreachable!();
            }
        };

        if status.is_err() {
            self.send_status(
                &repo,
                &sha,
                "error",
                description,
                "failed to sync repo",
                "https://oxidize.io",
            );
        } else {
            // run test
            let result_test = cargo_test(build_path.as_path());
            let result_fmt = cargo_fmt(build_path.as_path());
            let result_clippy = cargo_clippy(build_path.as_path());
            let result_fuzz = cargo_fuzz(build_path.as_path());

            // this should send a real result
            if result_test.is_err() || result_fmt.is_err() || result_clippy.is_err() ||
                result_fuzz.is_err()
            {
                self.send_status(
                    &repo,
                    &sha,
                    "failure",
                    description,
                    "test failures found",
                    "https://oxidize.io",
                );
            } else {
                self.send_status(
                    &repo,
                    &sha,
                    "success",
                    description,
                    "all tests passed",
                    "https://oxidize.io",
                );
            }
        }
    }
}

fn forced_string(input: Vec<u8>) -> String {
    String::from_utf8(input).unwrap_or_else(|_| "invalid utf8".to_owned())
}

// clone the repo into a build folder within the path given
fn clone_repo(path: &Path, name: &str, url: &str) -> Result<(), ()> {
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
        Err(())
    }
}

fn fetch_pull(path: &Path, number: &u64) -> Result<(), ()> {
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
        Err(())
    }
}

fn checkout_pr(path: &Path, number: &u64) -> Result<(), ()> {
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
        Err(())
    }
}

fn checkout_sha(path: &Path, sha: &str) -> Result<(), ()> {
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
        Err(())
    }
}

fn cargo_test(path: &Path) -> Result<(), ()> {
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

fn cargo_clippy(path: &Path) -> Result<(), ()> {
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

fn cargo_fmt(path: &Path) -> Result<(), ()> {
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

fn cargo_fuzz(path: &Path) -> Result<(), ()> {
    info!("cargo fuzz: started");
    if let Ok(targets) = cargo_fuzz_list(path) {
        for t in targets {
            if cargo_fuzz_run(path, t.clone()).is_err() {
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

fn cargo_fuzz_list(path: &Path) -> Result<Vec<String>, ()> {
    let output = Command::new("cargo")
        .arg("fuzz")
        .arg("list")
        .current_dir(path)
        .output()
        .expect("failed to run cargo fuzz list");
    if output.status.success() {
        cargo_fuzz_list_parse(output.stdout)
    } else {
        info!("cargo fuzz: failed to list fuzz targets");
        Err(())
    }
}

fn cargo_fuzz_run(path: &Path, fuzzer: String) -> Result<(), ()> {
    info!("cargo fuzz {}: started", fuzzer);
    let output = Command::new("cargo")
        .arg("+nightly")
        .arg("fuzz")
        .arg("run")
        .arg(fuzzer.clone())
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
fn cargo_fuzz_list_parse(stdout: Vec<u8>) -> Result<Vec<String>, ()> {
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
        let result = cargo_fuzz_list_parse(data.to_vec()).unwrap();
        assert_eq!(expected, result);
    }
}
