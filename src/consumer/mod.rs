mod caching;
pub mod cargo;
mod config;
mod git;

pub use self::config::Config;
use common::metrics::Metric;

use common::repoconfig;
use mktemp::Temp;
use mpmc::Queue;
use publisher::{self, Status};
use shuteye::sleep;
use std::default::Default;
use std::path::Path;
use std::time::Duration;
use tic::{Clocksource, Sample, Sender};
use webhook::event::*;

pub struct Consumer {
    clock: Clocksource,
    stats: Sender<Metric>,
    events: Queue<Event>,
    publisher: Queue<publisher::Event>,
    repo: Option<String>,
    author: Option<String>,
    fuzz_seconds: usize,
    fuzz_cores: usize,
    keep_temp: bool,
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
            fuzz_seconds: config.fuzz_seconds,
            fuzz_cores: config.fuzz_cores,
            keep_temp: false,
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

        let mut temp_dir = Temp::new_dir_in(Path::new("/mnt/scratch/")).unwrap();
        let path = temp_dir.to_path_buf();

        let description = match event {
            Event::Push(_) => "continuous-integration/oxidize-io/push",
            Event::PullRequest(_) => "continuous-integration/oxidize-io/pr",
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

        if git::clone_repo(path.as_path(), &repo, &url).is_err() {
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
        build_path.push("build");
        info!("build path: {:?}", build_path);

        let status = match event {
            Event::PullRequest(pr) => {
                let fetch = git::fetch_pull(build_path.as_path(), &pr.number());
                match fetch {
                    Ok(_) => git::checkout_pr(build_path.as_path(), &pr.number()),
                    Err(_) => Err(()),
                }
            }
            Event::Push(push) => git::checkout_sha(build_path.as_path(), &push.sha()),
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
            // load repo's .crucible.toml
            let mut file = build_path.clone();
            file.push(".crucible.toml");
            let repo_config = repoconfig::load_config(file.as_path()).unwrap_or_default();
            // load cache
            let cache_dir = "/mnt/cache/".to_owned() + &repo + "/stable";
            let _ = caching::load(build_path.as_path(), Path::new(&cache_dir));

            // run test
            let mut errors = 0;

            let path = build_path.as_path();
            let mut cargo = cargo::Cargo::new(path.to_str().unwrap().to_owned());

            if cargo.build().is_err() {
                errors += 1;
            }
            if cargo.test().is_err() {
                errors += 1;
            }
            if cargo.fmt().is_err() {
                errors += 1;
            }
            cargo.set_release(true);
            if cargo.build().is_err() {
                errors += 1;
            }
            if cargo.test().is_err() {
                errors += 1;
            }

            // save cache and clean buid dir
            let _ = caching::save(path, Path::new(&cache_dir));
            let _ = cargo.clean();
            cargo.set_release(false);

            // setup cache for nightly
            let cache_dir = "/mnt/cache/".to_owned() + &repo + "/nightly";
            let _ = caching::load(path, Path::new(&cache_dir));

            // run nightly tests
            if cargo.build().is_err() {
                errors += 1;
            }
            if cargo.test().is_err() {
                errors += 1;
            }
            if cargo.clippy().is_err() {
                errors += 1;
            }
            if repo_config.fuzz() {
                if cargo.fuzz_all(self.fuzz_seconds, self.fuzz_cores).is_err() {
                    errors += 1;
                }
            }
            cargo.set_release(true);
            if cargo.build().is_err() {
                errors += 1;
            }
            if cargo.test().is_err() {
                errors += 1;
            }

            let _ = caching::save(path, Path::new(&cache_dir));
            let _ = cargo.clean();
            cargo.set_release(false);

            if repo_config.cross() {
                let targets = vec![
                    "aarch64-unknown-linux-gnu", // Tier-2
                    "arm-unknown-linux-gnueabi", // Tier-2
                    "arm-unknown-linux-gnueabihf", // Tier-2
                    "armv7-unknown-linux-gnueabihf", // Tier-2
                    "i686-unknown-linux-gnu", // Tier-1
                    "i686-unknown-linux-musl", // Tier-2
                    "x86_64-unknown-linux-gnu", // Tier-1
                    "x86_64-unknown-linux-musl", // Tier-2
                ];

                for target in targets {
                    // setup cache for target
                    for channel in vec!["stable", "nightly"] {
                        let cache_dir = "/mnt/cache/".to_owned() + &repo + "/" + channel + "-" +
                            target;
                        let _ = caching::load(path, Path::new(&cache_dir));

                        cargo.set_target(target.to_owned());
                        if cargo.build().is_err() {
                            errors += 1;
                        }
                        if cargo.test().is_err() {
                            errors += 1;
                        }
                        cargo.set_release(true);
                        if cargo.build().is_err() {
                            errors += 1;
                        }
                        if cargo.test().is_err() {
                            errors += 1;
                        }

                        let _ = caching::save(path, Path::new(&cache_dir));
                        let _ = cargo.clean();
                        cargo.set_release(false);
                    }
                }
            }

            // report result
            if errors > 0 {
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
            if self.keep_temp {
                temp_dir.release();
            }
        }
    }
}

pub fn forced_string(input: Vec<u8>) -> String {
    String::from_utf8(input).unwrap_or_else(|_| "invalid utf8".to_owned())
}
