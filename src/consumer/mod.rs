mod cargo;
mod config;
mod git;

pub use self::config::Config;

use common::metrics::Metric;
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
        info!("build path: {:?}", build_path);
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
            // run test
            let result_test = cargo::test(build_path.as_path());
            let result_fmt = cargo::fmt(build_path.as_path());
            let result_clippy = cargo::clippy(build_path.as_path());
            let result_fuzz = cargo::fuzz_all(build_path.as_path());

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
