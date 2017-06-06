use curl::easy::{Easy, List};
use json;
use metrics::Metric;
use mpmc::Queue;
use shuteye::sleep;
use std::default::Default;
use std::fmt;
use std::io::Read;
use std::process::Command;
use std::time::Duration;
use tic::{Clocksource, Sample, Sender};
use webhook::event::*;

pub struct Config {
    events: Option<Queue<Event>>,
    clock: Option<Clocksource>,
    stats: Option<Sender<Metric>>,
    token: Option<String>,
}

impl Config {
    pub fn build(self) -> Result<Consumer, &'static str> {
        Consumer::configured(self)
    }

    pub fn clock(mut self, clock: Clocksource) -> Self {
        self.clock = Some(clock);
        self
    }

    pub fn events(mut self, queue: Queue<Event>) -> Self {
        self.events = Some(queue);
        self
    }

    pub fn stats(mut self, sender: Sender<Metric>) -> Self {
        self.stats = Some(sender);
        self
    }

    pub fn token(mut self, token: String) -> Self {
        self.token = Some(token);
        self
    }
}


impl Default for Config {
    fn default() -> Config {
        Config {
            events: None,
            clock: None,
            stats: None,
            token: None,
        }
    }
}

pub enum Status {
    Pending,
    Success,
    Error,
    Failure,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Status::Pending => write!(f, "pending"),
            Status::Success => write!(f, "success"),
            Status::Error => write!(f, "error"),
            Status::Failure => write!(f, "failure"),
        }
    }
}

pub struct Consumer {
    clock: Clocksource,
    stats: Sender<Metric>,
    events: Queue<Event>,
    token: String,
}

impl Consumer {
    pub fn configure() -> Config {
        Default::default()
    }

    pub fn configured(config: Config) -> Result<Consumer, &'static str> {
        let events = config.events.clone();
        let clock = config.clock.clone();
        let stats = config.stats.clone();
        let token = config.token.clone();

        if events.is_none() {
            return Err("need events queue");
        }
        if clock.is_none() {
            return Err("need clock");
        }
        if stats.is_none() {
            return Err("need stats");
        }
        if token.is_none() {
            return Err("need token");
        }
        Ok(Consumer {
               events: events.unwrap(),
               clock: clock.unwrap(),
               stats: stats.unwrap(),
               token: token.unwrap(),
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

    fn send_status(&self,
                   repo: &str,
                   sha: &str,
                   state: &str,
                   context: &str,
                   description: &str,
                   url: &str) {
        info!("set: {} to: {}", sha, state);
        let endpoint = format!("https://api.github.com/repos/{}/statuses/{}", repo, sha);
        let auth = format!("Authorization: token {}", self.token);
        let mut list = List::new();
        list.append(&auth).unwrap();
        list.append("content-type: application/json").unwrap();

        let data = object!{
            "state" => state,
            "target_url" => url,
            "description" => description,
            "context" => context
        };

        trace!("sending: {}", data);


        let mut data_to_upload = data.dump();
        let mut handle = Easy::new();
        handle.useragent("crucible");
        handle.url(&endpoint).unwrap();
        handle.http_headers(list).unwrap();
        handle.post(true).unwrap();
        let mut response = Vec::new();

        handle.post_fields_copy(&data_to_upload.as_bytes());
        {
            let mut transfer = handle.transfer();
            transfer
                .write_function(|new_data| {
                                    response.extend_from_slice(new_data);
                                    Ok(new_data.len())
                                })
                .unwrap();
            transfer.perform().unwrap();
        }
        let rsp_string = String::from_utf8(response).unwrap_or("invalid utf8".to_owned());
        trace!("response: {}", rsp_string);
    }

    fn handle_event(&mut self, event: Event) {
        // this gets scary
        let base_path = "/mnt/scratch/";

        let description = match event {
            Event::Push(_) => "continuous-integration/crucible/push",
            Event::PullRequest(_) => "continuous-integration/crucible/pr",
            _ => {
                return;
            }
        };

        let id = "temp";
        let path = base_path.to_owned() + id;

        // skip events with this sha, happens when branch deleted
        if let Event::Push(push) = event.clone() {
            if push.sha() == "0000000000000000000000000000000000000000" {
                return;
            }
        }

        // skip pull requests that aren't either opened or edited
        // this avoids retesting a closed pull request
        if let Event::PullRequest(pr) = event.clone() {
            let action = pr.action();
            match action.as_str() {
                "opened" | "edited" => {}
                _ => {
                    return;
                }
            }
        }

        let repo = match event.clone() {
            Event::PullRequest(pr) => pr.repo(),
            Event::Push(push) => push.repo(),
            _ => {
                panic!("unsupported event");
            }
        };
        let sha = match event.clone() {
            Event::PullRequest(pr) => pr.sha(),
            Event::Push(push) => push.sha(),
            _ => {
                panic!("unsupported event");
            }
        };

        // inform github we're running a test
        self.send_status(&repo,
                         &sha,
                         "pending",
                         description,
                         "pending...",
                         "https://oxidize.io");

        // prepare
        create_directory(&path);
        let status = match event {
            Event::PullRequest(pr) => clone_pr(&path, &pr.repo(), &pr.url(), &pr.number()),
            Event::Push(push) => clone_push(&path, &push.repo(), &push.url(), &push.sha()),
            _ => {
                panic!("unsupported event");
            }
        };

        if status.is_err() {
            self.send_status(&repo,
                             &sha,
                             "error",
                             description,
                             "whoops. error.",
                             "https://oxidize.io");
        } else {
            // run test
            let result_test = cargo_test(&path);
            let result_fmt = cargo_fmt(&path);
            let result_clippy = cargo_clippy(&path);

            // this should send a real result
            if result_test.is_err() || result_fmt.is_err() || result_clippy.is_err() {
                self.send_status(&repo,
                                 &sha,
                                 "failed",
                                 description,
                                 "the build failed",
                                 "https://oxidize.io");
            } else {
                self.send_status(&repo,
                                 &sha,
                                 "success",
                                 description,
                                 "lgtm. shipit",
                                 "https://oxidize.io");
            }
        }

        // cleanup
        remove_directory(&path);
    }
}

// clone the repo into a build folder within the path given
fn clone_repo(path: &str, name: &str, url: &str) -> Result<(), ()> {
    info!("clone repo: {}", name);
    let output = Command::new("git")
        .arg("clone")
        .arg(url)
        .arg("build")
        .current_dir(path)
        .output()
        .expect("failed to run git");
    if !output.status.success() {
        error!("clone failed!");
        Err(())
    } else {
        info!("clone completed");
        Ok(())
    }
}

//git fetch origin pull/7324/head:pr-7324
fn clone_pr(path: &str, name: &str, url: &str, number: &u64) -> Result<(), ()> {
    if clone_repo(path, name, url).is_ok() {
        let pr_ref = format!("pull/{}/head:pr-{}", number, number);
        let output = Command::new("git")
            .arg("fetch")
            .arg("origin")
            .arg(pr_ref)
            .current_dir(path.to_owned() + "/build")
            .output()
            .expect("failed to run git");
        if !output.status.success() {
            return Err(());
        }
        Ok(())
    } else {
        Err(())
    }

}

fn clone_push(path: &str, name: &str, url: &str, sha: &str) -> Result<(), ()> {
    if clone_repo(path, name, url).is_ok() {
        let output = Command::new("git")
            .arg("checkout")
            .arg(sha)
            .current_dir(path.to_owned() + "/build")
            .output()
            .expect("failed to run git");
        if !output.status.success() {
            Err(())
        } else {
            Ok(())
        }
    } else {
        Err(())
    }
}

fn create_directory(path: &str) {
    Command::new("mkdir")
        .arg("-p")
        .arg(path)
        .status()
        .expect("failed to run mkdir");
}

fn remove_directory(path: &str) {
    Command::new("rm")
        .arg("-rf")
        .arg(path)
        .status()
        .expect("failed to run rm");
}

fn cargo_test(path: &str) -> Result<(), ()> {
    let output = Command::new("cargo")
        .arg("test")
        .current_dir(path.to_owned() + "/build")
        .output()
        .expect("failed to run cargo test");
    if output.status.success() {
        info!("cargo test: passed");
        Ok(())
    } else {
        info!("cargo test: failed");
        Err(())
    }
}

fn cargo_clippy(path: &str) -> Result<(), ()> {
    let output = Command::new("cargo")
        .arg("+nightly")
        .arg("clippy")
        .current_dir(path.to_owned() + "/build")
        .output()
        .expect("failed to run cargo test");
    if output.status.success() {
        info!("cargo clippy: passed");
        Ok(())
    } else {
        info!("cargo clippy: failed");
        Err(())
    }
}

fn cargo_fmt(path: &str) -> Result<(), ()> {
    let output = Command::new("cargo")
        .arg("fmt")
        .arg("--")
        .arg("--write-mode=diff")
        .current_dir(path.to_owned() + "/build")
        .output()
        .expect("failed to run cargo fmt");
    if output.status.success() {
        info!("cargo fmt: passed");
        Ok(())
    } else {
        info!("cargo fmt: failed");
        Err(())
    }
}
