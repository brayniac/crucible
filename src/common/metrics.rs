use std::fmt;
use std::time::Duration;
use tic::Receiver;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Metric {
    Request,
    Processed,
}

impl fmt::Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Metric::Request => write!(f, "request"),
            Metric::Processed => write!(f, "processed"),
        }
    }
}

pub fn init(listen: Option<String>) -> Receiver<Metric> {
    let mut config = Receiver::<Metric>::configure()
        .batch_size(1)
        .capacity(4096)
        .poll_delay(Some(Duration::new(0, 1_000_000)))
        .service(true);

    if let Some(addr) = listen {
        info!("listening STATS {}", addr);
        config = config.http_listen(addr);
    }
    config.build()
}
