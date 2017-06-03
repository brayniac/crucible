use std::fmt;
use tic::Receiver;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Metric {
    //Window,
}

impl fmt::Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            //Metric::Window => write!(f, "window"),
        }
    }
}

pub fn init(listen: Option<String>) -> Receiver<Metric> {
    let mut config = Receiver::<Metric>::configure()
        .batch_size(1)
        .capacity(4096)
        .service(true);

    if let Some(addr) = listen {
        config = config.http_listen(addr);
    }
    config.build()
}
