extern crate time;
extern crate log;

pub struct SimpleLogger;

pub use log::{LogLevel, LogLevelFilter, LogMetadata, LogRecord};

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &LogMetadata) -> bool {
        metadata.level() <= LogLevel::Trace
    }

    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            println!("{} {:<5} [{}] {}",
                     time::strftime("%Y-%m-%d %H:%M:%S", &time::now()).unwrap(),
                     record.level().to_string(),
                     record.target().to_string(),
                     record.args());
        }
    }
}

pub fn set_log_level(level: usize) {
    let log_filter;
    match level {
        0 => {
            log_filter = LogLevelFilter::Info;
        }
        1 => {
            log_filter = LogLevelFilter::Debug;
        }
        _ => {
            log_filter = LogLevelFilter::Trace;
        }
    }
    let _ = log::set_logger(|max_log_level| {
                                max_log_level.set(log_filter);
                                Box::new(SimpleLogger)
                            });
}
