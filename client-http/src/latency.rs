use metriken::AtomicHistogram;

pub struct HttpLatency {
    request: AtomicHistogram,
    get: AtomicHistogram,
    post: AtomicHistogram,
}

impl HttpLatency {
    pub(crate) fn new() -> Self {
        Self {
            request: AtomicHistogram::new(7, 64),
            get: AtomicHistogram::new(7, 64),
            post: AtomicHistogram::new(7, 64),
        }
    }

    /// All operations combined.
    pub fn request(&self) -> &AtomicHistogram {
        &self.request
    }

    /// GET latency.
    pub fn get(&self) -> &AtomicHistogram {
        &self.get
    }

    /// POST latency.
    pub fn post(&self) -> &AtomicHistogram {
        &self.post
    }
}
