use metriken::AtomicHistogram;

pub struct MomentoLatency {
    get: AtomicHistogram,
    set: AtomicHistogram,
    delete: AtomicHistogram,
}

impl MomentoLatency {
    pub(crate) fn new() -> Self {
        Self {
            get: AtomicHistogram::new(7, 64),
            set: AtomicHistogram::new(7, 64),
            delete: AtomicHistogram::new(7, 64),
        }
    }

    /// Get latency histogram.
    pub fn get(&self) -> &AtomicHistogram {
        &self.get
    }

    /// Set latency histogram.
    pub fn set(&self) -> &AtomicHistogram {
        &self.set
    }

    /// Delete latency histogram.
    pub fn delete(&self) -> &AtomicHistogram {
        &self.delete
    }
}
