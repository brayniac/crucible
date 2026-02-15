use metriken::AtomicHistogram;

/// Wire latency histograms (nanoseconds). Not globally registered.
///
/// One histogram per operation type, plus a combined `request` histogram
/// that records every operation. Values are recorded in nanoseconds by
/// the kompio worker as `sent_at.elapsed().as_nanos()`.
pub struct ClientLatency {
    request: AtomicHistogram,
    get: AtomicHistogram,
    set: AtomicHistogram,
    del: AtomicHistogram,
}

impl ClientLatency {
    pub(crate) fn new() -> Self {
        Self {
            request: AtomicHistogram::new(7, 64),
            get: AtomicHistogram::new(7, 64),
            set: AtomicHistogram::new(7, 64),
            del: AtomicHistogram::new(7, 64),
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

    /// SET latency.
    pub fn set(&self) -> &AtomicHistogram {
        &self.set
    }

    /// DEL latency.
    pub fn del(&self) -> &AtomicHistogram {
        &self.del
    }
}
