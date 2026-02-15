use metriken::AtomicHistogram;

pub struct GrpcLatency {
    unary: AtomicHistogram,
}

impl GrpcLatency {
    pub(crate) fn new() -> Self {
        Self {
            unary: AtomicHistogram::new(7, 64),
        }
    }

    /// Unary call latency.
    pub fn unary(&self) -> &AtomicHistogram {
        &self.unary
    }
}
