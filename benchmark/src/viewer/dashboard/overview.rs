use super::*;

pub fn generate(
    data: &Arc<Tsdb>,
    _server: Option<&Arc<Tsdb>>,
    _client: Option<&Arc<Tsdb>>,
    sections: Vec<Section>,
) -> View {
    let mut view = View::new(data, sections);

    // Responses group - throughput and hit rate in one row
    let mut responses = Group::new("Responses", "responses");

    // Response rate
    responses.plot_promql(
        PlotOpts::line("Responses/sec", "responses-rate", Unit::Rate),
        "irate(responses_received[5m])".to_string(),
    );

    // Cache hit rate as percentage (0-1 range)
    responses.plot_promql(
        PlotOpts::line("Hit Rate", "hit-rate", Unit::Percentage),
        "irate(cache_hits[5m]) / (irate(cache_hits[5m]) + irate(cache_misses[5m]))".to_string(),
    );

    view.group(responses);

    // Latency overview group
    let mut latency = Group::new("Response Latency", "latency");

    // Response latency percentiles
    latency.plot_promql(
        PlotOpts::scatter(
            "Response Latency Percentiles",
            "response-latency-pct",
            Unit::Time,
        )
        .with_log_scale(true),
        "histogram_percentiles([0.5, 0.9, 0.99, 0.999, 0.9999], response_latency)".to_string(),
    );

    view.group(latency);

    view
}
