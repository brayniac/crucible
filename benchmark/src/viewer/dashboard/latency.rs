use super::*;

pub fn generate(
    data: &Arc<Tsdb>,
    _server: Option<&Arc<Tsdb>>,
    _client: Option<&Arc<Tsdb>>,
    sections: Vec<Section>,
) -> View {
    let mut view = View::new(data, sections);

    // Overall response latency
    let mut response = Group::new("Response Latency", "response-latency");

    response.plot_promql(
        PlotOpts::scatter(
            "Response Latency Percentiles",
            "response-latency-pct",
            Unit::Time,
        )
        .with_log_scale(true),
        "histogram_percentiles([0.5, 0.9, 0.99, 0.999, 0.9999], response_latency)".to_string(),
    );

    view.group(response);

    // GET latency
    let mut get_latency = Group::new("GET Latency", "get-latency");

    get_latency.plot_promql(
        PlotOpts::scatter("GET Latency Percentiles", "get-latency-pct", Unit::Time)
            .with_log_scale(true),
        "histogram_percentiles([0.5, 0.9, 0.99, 0.999, 0.9999], get_latency)".to_string(),
    );

    view.group(get_latency);

    // SET latency
    let mut set_latency = Group::new("SET Latency", "set-latency");

    set_latency.plot_promql(
        PlotOpts::scatter("SET Latency Percentiles", "set-latency-pct", Unit::Time)
            .with_log_scale(true),
        "histogram_percentiles([0.5, 0.9, 0.99, 0.999, 0.9999], set_latency)".to_string(),
    );

    view.group(set_latency);

    // DELETE latency
    let mut delete_latency = Group::new("DELETE Latency", "delete-latency");

    delete_latency.plot_promql(
        PlotOpts::scatter(
            "DELETE Latency Percentiles",
            "delete-latency-pct",
            Unit::Time,
        )
        .with_log_scale(true),
        "histogram_percentiles([0.5, 0.9, 0.99, 0.999, 0.9999], delete_latency)".to_string(),
    );

    view.group(delete_latency);

    view
}
