use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const SYSTEMSLAB_URL: &str = "http://systemslab";

// ============================================================================
// GraphQL types
// ============================================================================

#[derive(Debug, Deserialize)]
struct GraphQLResponse<T> {
    data: Option<T>,
    #[allow(dead_code)]
    errors: Option<Vec<GraphQLError>>,
}

#[derive(Debug, Deserialize)]
struct GraphQLError {
    #[allow(dead_code)]
    message: String,
}

#[derive(Debug, Deserialize)]
struct ContextByIdData {
    #[serde(rename = "contextById")]
    context_by_id: Option<ContextData>,
}

#[derive(Debug, Deserialize)]
struct ContextData {
    #[allow(dead_code)]
    id: String,
    #[allow(dead_code)]
    name: String,
    experiments: Vec<ContextExperimentData>,
}

#[derive(Debug, Deserialize)]
struct ContextExperimentData {
    #[serde(rename = "experimentId")]
    #[allow(dead_code)]
    experiment_id: String,
    params: HashMap<String, String>,
    experiment: ExperimentData,
}

#[derive(Debug, Deserialize)]
struct ExperimentData {
    id: String,
    #[allow(dead_code)]
    name: String,
    state: String,
    job: Vec<JobData>,
}

#[derive(Debug, Deserialize)]
struct JobData {
    #[allow(dead_code)]
    id: String,
    name: String,
    artifact: Vec<ArtifactData>,
}

#[derive(Debug, Deserialize)]
struct ArtifactData {
    id: String,
    name: String,
}

// ============================================================================
// Export types (for legacy fallback)
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct LogsFile {
    #[allow(dead_code)]
    pub version: String,
    pub events: Vec<LogEvent>,
}

#[derive(Debug, Deserialize)]
pub struct LogEvent {
    #[serde(default)]
    pub text: Option<String>,
}

// ============================================================================
// Result types
// ============================================================================

#[derive(Debug, Default, Serialize)]
pub struct BenchmarkMetrics {
    pub throughput: Option<f64>,
    pub p50_us: Option<f64>,
    pub p99_us: Option<f64>,
    pub p999_us: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct ExperimentResult {
    pub connections: usize,
    pub pipeline_depth: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_size: Option<usize>,
    pub state: String,
    pub metrics: BenchmarkMetrics,
}

/// All results for JSON/HTML export
#[derive(Debug, Serialize)]
pub struct BakeoffResults {
    pub experiments: HashMap<String, Vec<ExperimentResult>>,
}

// ============================================================================
// GraphQL-based fast fetching
// ============================================================================

/// Intermediate struct for parallel processing
struct ExperimentInfo {
    connections: usize,
    pipeline_depth: usize,
    state: String,
    experiment_id: String,
    jobs: Vec<JobData>,
}

/// Information about a failed experiment for retry purposes.
#[derive(Debug)]
pub struct FailedExperiment {
    pub label: String,
    pub connections: usize,
    pub pipeline_depth: usize,
}

/// Fetch the list of failed experiments from a context.
/// Returns experiment labels in the format "{exp_name}_c{connections}_p{pipeline_depth}".
pub fn fetch_failed_experiments(
    context_id: &str,
    exp_name: &str,
) -> Result<Vec<FailedExperiment>, Box<dyn std::error::Error>> {
    let results = fetch_context_results(context_id)?;

    Ok(results
        .into_iter()
        .filter(|r| r.state == "failure")
        .map(|r| FailedExperiment {
            label: format!("{}_c{}_p{}", exp_name, r.connections, r.pipeline_depth),
            connections: r.connections,
            pipeline_depth: r.pipeline_depth,
        })
        .collect())
}

/// Fetch results for a context using GraphQL (fast path).
pub fn fetch_context_results(
    context_id: &str,
) -> Result<Vec<ExperimentResult>, Box<dyn std::error::Error>> {
    // Query experiments and their states via GraphQL
    let query = format!(
        r#"{{
            contextById(id: "{}") {{
                id
                name
                experiments {{
                    experimentId
                    params
                    experiment {{
                        id
                        name
                        state
                        job {{
                            id
                            name
                            artifact {{
                                id
                                name
                            }}
                        }}
                    }}
                }}
            }}
        }}"#,
        context_id
    );

    let response: GraphQLResponse<ContextByIdData> =
        ureq::post(&format!("{}/api/graphql", SYSTEMSLAB_URL))
            .set("Content-Type", "application/json")
            .send_json(serde_json::json!({ "query": query }))?
            .into_json()?;

    let context = response
        .data
        .and_then(|d| d.context_by_id)
        .ok_or("Context not found")?;

    // Collect experiment info for parallel processing
    let experiment_infos: Vec<ExperimentInfo> = context
        .experiments
        .into_iter()
        .map(|ctx_exp| {
            let connections = ctx_exp
                .params
                .get("connections")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let pipeline_depth = ctx_exp
                .params
                .get("pipeline_depth")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            ExperimentInfo {
                connections,
                pipeline_depth,
                state: ctx_exp.experiment.state,
                experiment_id: ctx_exp.experiment.id,
                jobs: ctx_exp.experiment.job,
            }
        })
        .collect();

    // Fetch metrics in parallel for successful experiments
    let mut results: Vec<ExperimentResult> = experiment_infos
        .into_par_iter()
        .map(|info| {
            let metrics = if info.state == "success" {
                fetch_experiment_metrics(&info.experiment_id, &info.jobs)
            } else {
                BenchmarkMetrics::default()
            };

            ExperimentResult {
                connections: info.connections,
                pipeline_depth: info.pipeline_depth,
                value_size: None,
                state: info.state,
                metrics,
            }
        })
        .collect();

    // Sort by connections, then pipeline_depth
    results.sort_by(|a, b| {
        a.connections
            .cmp(&b.connections)
            .then(a.pipeline_depth.cmp(&b.pipeline_depth))
    });

    // Filter out experiments with fewer than 8 connections
    results.retain(|r| r.connections >= 8);

    Ok(results)
}

/// Fetch results for a native IO context (includes value_size).
pub fn fetch_native_context_results(
    context_id: &str,
) -> Result<Vec<ExperimentResult>, Box<dyn std::error::Error>> {
    // Query experiments and their states via GraphQL
    let query = format!(
        r#"{{
            contextById(id: "{}") {{
                id
                name
                experiments {{
                    experimentId
                    params
                    experiment {{
                        id
                        name
                        state
                        job {{
                            id
                            name
                            artifact {{
                                id
                                name
                            }}
                        }}
                    }}
                }}
            }}
        }}"#,
        context_id
    );

    let response: GraphQLResponse<ContextByIdData> =
        ureq::post(&format!("{}/api/graphql", SYSTEMSLAB_URL))
            .set("Content-Type", "application/json")
            .send_json(serde_json::json!({ "query": query }))?
            .into_json()?;

    let context = response
        .data
        .and_then(|d| d.context_by_id)
        .ok_or("Context not found")?;

    // Collect experiment info with value_size
    struct NativeExperimentInfo {
        connections: usize,
        pipeline_depth: usize,
        value_size: usize,
        state: String,
        experiment_id: String,
        jobs: Vec<JobData>,
    }

    let experiment_infos: Vec<NativeExperimentInfo> = context
        .experiments
        .into_iter()
        .map(|ctx_exp| {
            let connections = ctx_exp
                .params
                .get("connections")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let pipeline_depth = ctx_exp
                .params
                .get("pipeline_depth")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let value_size = ctx_exp
                .params
                .get("value_length")
                .and_then(|s| s.parse().ok())
                .unwrap_or(64); // default to 64 if not specified

            NativeExperimentInfo {
                connections,
                pipeline_depth,
                value_size,
                state: ctx_exp.experiment.state,
                experiment_id: ctx_exp.experiment.id,
                jobs: ctx_exp.experiment.job,
            }
        })
        .collect();

    // Fetch metrics in parallel for successful experiments
    let mut results: Vec<ExperimentResult> = experiment_infos
        .into_par_iter()
        .map(|info| {
            let metrics = if info.state == "success" {
                fetch_experiment_metrics(&info.experiment_id, &info.jobs)
            } else {
                BenchmarkMetrics::default()
            };

            ExperimentResult {
                connections: info.connections,
                pipeline_depth: info.pipeline_depth,
                value_size: Some(info.value_size),
                state: info.state,
                metrics,
            }
        })
        .collect();

    // Sort by connections, then pipeline_depth, then value_size
    results.sort_by(|a, b| {
        a.connections
            .cmp(&b.connections)
            .then(a.pipeline_depth.cmp(&b.pipeline_depth))
            .then(a.value_size.cmp(&b.value_size))
    });

    // Filter out experiments with fewer than 8 connections
    results.retain(|r| r.connections >= 8);

    Ok(results)
}

/// Fetch metrics for a successful experiment by downloading the logs artifact.
fn fetch_experiment_metrics(_experiment_id: &str, jobs: &[JobData]) -> BenchmarkMetrics {
    // Find the client job
    let client_job = match jobs.iter().find(|j| j.name == "client") {
        Some(j) => j,
        None => return BenchmarkMetrics::default(),
    };

    // Find the logs.json artifact
    let logs_artifact = match client_job.artifact.iter().find(|a| a.name == "logs.json") {
        Some(a) => a,
        None => return BenchmarkMetrics::default(),
    };

    // Download artifact directly via HTTP (UUID without hyphens)
    let artifact_id_no_hyphens = logs_artifact.id.replace('-', "");
    let url = format!(
        "{}/api/v1/artifact/{}",
        SYSTEMSLAB_URL, artifact_id_no_hyphens
    );

    let response = match ureq::get(&url).call() {
        Ok(r) => r,
        Err(_) => return BenchmarkMetrics::default(),
    };

    // Parse logs and extract metrics
    match response.into_json::<LogsFile>() {
        Ok(logs) => parse_benchmark_metrics(&logs),
        Err(_) => BenchmarkMetrics::default(),
    }
}

/// Parse benchmark summary metrics from log events.
fn parse_benchmark_metrics(logs: &LogsFile) -> BenchmarkMetrics {
    let mut metrics = BenchmarkMetrics::default();
    let mut saw_latency_header = false;

    for event in &logs.events {
        if let Some(text) = &event.text {
            // crucible-benchmark format
            if text.contains("throughput:") && text.contains("req/s") {
                // Format: "throughput: 773993.23 req/s"
                if let Some(val) = extract_float_before(text, "req/s") {
                    metrics.throughput = Some(val);
                }
            } else if text.contains("latency p50:") {
                // Format: "latency p50: 329.73 us"
                if let Some(val) = extract_float_before(text, "us") {
                    metrics.p50_us = Some(val);
                }
            } else if text.contains("latency p99:") && !text.contains("p99.") {
                // Format: "latency p99: 507.90 us"
                if let Some(val) = extract_float_before(text, "us") {
                    metrics.p99_us = Some(val);
                }
            } else if text.contains("latency p99.9:") && !text.contains("p99.99") {
                // Format: "latency p99.9: 917.50 us"
                if let Some(val) = extract_float_before(text, "us") {
                    metrics.p999_us = Some(val);
                }
            }
            // valkey-benchmark format: "GET: 867313.0 requests per second, p50=0.287 msec"
            else if text.contains("GET:") && text.contains("requests per second") {
                if let Some(val) = extract_float_before(text, "requests per second") {
                    metrics.throughput = Some(val);
                }
                // p50 in msec
                if let Some(idx) = text.find("p50=") {
                    let after = &text[idx + 4..];
                    if let Some(val) = extract_float_before(after, "msec") {
                        metrics.p50_us = Some(val * 1000.0); // convert ms to us
                    }
                }
            }
            // valkey-benchmark final summary: "throughput summary: 865201.56 requests per second"
            else if text.contains("throughput summary:") && text.contains("requests per second") {
                if let Some(val) = extract_float_before(text, "requests per second") {
                    metrics.throughput = Some(val);
                }
            }
            // valkey-benchmark latency summary table header (verbose mode)
            else if text.contains("latency summary (msec):") {
                saw_latency_header = true;
            } else if saw_latency_header && text.trim().starts_with("avg") {
                // This is the column header line, skip it but stay in latency mode
            } else if saw_latency_header {
                // Try to parse the values line: "0.167     0.016     0.159     0.287     0.343     2.527"
                let parts: Vec<&str> = text.split_whitespace().collect();
                if parts.len() >= 6 {
                    // Format: avg, min, p50, p95, p99, max (all in msec)
                    if let Ok(p50_ms) = parts[2].parse::<f64>() {
                        metrics.p50_us = Some(p50_ms * 1000.0);
                    }
                    if let Ok(p99_ms) = parts[4].parse::<f64>() {
                        metrics.p99_us = Some(p99_ms * 1000.0);
                    }
                }
                saw_latency_header = false;
            }
        }
    }

    metrics
}

/// Extract a float value that appears before a given suffix.
fn extract_float_before(text: &str, suffix: &str) -> Option<f64> {
    let idx = text.find(suffix)?;
    let before = text[..idx].trim();
    let parts: Vec<&str> = before.split_whitespace().collect();
    parts.last()?.parse().ok()
}

// ============================================================================
// Output formatting
// ============================================================================

/// Print results as a table.
pub fn print_results_table(experiment_name: &str, results: &[ExperimentResult]) {
    // Check if any results have value_size to determine table format
    let has_value_size = results.iter().any(|r| r.value_size.is_some());

    println!("{}:", experiment_name);
    if has_value_size {
        println!(
            "  {:>6} {:>6} {:>8} {:>10} {:>12} {:>10} {:>10} {:>10}",
            "conns", "pipe", "vsize", "state", "throughput", "p50", "p99", "p99.9"
        );
        println!(
            "  {:->6} {:->6} {:->8} {:->10} {:->12} {:->10} {:->10} {:->10}",
            "", "", "", "", "", "", "", ""
        );
    } else {
        println!(
            "  {:>6} {:>6} {:>10} {:>12} {:>10} {:>10} {:>10}",
            "conns", "pipe", "state", "throughput", "p50", "p99", "p99.9"
        );
        println!(
            "  {:->6} {:->6} {:->10} {:->12} {:->10} {:->10} {:->10}",
            "", "", "", "", "", "", ""
        );
    }

    for r in results {
        let state_display = match r.state.as_str() {
            "success" => "ok",
            "failure" => "FAILED",
            "running" => "running",
            "pending" => "pending",
            _ => &r.state,
        };

        let throughput = r
            .metrics
            .throughput
            .map(format_throughput)
            .unwrap_or_else(|| "-".to_string());

        let p50 = r
            .metrics
            .p50_us
            .map(format_latency)
            .unwrap_or_else(|| "-".to_string());

        let p99 = r
            .metrics
            .p99_us
            .map(format_latency)
            .unwrap_or_else(|| "-".to_string());

        let p999 = r
            .metrics
            .p999_us
            .map(format_latency)
            .unwrap_or_else(|| "-".to_string());

        if has_value_size {
            let vsize = r
                .value_size
                .map(format_value_size)
                .unwrap_or_else(|| "-".to_string());
            println!(
                "  {:>6} {:>6} {:>8} {:>10} {:>12} {:>10} {:>10} {:>10}",
                r.connections, r.pipeline_depth, vsize, state_display, throughput, p50, p99, p999
            );
        } else {
            println!(
                "  {:>6} {:>6} {:>10} {:>12} {:>10} {:>10} {:>10}",
                r.connections, r.pipeline_depth, state_display, throughput, p50, p99, p999
            );
        }
    }
    println!();
}

fn format_throughput(val: f64) -> String {
    if val >= 1_000_000.0 {
        format!("{:.2}M/s", val / 1_000_000.0)
    } else if val >= 1_000.0 {
        format!("{:.1}K/s", val / 1_000.0)
    } else {
        format!("{:.0}/s", val)
    }
}

fn format_latency(us: f64) -> String {
    if us >= 1000.0 {
        format!("{:.2}ms", us / 1000.0)
    } else {
        format!("{:.0}us", us)
    }
}

fn format_value_size(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{}MB", bytes / (1024 * 1024))
    } else if bytes >= 1024 {
        format!("{}KB", bytes / 1024)
    } else {
        format!("{}B", bytes)
    }
}

/// Print results as JSON.
pub fn print_results_json(results: &BakeoffResults) {
    println!("{}", serde_json::to_string_pretty(results).unwrap());
}

/// Generate an HTML report with interactive charts.
pub fn generate_html_report(results: &BakeoffResults) -> String {
    let json_data = serde_json::to_string(results).unwrap();

    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Crucible Bakeoff Results</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
            color: #333;
        }}
        h1 {{
            text-align: center;
            color: #2c3e50;
            margin-bottom: 30px;
        }}
        h2 {{
            color: #34495e;
            border-bottom: 2px solid #3498db;
            padding-bottom: 10px;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}
        .summary-card {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .summary-card h3 {{
            margin-top: 0;
            color: #2c3e50;
        }}
        .metric {{
            font-size: 2em;
            font-weight: bold;
            color: #3498db;
        }}
        .metric-label {{
            color: #7f8c8d;
            font-size: 0.9em;
        }}
        .chart-container {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 30px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .chart-row {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }}
        @media (max-width: 900px) {{
            .chart-row {{
                grid-template-columns: 1fr;
            }}
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th, td {{
            padding: 12px 15px;
            text-align: right;
            border-bottom: 1px solid #ecf0f1;
        }}
        th {{
            background: #3498db;
            color: white;
            font-weight: 600;
        }}
        th:first-child, td:first-child {{
            text-align: left;
        }}
        tr:hover {{
            background: #f8f9fa;
        }}
        .winner {{
            background: #d5f5e3;
        }}
        .tabs {{
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
        }}
        .tab {{
            padding: 10px 20px;
            background: white;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 14px;
            transition: all 0.2s;
        }}
        .tab:hover {{
            background: #3498db;
            color: white;
        }}
        .tab.active {{
            background: #3498db;
            color: white;
        }}
        .view {{
            display: none;
        }}
        .view.active {{
            display: block;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Crucible Bakeoff Results</h1>

        <div class="summary" id="summary"></div>

        <div class="tabs">
            <button class="tab active" onclick="showView('throughput')">Throughput</button>
            <button class="tab" onclick="showView('latency')">Latency</button>
            <button class="tab" onclick="showView('data')">Raw Data</button>
        </div>

        <div id="throughput" class="view active">
            <h2>Throughput by Configuration</h2>
            <div class="chart-container">
                <canvas id="throughputChart"></canvas>
            </div>
            <div class="chart-row">
                <div class="chart-container">
                    <h3>Throughput vs Connections (pipeline=1)</h3>
                    <canvas id="throughputByConns"></canvas>
                </div>
                <div class="chart-container">
                    <h3>Throughput vs Pipeline Depth (conns=256)</h3>
                    <canvas id="throughputByPipeline"></canvas>
                </div>
            </div>
        </div>

        <div id="latency" class="view">
            <h2>Latency (p99) by Configuration</h2>
            <div class="chart-container">
                <canvas id="latencyChart"></canvas>
            </div>
            <div class="chart-row">
                <div class="chart-container">
                    <h3>Throughput vs p99 Latency</h3>
                    <canvas id="throughputVsLatency"></canvas>
                </div>
                <div class="chart-container">
                    <h3>p99 Latency at Different Load Levels</h3>
                    <canvas id="latencyByLoad"></canvas>
                </div>
            </div>
        </div>

        <div id="data" class="view">
            <h2>Raw Data</h2>
            <div id="dataTables"></div>
        </div>
    </div>

    <script>
        const data = {json_data};
        const defaultColors = [
            '#3498db', '#e74c3c', '#2ecc71', '#9b59b6',
            '#f39c12', '#1abc9c', '#34495e', '#e67e22'
        ];

        function getColor(name, index) {{
            const colorMap = {{
                'crucible-segcache': '#3498db',
                'crucible-valkey': '#e74c3c',
                'valkey-segcache': '#2ecc71',
                'valkey-valkey': '#9b59b6',
                'uring-uring': '#3498db',
                'uring-mio': '#e74c3c',
                'uring-tokio': '#2ecc71',
                'mio-uring': '#9b59b6',
                'mio-mio': '#f39c12',
                'mio-tokio': '#1abc9c'
            }};
            return colorMap[name] || defaultColors[index % defaultColors.length];
        }}

        function formatThroughput(val) {{
            if (!val) return '-';
            if (val >= 1000000) return (val / 1000000).toFixed(2) + 'M/s';
            if (val >= 1000) return (val / 1000).toFixed(1) + 'K/s';
            return val.toFixed(0) + '/s';
        }}

        function formatLatency(us) {{
            if (!us) return '-';
            if (us >= 1000) return (us / 1000).toFixed(2) + 'ms';
            return us.toFixed(0) + 'us';
        }}

        function showView(viewId) {{
            document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.getElementById(viewId).classList.add('active');
            event.target.classList.add('active');
        }}

        // Build summary cards
        function buildSummary() {{
            const summary = document.getElementById('summary');
            let html = '';

            for (const [name, results] of Object.entries(data.experiments)) {{
                const successResults = results.filter(r => r.state === 'success' && r.metrics.throughput);
                if (successResults.length === 0) continue;

                const maxThroughput = Math.max(...successResults.map(r => r.metrics.throughput));
                const bestResult = successResults.find(r => r.metrics.throughput === maxThroughput);

                html += `
                    <div class="summary-card">
                        <h3>${{name}}</h3>
                        <div class="metric">${{formatThroughput(maxThroughput)}}</div>
                        <div class="metric-label">Peak throughput at c${{bestResult.connections}}/p${{bestResult.pipeline_depth}}</div>
                    </div>
                `;
            }}
            summary.innerHTML = html;
        }}

        // Build throughput chart
        function buildThroughputChart() {{
            const ctx = document.getElementById('throughputChart').getContext('2d');
            const datasets = [];
            const labels = new Set();

            for (const [name, results] of Object.entries(data.experiments)) {{
                results.forEach(r => labels.add(`c${{r.connections}}/p${{r.pipeline_depth}}`));
            }}

            const sortedLabels = Array.from(labels).sort((a, b) => {{
                const [ca, pa] = a.match(/\d+/g).map(Number);
                const [cb, pb] = b.match(/\d+/g).map(Number);
                return ca - cb || pa - pb;
            }});

            let idx = 0;
            for (const [name, results] of Object.entries(data.experiments)) {{
                const color = getColor(name, idx++);
                const throughputData = sortedLabels.map(label => {{
                    const [c, p] = label.match(/\d+/g).map(Number);
                    const r = results.find(r => r.connections === c && r.pipeline_depth === p);
                    return r?.metrics?.throughput ? r.metrics.throughput / 1000000 : null;
                }});

                datasets.push({{
                    label: name,
                    data: throughputData,
                    backgroundColor: color + '80',
                    borderColor: color,
                    borderWidth: 2
                }});
            }}

            new Chart(ctx, {{
                type: 'bar',
                data: {{ labels: sortedLabels, datasets }},
                options: {{
                    responsive: true,
                    plugins: {{
                        title: {{ display: false }},
                        legend: {{ position: 'top' }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            title: {{ display: true, text: 'Throughput (M req/s)' }}
                        }}
                    }}
                }}
            }});
        }}

        // Build throughput by connections chart
        function buildThroughputByConns() {{
            const ctx = document.getElementById('throughputByConns').getContext('2d');
            const connections = [64, 256, 512, 1024];
            const datasets = [];

            let idx = 0;
            for (const [name, results] of Object.entries(data.experiments)) {{
                const color = getColor(name, idx++);
                const throughputData = connections.map(c => {{
                    const r = results.find(r => r.connections === c && r.pipeline_depth === 1);
                    return r?.metrics?.throughput ? r.metrics.throughput / 1000000 : null;
                }});

                datasets.push({{
                    label: name,
                    data: throughputData,
                    borderColor: color,
                    backgroundColor: color + '20',
                    tension: 0.3,
                    fill: false
                }});
            }}

            new Chart(ctx, {{
                type: 'line',
                data: {{ labels: connections.map(c => c.toString()), datasets }},
                options: {{
                    responsive: true,
                    plugins: {{ legend: {{ position: 'top' }} }},
                    scales: {{
                        x: {{ title: {{ display: true, text: 'Connections' }} }},
                        y: {{ beginAtZero: true, title: {{ display: true, text: 'Throughput (M req/s)' }} }}
                    }}
                }}
            }});
        }}

        // Build throughput by pipeline chart
        function buildThroughputByPipeline() {{
            const ctx = document.getElementById('throughputByPipeline').getContext('2d');
            const pipelines = [1, 8, 32, 64, 128];
            const datasets = [];

            let idx = 0;
            for (const [name, results] of Object.entries(data.experiments)) {{
                const color = getColor(name, idx++);
                const throughputData = pipelines.map(p => {{
                    const r = results.find(r => r.connections === 256 && r.pipeline_depth === p);
                    return r?.metrics?.throughput ? r.metrics.throughput / 1000000 : null;
                }});

                datasets.push({{
                    label: name,
                    data: throughputData,
                    borderColor: color,
                    backgroundColor: color + '20',
                    tension: 0.3,
                    fill: false
                }});
            }}

            new Chart(ctx, {{
                type: 'line',
                data: {{ labels: pipelines.map(p => p.toString()), datasets }},
                options: {{
                    responsive: true,
                    plugins: {{ legend: {{ position: 'top' }} }},
                    scales: {{
                        x: {{ title: {{ display: true, text: 'Pipeline Depth' }} }},
                        y: {{ beginAtZero: true, title: {{ display: true, text: 'Throughput (M req/s)' }} }}
                    }}
                }}
            }});
        }}

        // Build latency chart
        function buildLatencyChart() {{
            const ctx = document.getElementById('latencyChart').getContext('2d');
            const datasets = [];
            const labels = new Set();

            for (const [name, results] of Object.entries(data.experiments)) {{
                results.forEach(r => labels.add(`c${{r.connections}}/p${{r.pipeline_depth}}`));
            }}

            const sortedLabels = Array.from(labels).sort((a, b) => {{
                const [ca, pa] = a.match(/\d+/g).map(Number);
                const [cb, pb] = b.match(/\d+/g).map(Number);
                return ca - cb || pa - pb;
            }});

            let idx = 0;
            for (const [name, results] of Object.entries(data.experiments)) {{
                const color = getColor(name, idx++);
                const latencyData = sortedLabels.map(label => {{
                    const [c, p] = label.match(/\d+/g).map(Number);
                    const r = results.find(r => r.connections === c && r.pipeline_depth === p);
                    return r?.metrics?.p99_us ? r.metrics.p99_us / 1000 : null;
                }});

                datasets.push({{
                    label: name,
                    data: latencyData,
                    backgroundColor: color + '80',
                    borderColor: color,
                    borderWidth: 2
                }});
            }}

            new Chart(ctx, {{
                type: 'bar',
                data: {{ labels: sortedLabels, datasets }},
                options: {{
                    responsive: true,
                    plugins: {{ legend: {{ position: 'top' }} }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            title: {{ display: true, text: 'p99 Latency (ms)' }}
                        }}
                    }}
                }}
            }});
        }}

        // Build throughput vs latency scatter plot
        function buildThroughputVsLatency() {{
            const ctx = document.getElementById('throughputVsLatency').getContext('2d');
            const datasets = [];

            let idx = 0;
            for (const [name, results] of Object.entries(data.experiments)) {{
                const color = getColor(name, idx++);
                const points = results
                    .filter(r => r.metrics.throughput && r.metrics.p99_us)
                    .map(r => ({{
                        x: r.metrics.throughput / 1000000,
                        y: r.metrics.p99_us / 1000
                    }}));

                datasets.push({{
                    label: name,
                    data: points,
                    backgroundColor: color,
                    borderColor: color,
                    pointRadius: 6
                }});
            }}

            new Chart(ctx, {{
                type: 'scatter',
                data: {{ datasets }},
                options: {{
                    responsive: true,
                    plugins: {{ legend: {{ position: 'top' }} }},
                    scales: {{
                        x: {{ title: {{ display: true, text: 'Throughput (M req/s)' }} }},
                        y: {{ title: {{ display: true, text: 'p99 Latency (ms)' }} }}
                    }}
                }}
            }});
        }}

        // Build latency by load chart
        function buildLatencyByLoad() {{
            const ctx = document.getElementById('latencyByLoad').getContext('2d');
            const configs = ['c64/p1', 'c256/p1', 'c256/p32', 'c1024/p64'];
            const datasets = [];

            let idx = 0;
            for (const [name, results] of Object.entries(data.experiments)) {{
                const color = getColor(name, idx++);
                const latencyData = configs.map(cfg => {{
                    const [c, p] = cfg.match(/\d+/g).map(Number);
                    const r = results.find(r => r.connections === c && r.pipeline_depth === p);
                    return r?.metrics?.p99_us ? r.metrics.p99_us / 1000 : null;
                }});

                datasets.push({{
                    label: name,
                    data: latencyData,
                    borderColor: color,
                    backgroundColor: color + '20',
                    tension: 0.3,
                    fill: false
                }});
            }}

            new Chart(ctx, {{
                type: 'line',
                data: {{ labels: configs, datasets }},
                options: {{
                    responsive: true,
                    plugins: {{ legend: {{ position: 'top' }} }},
                    scales: {{
                        x: {{ title: {{ display: true, text: 'Configuration' }} }},
                        y: {{ title: {{ display: true, text: 'p99 Latency (ms)' }} }}
                    }}
                }}
            }});
        }}

        // Build raw data tables
        function buildDataTables() {{
            let html = '';

            for (const [name, results] of Object.entries(data.experiments)) {{
                html += `<h3>${{name}}</h3><table>
                    <thead>
                        <tr>
                            <th>Connections</th>
                            <th>Pipeline</th>
                            <th>State</th>
                            <th>Throughput</th>
                            <th>p50</th>
                            <th>p99</th>
                            <th>p99.9</th>
                        </tr>
                    </thead>
                    <tbody>
                `;

                results.forEach(r => {{
                    html += `
                        <tr>
                            <td>${{r.connections}}</td>
                            <td>${{r.pipeline_depth}}</td>
                            <td>${{r.state === 'success' ? 'ok' : r.state}}</td>
                            <td>${{formatThroughput(r.metrics.throughput)}}</td>
                            <td>${{formatLatency(r.metrics.p50_us)}}</td>
                            <td>${{formatLatency(r.metrics.p99_us)}}</td>
                            <td>${{formatLatency(r.metrics.p999_us)}}</td>
                        </tr>
                    `;
                }});

                html += '</tbody></table>';
            }}

            document.getElementById('dataTables').innerHTML = html;
        }}

        // Initialize all charts
        buildSummary();
        buildThroughputChart();
        buildThroughputByConns();
        buildThroughputByPipeline();
        buildLatencyChart();
        buildThroughputVsLatency();
        buildLatencyByLoad();
        buildDataTables();
    </script>
</body>
</html>
"##
    )
}
