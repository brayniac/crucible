use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::process::Command;

#[derive(Debug, Deserialize)]
pub struct ContextExport {
    #[allow(dead_code)]
    pub id: String,
    #[allow(dead_code)]
    pub name: String,
    #[allow(dead_code)]
    pub state: String,
    pub experiments: Vec<ExperimentRef>,
}

#[derive(Debug, Deserialize)]
pub struct ExperimentRef {
    pub experiment_id: String,
    #[serde(default)]
    pub params: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct ExperimentExport {
    #[allow(dead_code)]
    pub id: String,
    #[allow(dead_code)]
    pub name: String,
    pub state: String,
    #[serde(default)]
    pub artifacts: Vec<ArtifactRef>,
    #[serde(default)]
    pub jobs: Vec<JobRef>,
}

#[derive(Debug, Deserialize)]
pub struct ArtifactRef {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub job_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct JobRef {
    pub id: String,
    pub name: String,
}

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
    pub state: String,
    pub metrics: BenchmarkMetrics,
}

/// All results for JSON/HTML export
#[derive(Debug, Serialize)]
pub struct BakeoffResults {
    pub experiments: HashMap<String, Vec<ExperimentResult>>,
}

/// Fetch results for a context by exporting and parsing the data.
pub fn fetch_context_results(
    context_id: &str,
) -> Result<Vec<ExperimentResult>, Box<dyn std::error::Error>> {
    // Export context and extract context.json using tar
    let output = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "systemslab export context {} -o - 2>/dev/null | tar -xOf - context.json",
            context_id
        ))
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Failed to export context: {}", stderr).into());
    }

    let context: ContextExport = serde_json::from_slice(&output.stdout)?;

    let mut results = Vec::new();

    for exp_ref in &context.experiments {
        // Extract experiment.json for each experiment
        let exp_output = Command::new("sh")
            .arg("-c")
            .arg(format!(
                "systemslab export context {} -o - 2>/dev/null | tar -xOf - {}/experiment.json",
                context_id, exp_ref.experiment_id
            ))
            .output()?;

        let (state, metrics) = if exp_output.status.success() {
            let exp: ExperimentExport = serde_json::from_slice(&exp_output.stdout)?;
            let metrics = if exp.state == "success" {
                extract_metrics(context_id, &exp)
            } else {
                BenchmarkMetrics::default()
            };
            (exp.state, metrics)
        } else {
            ("unknown".to_string(), BenchmarkMetrics::default())
        };

        let connections = exp_ref
            .params
            .get("connections")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let pipeline_depth = exp_ref
            .params
            .get("pipeline_depth")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        results.push(ExperimentResult {
            connections,
            pipeline_depth,
            state,
            metrics,
        });
    }

    // Sort by connections, then pipeline_depth
    results.sort_by(|a, b| {
        a.connections
            .cmp(&b.connections)
            .then(a.pipeline_depth.cmp(&b.pipeline_depth))
    });

    Ok(results)
}

/// Extract benchmark metrics from experiment logs.
fn extract_metrics(context_id: &str, exp: &ExperimentExport) -> BenchmarkMetrics {
    // Find the client job
    let client_job = exp.jobs.iter().find(|j| j.name == "client");
    let client_job_id = match client_job {
        Some(j) => &j.id,
        None => return BenchmarkMetrics::default(),
    };

    // Find the logs.json artifact for the client job
    let logs_artifact = exp
        .artifacts
        .iter()
        .find(|a| a.name == "logs.json" && a.job_id.as_ref() == Some(client_job_id));

    let artifact_id = match logs_artifact {
        Some(a) => &a.id,
        None => return BenchmarkMetrics::default(),
    };

    // Extract the logs file
    let output = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "systemslab export context {} -o - 2>/dev/null | tar -xOf - {}/artifact/{}",
            context_id, exp.id, artifact_id
        ))
        .output();

    let output = match output {
        Ok(o) if o.status.success() => o,
        _ => return BenchmarkMetrics::default(),
    };

    // Parse logs and extract metrics
    let logs: LogsFile = match serde_json::from_slice(&output.stdout) {
        Ok(l) => l,
        Err(_) => return BenchmarkMetrics::default(),
    };

    parse_benchmark_metrics(&logs)
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
            // Format:
            //   latency summary (msec):
            //           avg       min       p50       p95       p99       max
            //         0.167     0.016     0.159     0.287     0.343     2.527
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

/// Print results as a table.
pub fn print_results_table(experiment_name: &str, results: &[ExperimentResult]) {
    println!("{}:", experiment_name);
    println!(
        "  {:>6} {:>6} {:>10} {:>12} {:>10} {:>10} {:>10}",
        "conns", "pipe", "state", "throughput", "p50", "p99", "p99.9"
    );
    println!(
        "  {:->6} {:->6} {:->10} {:->12} {:->10} {:->10} {:->10}",
        "", "", "", "", "", "", ""
    );

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

        println!(
            "  {:>6} {:>6} {:>10} {:>12} {:>10} {:>10} {:>10}",
            r.connections, r.pipeline_depth, state_display, throughput, p50, p99, p999
        );
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
        const colors = {{
            'crucible-segcache': '#3498db',
            'crucible-valkey': '#e74c3c',
            'valkey-segcache': '#2ecc71',
            'valkey-valkey': '#9b59b6'
        }};

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

            for (const [name, results] of Object.entries(data.experiments)) {{
                const throughputData = sortedLabels.map(label => {{
                    const [c, p] = label.match(/\d+/g).map(Number);
                    const r = results.find(r => r.connections === c && r.pipeline_depth === p);
                    return r?.metrics?.throughput ? r.metrics.throughput / 1000000 : null;
                }});

                datasets.push({{
                    label: name,
                    data: throughputData,
                    backgroundColor: colors[name] + '80',
                    borderColor: colors[name],
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

            for (const [name, results] of Object.entries(data.experiments)) {{
                const throughputData = connections.map(c => {{
                    const r = results.find(r => r.connections === c && r.pipeline_depth === 1);
                    return r?.metrics?.throughput ? r.metrics.throughput / 1000000 : null;
                }});

                datasets.push({{
                    label: name,
                    data: throughputData,
                    borderColor: colors[name],
                    backgroundColor: colors[name] + '20',
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

            for (const [name, results] of Object.entries(data.experiments)) {{
                const throughputData = pipelines.map(p => {{
                    const r = results.find(r => r.connections === 256 && r.pipeline_depth === p);
                    return r?.metrics?.throughput ? r.metrics.throughput / 1000000 : null;
                }});

                datasets.push({{
                    label: name,
                    data: throughputData,
                    borderColor: colors[name],
                    backgroundColor: colors[name] + '20',
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

            for (const [name, results] of Object.entries(data.experiments)) {{
                const latencyData = sortedLabels.map(label => {{
                    const [c, p] = label.match(/\d+/g).map(Number);
                    const r = results.find(r => r.connections === c && r.pipeline_depth === p);
                    return r?.metrics?.p99_us ? r.metrics.p99_us / 1000 : null;
                }});

                datasets.push({{
                    label: name,
                    data: latencyData,
                    backgroundColor: colors[name] + '80',
                    borderColor: colors[name],
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

            for (const [name, results] of Object.entries(data.experiments)) {{
                const points = results
                    .filter(r => r.metrics.throughput && r.metrics.p99_us)
                    .map(r => ({{
                        x: r.metrics.throughput / 1000000,
                        y: r.metrics.p99_us / 1000
                    }}));

                datasets.push({{
                    label: name,
                    data: points,
                    backgroundColor: colors[name],
                    borderColor: colors[name],
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

            for (const [name, results] of Object.entries(data.experiments)) {{
                const latencyData = configs.map(cfg => {{
                    const [c, p] = cfg.match(/\d+/g).map(Number);
                    const r = results.find(r => r.connections === c && r.pipeline_depth === p);
                    return r?.metrics?.p99_us ? r.metrics.p99_us / 1000 : null;
                }});

                datasets.push({{
                    label: name,
                    data: latencyData,
                    borderColor: colors[name],
                    backgroundColor: colors[name] + '20',
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
