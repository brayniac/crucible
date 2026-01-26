use promql_parser::parser::{self, Expr, token::TokenType};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use thiserror::Error;

use crate::viewer::tsdb::{Labels, Tsdb};
use tracing::debug;

mod api;

pub use api::routes;

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Evaluation error: {0}")]
    EvaluationError(String),

    #[allow(dead_code)]
    #[error("Unsupported operation: {0}")]
    Unsupported(String),

    #[error("Metric not found: {0}")]
    MetricNotFound(String),
}

/// A single sample in the result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    pub metric: HashMap<String, String>,
    pub value: (f64, f64), // (timestamp_seconds, value)
}

/// A matrix sample with multiple values over time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixSample {
    pub metric: HashMap<String, String>,
    pub values: Vec<(f64, f64)>, // Vec of (timestamp_seconds, value)
}

/// Histogram heatmap data for visualization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramHeatmapResult {
    /// Timestamps in seconds
    pub timestamps: Vec<f64>,
    /// Bucket boundaries (latency values in the histogram's unit, e.g., nanoseconds)
    pub bucket_bounds: Vec<u64>,
    /// Heatmap data as [time_index, bucket_index, count]
    pub data: Vec<(usize, usize, f64)>,
    /// Minimum count value (for color scaling)
    pub min_value: f64,
    /// Maximum count value (for color scaling)
    pub max_value: f64,
}

/// Result of a PromQL query
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "resultType", rename_all = "camelCase")]
pub enum QueryResult {
    #[serde(rename = "vector")]
    Vector { result: Vec<Sample> },

    #[serde(rename = "matrix")]
    Matrix { result: Vec<MatrixSample> },

    #[serde(rename = "scalar")]
    Scalar { result: (f64, f64) }, // (timestamp, value)

    #[serde(rename = "histogram_heatmap")]
    HistogramHeatmap { result: HistogramHeatmapResult },
}

/// The PromQL query engine
pub struct QueryEngine {
    tsdb: Arc<Tsdb>,
}

impl QueryEngine {
    pub fn new(tsdb: Arc<Tsdb>) -> Self {
        Self { tsdb }
    }

    /// Get the time range (min, max) of all data in seconds
    pub fn get_time_range(&self) -> (f64, f64) {
        let mut min_time = f64::INFINITY;
        let mut max_time = f64::NEG_INFINITY;

        // Try a few known metrics to get the time range
        let known_metrics = [
            "requests_sent",
            "responses_received",
            "cache_hits",
            "cpu_cores",
            "memory_total",
        ];
        for metric_name in &known_metrics {
            if let Some(collection) = self.tsdb.gauges(metric_name, Labels::default()) {
                let sum_series = collection.filtered_sum(&Labels::default());
                for (&timestamp_ns, _) in sum_series.inner.iter() {
                    let timestamp_s = timestamp_ns as f64 / 1e9;
                    min_time = min_time.min(timestamp_s);
                    max_time = max_time.max(timestamp_s);
                }
            }

            if let Some(collection) = self.tsdb.counters(metric_name, Labels::default()) {
                let rate_collection = collection.filtered_rate(&Labels::default());
                let sum_series = rate_collection.sum();
                for (&timestamp_ns, _) in sum_series.inner.iter() {
                    let timestamp_s = timestamp_ns as f64 / 1e9;
                    min_time = min_time.min(timestamp_s);
                    max_time = max_time.max(timestamp_s);
                }
            }
        }

        if min_time == f64::INFINITY {
            // No data found, return a reasonable default
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64();
            (now - 3600.0, now) // 1 hour ago to now
        } else {
            (min_time, max_time)
        }
    }

    /// Execute a simple query
    pub fn query(&self, query_str: &str, time: Option<f64>) -> Result<QueryResult, QueryError> {
        if query_str.starts_with("irate(") && query_str.ends_with(")") {
            self.handle_simple_rate(query_str, time)
        } else if query_str.starts_with("sum(irate(") && query_str.ends_with("))") {
            self.handle_sum_rate(query_str, time)
        } else {
            self.handle_simple_metric(query_str, time)
        }
    }

    /// Handle simple irate() queries
    fn handle_simple_rate(
        &self,
        query: &str,
        time: Option<f64>,
    ) -> Result<QueryResult, QueryError> {
        let inner = &query[6..query.len() - 1];

        if let Some(bracket_pos) = inner.find('[') {
            let metric_part = inner[..bracket_pos].trim();
            let (metric_name, labels) = self.parse_metric_selector(metric_part)?;

            if let Some(collection) = self.tsdb.counters(&metric_name, labels.clone()) {
                let rate_collection = collection.filtered_rate(&labels);

                if labels.inner.is_empty() {
                    let mut result_samples = Vec::new();

                    for (series_labels, series) in rate_collection.iter() {
                        if let Some((timestamp, value)) =
                            self.get_value_at_time(&series.inner, time)
                        {
                            let mut metric_labels = HashMap::new();
                            metric_labels.insert("__name__".to_string(), metric_name.to_string());

                            for (key, value) in series_labels.inner.iter() {
                                metric_labels.insert(key.clone(), value.clone());
                            }

                            result_samples.push(Sample {
                                metric: metric_labels,
                                value: (timestamp, value),
                            });
                        }
                    }

                    if !result_samples.is_empty() {
                        return Ok(QueryResult::Vector {
                            result: result_samples,
                        });
                    }
                } else {
                    let sum_series = rate_collection.sum();
                    if let Some((timestamp, value)) =
                        self.get_value_at_time(&sum_series.inner, time)
                    {
                        let mut metric_labels = HashMap::new();
                        metric_labels.insert("__name__".to_string(), metric_name.to_string());

                        for (key, value) in labels.inner.iter() {
                            metric_labels.insert(key.clone(), value.clone());
                        }

                        return Ok(QueryResult::Vector {
                            result: vec![Sample {
                                metric: metric_labels,
                                value: (timestamp, value),
                            }],
                        });
                    }
                }
            }
        }

        Err(QueryError::MetricNotFound(format!(
            "Could not find metric for query: {query}"
        )))
    }

    /// Handle sum(irate()) queries
    fn handle_sum_rate(&self, query: &str, time: Option<f64>) -> Result<QueryResult, QueryError> {
        let rate_part = &query[4..query.len() - 1];
        let rate_result = self.handle_simple_rate(rate_part, time)?;

        if let QueryResult::Vector { result: samples } = rate_result {
            if samples.is_empty() {
                return Err(QueryError::MetricNotFound(
                    "No data found for sum(irate()) query".to_string(),
                ));
            }

            let timestamp = samples[0].value.0;
            let summed_value: f64 = samples.iter().map(|s| s.value.1).sum();

            let metric_name = samples[0]
                .metric
                .get("__name__")
                .cloned()
                .unwrap_or_else(|| "sum".to_string());

            let mut metric_labels = HashMap::new();
            metric_labels.insert("__name__".to_string(), metric_name);

            return Ok(QueryResult::Vector {
                result: vec![Sample {
                    metric: metric_labels,
                    value: (timestamp, summed_value),
                }],
            });
        }

        Err(QueryError::MetricNotFound(format!(
            "Could not process sum(irate()) query: {query}"
        )))
    }

    /// Parse a metric selector
    fn parse_metric_selector(&self, selector: &str) -> Result<(String, Labels), QueryError> {
        if let Some(brace_pos) = selector.find('{') {
            let metric_name = selector[..brace_pos].trim().to_string();
            let labels_part = &selector[brace_pos + 1..selector.len() - 1];

            let mut labels = Labels::default();
            for part in labels_part.split(',') {
                let kv: Vec<&str> = part.split('=').collect();
                if kv.len() == 2 {
                    let key = kv[0].trim().to_string();
                    let value = kv[1].trim().trim_matches('"').to_string();
                    labels.inner.insert(key, value);
                }
            }
            Ok((metric_name, labels))
        } else {
            Ok((selector.to_string(), Labels::default()))
        }
    }

    /// Get a value at a specific time from a time series
    fn get_value_at_time(
        &self,
        series: &BTreeMap<u64, f64>,
        time: Option<f64>,
    ) -> Option<(f64, f64)> {
        if series.is_empty() {
            return None;
        }

        let target_ns = if let Some(t) = time {
            (t * 1e9) as u64
        } else {
            *series.keys().next_back()?
        };

        if let Some((ts, val)) = series.range(..=target_ns).next_back() {
            Some((*ts as f64 / 1e9, *val))
        } else if let Some((ts, val)) = series.iter().next() {
            Some((*ts as f64 / 1e9, *val))
        } else {
            None
        }
    }

    /// Handle simple metric queries
    fn handle_simple_metric(
        &self,
        query: &str,
        time: Option<f64>,
    ) -> Result<QueryResult, QueryError> {
        let (metric_name, labels) = self.parse_metric_selector(query)?;

        if let Some(collection) = self.tsdb.gauges(&metric_name, labels.clone()) {
            let sum_series = collection.filtered_sum(&labels);
            if let Some((timestamp, value)) = self.get_value_at_time(&sum_series.inner, time) {
                let mut metric_labels = HashMap::new();
                metric_labels.insert("__name__".to_string(), metric_name.to_string());

                return Ok(QueryResult::Vector {
                    result: vec![Sample {
                        metric: metric_labels,
                        value: (timestamp, value),
                    }],
                });
            }
        }

        if let Some(collection) = self.tsdb.counters(&metric_name, labels.clone()) {
            let _filtered = collection.filter(&labels);

            let mut metric_labels = HashMap::new();
            metric_labels.insert("__name__".to_string(), metric_name.to_string());

            return Ok(QueryResult::Vector {
                result: vec![Sample {
                    metric: metric_labels,
                    value: (time.unwrap_or(0.0), 0.0),
                }],
            });
        }

        Err(QueryError::MetricNotFound(format!(
            "Metric not found: {metric_name}"
        )))
    }

    /// Range queries
    fn handle_function_call(
        &self,
        call: &parser::Call,
        start: f64,
        end: f64,
        _step: f64,
    ) -> Result<QueryResult, QueryError> {
        match call.func.name {
            "irate" | "rate" => {
                if let Some(first_arg) = call.args.args.first() {
                    if let Expr::MatrixSelector(selector) = &**first_arg {
                        let metric_name = selector.vs.name.as_deref().ok_or_else(|| {
                            QueryError::ParseError("Matrix selector missing name".to_string())
                        })?;

                        let mut filter_labels = Labels::default();
                        for matcher in &selector.vs.matchers.matchers {
                            if matcher.op.to_string() == "=" {
                                filter_labels
                                    .inner
                                    .insert(matcher.name.clone(), matcher.value.clone());
                            }
                        }

                        debug!("Looking up counter: {}", metric_name);
                        if let Some(collection) = self.tsdb.counters(metric_name, Labels::default())
                        {
                            debug!(
                                "Found counter collection for: {} with {} series",
                                metric_name,
                                collection.len()
                            );
                            let rate_collection = if filter_labels.inner.is_empty() {
                                collection.rate()
                            } else {
                                collection.filtered_rate(&filter_labels)
                            };
                            debug!("Rate collection has {} series", rate_collection.len());

                            let start_ns = (start * 1e9) as u64;
                            let end_ns = (end * 1e9) as u64;
                            debug!("Query time range: {} - {} ns", start_ns, end_ns);

                            let mut result_samples = Vec::new();

                            for (labels, series) in rate_collection.iter() {
                                debug!("Series has {} data points", series.inner.len());
                                if !series.inner.is_empty() {
                                    let first_ts = *series.inner.keys().next().unwrap();
                                    let last_ts = *series.inner.keys().last().unwrap();
                                    debug!("Series time range: {} - {} ns", first_ts, last_ts);
                                }
                                let values: Vec<(f64, f64)> = series
                                    .inner
                                    .range(start_ns..=end_ns)
                                    .map(|(ts, val)| (*ts as f64 / 1e9, *val))
                                    .collect();
                                debug!("After range filter: {} values", values.len());

                                if !values.is_empty() {
                                    let mut metric_labels = HashMap::new();
                                    metric_labels
                                        .insert("__name__".to_string(), metric_name.to_string());

                                    for (key, value) in labels.inner.iter() {
                                        metric_labels.insert(key.clone(), value.clone());
                                    }

                                    result_samples.push(MatrixSample {
                                        metric: metric_labels,
                                        values,
                                    });
                                }
                            }

                            if result_samples.is_empty() {
                                debug!("No result samples for {}", metric_name);
                                return Err(QueryError::MetricNotFound(metric_name.to_string()));
                            }

                            Ok(QueryResult::Matrix {
                                result: result_samples,
                            })
                        } else {
                            debug!("Counter not found: {}", metric_name);
                            Err(QueryError::MetricNotFound(metric_name.to_string()))
                        }
                    } else {
                        Err(QueryError::ParseError(format!(
                            "{} requires matrix selector argument",
                            call.func.name
                        )))
                    }
                } else {
                    Err(QueryError::ParseError(format!(
                        "{} requires an argument",
                        call.func.name
                    )))
                }
            }
            "histogram_quantile" => {
                if call.args.args.len() >= 2 {
                    let quantile = match &*call.args.args[0] {
                        Expr::NumberLiteral(num) => num.val,
                        _ => {
                            return Err(QueryError::ParseError(
                                "histogram_quantile first argument must be a number".to_string(),
                            ));
                        }
                    };

                    let metric_name = match &*call.args.args[1] {
                        Expr::VectorSelector(selector) => {
                            selector.name.as_deref().ok_or_else(|| {
                                QueryError::ParseError("Vector selector missing name".to_string())
                            })?
                        }
                        _ => {
                            return Err(QueryError::ParseError(
                                "histogram_quantile second argument must be a metric name"
                                    .to_string(),
                            ));
                        }
                    };

                    if let Some(collection) = self.tsdb.histograms(metric_name, Labels::default()) {
                        let summed_series = collection.sum();

                        if let Some(percentile_series) = summed_series.percentiles(&[quantile])
                            && let Some(series) = percentile_series.first()
                        {
                            let start_ns = (start * 1e9) as u64;
                            let end_ns = (end * 1e9) as u64;

                            let values: Vec<(f64, f64)> = series
                                .inner
                                .range(start_ns..=end_ns)
                                .map(|(ts, val)| (*ts as f64 / 1e9, *val))
                                .collect();

                            if !values.is_empty() {
                                let mut metric_labels = HashMap::new();
                                metric_labels
                                    .insert("__name__".to_string(), metric_name.to_string());
                                metric_labels.insert("quantile".to_string(), quantile.to_string());

                                return Ok(QueryResult::Matrix {
                                    result: vec![MatrixSample {
                                        metric: metric_labels,
                                        values,
                                    }],
                                });
                            }
                        }

                        Err(QueryError::MetricNotFound(format!(
                            "No histogram data found for {}",
                            metric_name
                        )))
                    } else {
                        Err(QueryError::MetricNotFound(metric_name.to_string()))
                    }
                } else {
                    Err(QueryError::ParseError(
                        "histogram_quantile requires 2 arguments".to_string(),
                    ))
                }
            }
            _ => Err(QueryError::Unsupported(format!(
                "Function {} not yet supported",
                call.func.name
            ))),
        }
    }

    /// Handle aggregate expressions
    fn handle_aggregate(
        &self,
        agg: &parser::AggregateExpr,
        start: f64,
        end: f64,
        step: f64,
    ) -> Result<QueryResult, QueryError> {
        let op_str = agg.op.to_string();

        match op_str.as_str() {
            "sum" => {
                let inner = self.evaluate_expr(&agg.expr, start, end, step)?;

                if let Some(modifier) = &agg.modifier {
                    match inner {
                        QueryResult::Matrix { result: samples } => {
                            let mut grouped: HashMap<BTreeMap<String, String>, Vec<&MatrixSample>> =
                                HashMap::new();

                            for sample in &samples {
                                let mut group_key = BTreeMap::new();

                                match modifier {
                                    parser::LabelModifier::Include(labels) => {
                                        for label_name in &labels.labels {
                                            if let Some(value) = sample.metric.get(label_name) {
                                                group_key.insert(label_name.clone(), value.clone());
                                            }
                                        }
                                    }
                                    parser::LabelModifier::Exclude(labels) => {
                                        for (key, value) in &sample.metric {
                                            if !labels.labels.contains(key) && key != "__name__" {
                                                group_key.insert(key.clone(), value.clone());
                                            }
                                        }
                                    }
                                }

                                grouped.entry(group_key).or_default().push(sample);
                            }

                            let mut result_samples = Vec::new();

                            for (group_labels, group_samples) in grouped {
                                let mut timestamp_map: BTreeMap<u64, f64> = BTreeMap::new();

                                for sample in group_samples {
                                    for (ts, val) in &sample.values {
                                        let ts_key = (*ts * 1e9) as u64;
                                        *timestamp_map.entry(ts_key).or_insert(0.0) += val;
                                    }
                                }

                                let result_values: Vec<(f64, f64)> = timestamp_map
                                    .into_iter()
                                    .map(|(ts_ns, val)| (ts_ns as f64 / 1e9, val))
                                    .collect();

                                if !result_values.is_empty() {
                                    let mut metric_map = HashMap::new();
                                    for (k, v) in group_labels {
                                        metric_map.insert(k, v);
                                    }

                                    result_samples.push(MatrixSample {
                                        metric: metric_map,
                                        values: result_values,
                                    });
                                }
                            }

                            Ok(QueryResult::Matrix {
                                result: result_samples,
                            })
                        }
                        _ => Ok(inner),
                    }
                } else {
                    match inner {
                        QueryResult::Matrix { result: samples } => {
                            if samples.is_empty() {
                                return Ok(QueryResult::Matrix { result: vec![] });
                            }

                            let mut timestamp_map: BTreeMap<u64, f64> = BTreeMap::new();

                            for sample in &samples {
                                for (ts, val) in &sample.values {
                                    let ts_key = (*ts * 1e9) as u64;
                                    *timestamp_map.entry(ts_key).or_insert(0.0) += val;
                                }
                            }

                            let result_values: Vec<(f64, f64)> = timestamp_map
                                .into_iter()
                                .map(|(ts_ns, val)| (ts_ns as f64 / 1e9, val))
                                .collect();

                            Ok(QueryResult::Matrix {
                                result: vec![MatrixSample {
                                    metric: HashMap::new(),
                                    values: result_values,
                                }],
                            })
                        }
                        _ => Ok(inner),
                    }
                }
            }
            _ => Err(QueryError::Unsupported(format!(
                "Aggregation {} not yet fully implemented",
                op_str
            ))),
        }
    }

    /// Apply a binary operation
    fn apply_binary_op(
        &self,
        op: &TokenType,
        left: QueryResult,
        right: QueryResult,
    ) -> Result<QueryResult, QueryError> {
        match (left, right) {
            (
                QueryResult::Matrix {
                    result: left_samples,
                },
                QueryResult::Matrix {
                    result: right_samples,
                },
            ) => {
                debug!(
                    "Binary op: Matrix({}) {:?} Matrix({})",
                    left_samples.len(),
                    op,
                    right_samples.len()
                );
                let mut result_samples = Vec::new();

                let mut right_by_labels: HashMap<BTreeMap<String, String>, &MatrixSample> =
                    HashMap::new();
                for right_sample in &right_samples {
                    let mut labels = BTreeMap::new();
                    for (k, v) in &right_sample.metric {
                        if k != "__name__" {
                            labels.insert(k.clone(), v.clone());
                        }
                    }
                    debug!(
                        "Right sample labels: {:?}, values count: {}",
                        labels,
                        right_sample.values.len()
                    );
                    right_by_labels.insert(labels, right_sample);
                }

                for left_sample in &left_samples {
                    let mut left_labels = BTreeMap::new();
                    for (k, v) in &left_sample.metric {
                        if k != "__name__" {
                            left_labels.insert(k.clone(), v.clone());
                        }
                    }
                    debug!(
                        "Left sample labels: {:?}, values count: {}",
                        left_labels,
                        left_sample.values.len()
                    );

                    // When both sides have exactly 1 sample, match them directly
                    // regardless of labels (common case for aggregated metrics)
                    let right_sample = if left_samples.len() == 1 && right_samples.len() == 1 {
                        debug!("Using single-sample matching (both sides have 1 sample)");
                        right_samples.first()
                    } else if left_labels.is_empty() {
                        debug!("Using single-sample fallback (no labels on left)");
                        right_samples.first()
                    } else {
                        debug!("Looking up right sample by labels");
                        right_by_labels.get(&left_labels).copied()
                    };

                    if let Some(right_sample) = right_sample {
                        debug!(
                            "Found matching right sample with {} values",
                            right_sample.values.len()
                        );
                        let mut result_values = Vec::new();
                        let op_str = op.to_string();
                        debug!("Operator string: '{}'", op_str);

                        // If same length, use index-based matching (more reliable)
                        if left_sample.values.len() == right_sample.values.len() {
                            debug!(
                                "Using index-based matching (same length: {})",
                                left_sample.values.len()
                            );
                            for (i, (left_ts, left_val)) in left_sample.values.iter().enumerate() {
                                let right_val = right_sample.values[i].1;
                                debug!("  i={}: left={}, right={}", i, left_val, right_val);
                                let result_val = match op_str.as_str() {
                                    "+" => left_val + right_val,
                                    "-" => left_val - right_val,
                                    "*" => left_val * right_val,
                                    "/" => {
                                        if right_val != 0.0 {
                                            left_val / right_val
                                        } else {
                                            continue;
                                        }
                                    }
                                    _ => {
                                        debug!("Unsupported operator in match: '{}'", op_str);
                                        return Err(QueryError::Unsupported(format!(
                                            "Unsupported operator: {}",
                                            op_str
                                        )));
                                    }
                                };
                                debug!("  result={}", result_val);
                                result_values.push((*left_ts, result_val));
                            }
                            debug!(
                                "After index-based loop: {} result values",
                                result_values.len()
                            );
                        } else {
                            // Fall back to timestamp-based matching with tolerance
                            let right_map: HashMap<i64, f64> = right_sample
                                .values
                                .iter()
                                .map(|(ts, val)| ((ts * 1000.0) as i64, *val))
                                .collect();

                            for (left_ts, left_val) in &left_sample.values {
                                let ts_ms = (*left_ts * 1000.0) as i64;

                                if let Some(&right_val) = right_map.get(&ts_ms) {
                                    let result_val = match op_str.as_str() {
                                        "+" => left_val + right_val,
                                        "-" => left_val - right_val,
                                        "*" => left_val * right_val,
                                        "/" => {
                                            if right_val != 0.0 {
                                                left_val / right_val
                                            } else {
                                                continue;
                                            }
                                        }
                                        _ => {
                                            return Err(QueryError::Unsupported(format!(
                                                "Unsupported operator: {}",
                                                op_str
                                            )));
                                        }
                                    };
                                    result_values.push((*left_ts, result_val));
                                }
                            }
                        }

                        debug!("Final result_values count: {}", result_values.len());
                        if !result_values.is_empty() {
                            result_samples.push(MatrixSample {
                                metric: left_sample.metric.clone(),
                                values: result_values,
                            });
                        }
                    } else {
                        debug!(
                            "No matching right sample found for left_labels: {:?}",
                            left_labels
                        );
                    }
                }

                debug!(
                    "Binary op returning {} result samples",
                    result_samples.len()
                );
                Ok(QueryResult::Matrix {
                    result: result_samples,
                })
            }
            (
                QueryResult::Matrix {
                    result: mut samples,
                },
                QueryResult::Scalar { result: scalar },
            ) => {
                let scalar_val = scalar.1;
                let op_str = op.to_string();

                for sample in &mut samples {
                    for value in &mut sample.values {
                        value.1 = match op_str.as_str() {
                            "+" => value.1 + scalar_val,
                            "-" => value.1 - scalar_val,
                            "*" => value.1 * scalar_val,
                            "/" => {
                                if scalar_val != 0.0 {
                                    value.1 / scalar_val
                                } else {
                                    continue;
                                }
                            }
                            _ => {
                                return Err(QueryError::Unsupported(format!(
                                    "Unsupported operator: {}",
                                    op_str
                                )));
                            }
                        };
                    }
                }
                Ok(QueryResult::Matrix { result: samples })
            }
            (
                QueryResult::Scalar { result: scalar },
                QueryResult::Matrix {
                    result: mut samples,
                },
            ) => {
                let scalar_val = scalar.1;
                let op_str = op.to_string();

                for sample in &mut samples {
                    for value in &mut sample.values {
                        value.1 = match op_str.as_str() {
                            "+" => scalar_val + value.1,
                            "-" => scalar_val - value.1,
                            "*" => scalar_val * value.1,
                            "/" => {
                                if value.1 != 0.0 {
                                    scalar_val / value.1
                                } else {
                                    continue;
                                }
                            }
                            _ => {
                                return Err(QueryError::Unsupported(format!(
                                    "Unsupported operator: {}",
                                    op_str
                                )));
                            }
                        };
                    }
                }
                Ok(QueryResult::Matrix { result: samples })
            }
            _ => Err(QueryError::EvaluationError(
                "Incompatible operands for binary operation".to_string(),
            )),
        }
    }

    /// Evaluate an AST expression
    fn evaluate_expr(
        &self,
        expr: &Expr,
        start: f64,
        end: f64,
        step: f64,
    ) -> Result<QueryResult, QueryError> {
        match expr {
            Expr::Binary(binary) => {
                debug!("Evaluating binary expression: {:?}", binary.op);
                let left = self.evaluate_expr(&binary.lhs, start, end, step)?;
                debug!(
                    "Left operand result type: {:?}",
                    match &left {
                        QueryResult::Matrix { result } =>
                            format!("Matrix with {} samples", result.len()),
                        QueryResult::Vector { result } =>
                            format!("Vector with {} samples", result.len()),
                        QueryResult::Scalar { .. } => "Scalar".to_string(),
                        QueryResult::HistogramHeatmap { .. } => "HistogramHeatmap".to_string(),
                    }
                );
                let right = self.evaluate_expr(&binary.rhs, start, end, step)?;
                debug!(
                    "Right operand result type: {:?}",
                    match &right {
                        QueryResult::Matrix { result } =>
                            format!("Matrix with {} samples", result.len()),
                        QueryResult::Vector { result } =>
                            format!("Vector with {} samples", result.len()),
                        QueryResult::Scalar { .. } => "Scalar".to_string(),
                        QueryResult::HistogramHeatmap { .. } => "HistogramHeatmap".to_string(),
                    }
                );
                self.apply_binary_op(&binary.op, left, right)
            }
            Expr::VectorSelector(selector) => {
                let metric_name = selector.name.as_deref().ok_or_else(|| {
                    QueryError::ParseError("Vector selector missing name".to_string())
                })?;

                let mut filter_labels = Labels::default();
                for matcher in &selector.matchers.matchers {
                    if matcher.op.to_string() == "=" {
                        filter_labels
                            .inner
                            .insert(matcher.name.clone(), matcher.value.clone());
                    }
                }

                if let Some(collection) = self.tsdb.gauges(metric_name, Labels::default()) {
                    let start_ns = (start * 1e9) as u64;
                    let end_ns = (end * 1e9) as u64;

                    let mut result_samples = Vec::new();

                    for (labels, series) in collection.iter() {
                        if !filter_labels.inner.is_empty() && !labels.matches(&filter_labels) {
                            continue;
                        }
                        let untyped = series.untyped();
                        let values: Vec<(f64, f64)> = untyped
                            .inner
                            .range(start_ns..=end_ns)
                            .map(|(ts, val)| (*ts as f64 / 1e9, *val))
                            .collect();

                        if !values.is_empty() {
                            let mut metric_labels = HashMap::new();
                            metric_labels.insert("__name__".to_string(), metric_name.to_string());

                            for (key, value) in labels.inner.iter() {
                                metric_labels.insert(key.clone(), value.clone());
                            }

                            result_samples.push(MatrixSample {
                                metric: metric_labels,
                                values,
                            });
                        }
                    }

                    if !result_samples.is_empty() {
                        return Ok(QueryResult::Matrix {
                            result: result_samples,
                        });
                    }
                }

                Err(QueryError::MetricNotFound(metric_name.to_string()))
            }
            Expr::NumberLiteral(num) => Ok(QueryResult::Scalar {
                result: (start, num.val),
            }),
            Expr::Call(call) => self.handle_function_call(call, start, end, step),
            Expr::Aggregate(agg) => self.handle_aggregate(agg, start, end, step),
            Expr::Paren(paren) => {
                // Parenthesized expression - just evaluate the inner expression
                self.evaluate_expr(&paren.expr, start, end, step)
            }
            Expr::MatrixSelector(_selector) => Err(QueryError::Unsupported(
                "Direct matrix selector not supported".to_string(),
            )),
            _ => Err(QueryError::Unsupported(format!(
                "Unsupported expression type: {:?}",
                expr
            ))),
        }
    }

    /// Handle histogram_percentiles queries
    fn handle_histogram_percentiles(
        &self,
        query_str: &str,
        start: f64,
        end: f64,
    ) -> Result<QueryResult, QueryError> {
        let inner = &query_str["histogram_percentiles(".len()..query_str.len() - 1];

        let array_start = inner.find('[').ok_or_else(|| {
            QueryError::ParseError(
                "histogram_percentiles first argument must be an array of percentiles".to_string(),
            )
        })?;
        let array_end = inner.find(']').ok_or_else(|| {
            QueryError::ParseError("Missing closing bracket in percentiles array".to_string())
        })?;

        let array_str = &inner[array_start + 1..array_end];
        let percentiles: Vec<f64> = array_str
            .split(',')
            .map(|s| s.trim().parse::<f64>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                QueryError::ParseError(format!("Failed to parse percentile value: {}", e))
            })?;

        if percentiles.is_empty() {
            return Err(QueryError::ParseError(
                "Percentiles array cannot be empty".to_string(),
            ));
        }

        let after_array = &inner[array_end + 1..].trim();
        let metric_selector = after_array
            .strip_prefix(',')
            .map(|s| s.trim())
            .ok_or_else(|| {
                QueryError::ParseError(
                    "histogram_percentiles requires a metric name as second argument".to_string(),
                )
            })?;

        let (metric_name, labels) = self.parse_metric_selector(metric_selector)?;

        if let Some(collection) = self.tsdb.histograms(&metric_name, labels) {
            let summed_series = collection.sum();

            if let Some(percentile_series_vec) = summed_series.percentiles(&percentiles) {
                let start_ns = (start * 1e9) as u64;
                let end_ns = (end * 1e9) as u64;

                let mut result_samples = Vec::new();

                for (idx, series) in percentile_series_vec.iter().enumerate() {
                    let values: Vec<(f64, f64)> = series
                        .inner
                        .range(start_ns..=end_ns)
                        .map(|(ts, val)| (*ts as f64 / 1e9, *val))
                        .collect();

                    if !values.is_empty() {
                        let mut metric_labels = HashMap::new();
                        metric_labels.insert("__name__".to_string(), metric_name.to_string());
                        metric_labels
                            .insert("percentile".to_string(), percentiles[idx].to_string());

                        result_samples.push(MatrixSample {
                            metric: metric_labels,
                            values,
                        });
                    }
                }

                if !result_samples.is_empty() {
                    return Ok(QueryResult::Matrix {
                        result: result_samples,
                    });
                }
            }

            Err(QueryError::MetricNotFound(format!(
                "No histogram data found for {}",
                metric_name
            )))
        } else {
            Err(QueryError::MetricNotFound(metric_name.to_string()))
        }
    }

    /// Handle histogram_heatmap(histogram_metric) queries
    /// Returns bucket data suitable for rendering as a latency heatmap
    fn handle_histogram_heatmap(
        &self,
        query_str: &str,
        start: f64,
        end: f64,
    ) -> Result<QueryResult, QueryError> {
        // Extract the metric selector from histogram_heatmap(metric_selector)
        let inner = &query_str["histogram_heatmap(".len()..query_str.len() - 1];
        let metric_selector = inner.trim();

        // Parse the metric selector to extract name and labels
        let (metric_name, labels) = self.parse_metric_selector(metric_selector)?;

        // Get the histogram data with label filtering
        if let Some(collection) = self.tsdb.histograms(&metric_name, labels) {
            // Sum all histogram series together
            let summed_series = collection.sum();

            // Get heatmap data
            if let Some(heatmap_data) = summed_series.heatmap() {
                let start_sec = start;
                let end_sec = end;

                // Filter data to the requested time range
                let mut filtered_timestamps = Vec::new();
                let mut filtered_data = Vec::new();
                let mut time_index_map = std::collections::HashMap::new();

                for (old_idx, ts) in heatmap_data.timestamps.iter().enumerate() {
                    if *ts >= start_sec && *ts <= end_sec {
                        let new_idx = filtered_timestamps.len();
                        time_index_map.insert(old_idx, new_idx);
                        filtered_timestamps.push(*ts);
                    }
                }

                let mut min_value = f64::MAX;
                let mut max_value = f64::MIN;

                for (time_idx, bucket_idx, count) in heatmap_data.data.iter() {
                    if let Some(&new_time_idx) = time_index_map.get(time_idx) {
                        filtered_data.push((new_time_idx, *bucket_idx, *count));
                        min_value = min_value.min(*count);
                        max_value = max_value.max(*count);
                    }
                }

                if min_value == f64::MAX {
                    min_value = 0.0;
                }
                if max_value == f64::MIN {
                    max_value = 0.0;
                }

                return Ok(QueryResult::HistogramHeatmap {
                    result: HistogramHeatmapResult {
                        timestamps: filtered_timestamps,
                        bucket_bounds: heatmap_data.bucket_bounds,
                        data: filtered_data,
                        min_value,
                        max_value,
                    },
                });
            }

            Err(QueryError::MetricNotFound(format!(
                "No histogram data found for {}",
                metric_name
            )))
        } else {
            Err(QueryError::MetricNotFound(metric_name.to_string()))
        }
    }

    pub fn query_range(
        &self,
        query_str: &str,
        start: f64,
        end: f64,
        step: f64,
    ) -> Result<QueryResult, QueryError> {
        if query_str.starts_with("histogram_percentiles(") && query_str.ends_with(")") {
            return self.handle_histogram_percentiles(query_str, start, end);
        }

        if query_str.starts_with("histogram_heatmap(") && query_str.ends_with(")") {
            return self.handle_histogram_heatmap(query_str, start, end);
        }

        match parser::parse(query_str) {
            Ok(expr) => self.evaluate_expr(&expr, start, end, step),
            Err(err) => {
                let error_msg = format!("{:?}", err);
                if error_msg.contains("invalid promql query") && query_str.contains(" by ") {
                    Err(QueryError::ParseError(
                        "Invalid query syntax. Aggregation operators require parentheses around the expression, e.g., 'sum by (id) (irate(metric[5m]))' not 'sum by (id) irate(metric[5m])'".to_string()
                    ))
                } else {
                    Err(QueryError::ParseError(format!(
                        "Failed to parse query: {}",
                        error_msg
                    )))
                }
            }
        }
    }
}
