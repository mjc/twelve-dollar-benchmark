use super::{connections, errors};
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use twelve_dollar_benchmark::ENDPOINTS;

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Samples {
    // ponytail: exact samples match k6 and use about 8 MB per million requests.
    pub(crate) latency_ms: Vec<f64>,
    pub(crate) failed: usize,
    pub(crate) checks_failed: usize,
    #[serde(default)]
    pub(crate) errors: errors::ErrorDetails,
}
pub(crate) type Metrics = Arc<Mutex<[Samples; 4]>>;

#[derive(Serialize, Debug)]
pub(crate) struct Summary {
    pub(crate) requests: usize,
    failed: usize,
    failure_rate: Option<f64>,
    checks_failed: usize,
    #[serde(flatten)]
    pub(crate) latency: Distribution,
    errors: errors::ErrorDetails,
}
impl Samples {
    pub(crate) fn merge(&mut self, other: &mut Self) {
        self.latency_ms
            .append(&mut std::mem::take(&mut other.latency_ms));
        self.failed += other.failed;
        self.checks_failed += other.checks_failed;
        self.errors.merge(&other.errors);
    }

    pub(crate) fn summary(&mut self) -> Summary {
        let n = self.latency_ms.len();
        let count = n.to_f64();
        Summary {
            requests: n,
            failed: self.failed,
            failure_rate: count
                .filter(|_| n > 0)
                .and_then(|count| self.failed.to_f64().map(|failed| failed / count)),
            checks_failed: self.checks_failed,
            latency: distribution(&mut self.latency_ms),
            errors: self.errors.clone(),
        }
    }
}

#[derive(Serialize, Debug)]
pub(crate) struct Distribution {
    #[serde(rename = "avg_ms")]
    pub(crate) average: Option<f64>,
    #[serde(rename = "med_ms")]
    pub(crate) median: Option<f64>,
    #[serde(rename = "p95_ms")]
    pub(crate) p95: Option<f64>,
    #[serde(rename = "p99_ms")]
    pub(crate) p99: Option<f64>,
    #[serde(rename = "max_ms")]
    pub(crate) maximum: Option<f64>,
}

pub(crate) fn distribution(samples: &mut [f64]) -> Distribution {
    samples.sort_unstable_by(f64::total_cmp);
    let count = samples.len().to_f64().filter(|_| !samples.is_empty());
    Distribution {
        average: count.map(|count| samples.iter().sum::<f64>() / count),
        median: percentile(samples, 0.5),
        p95: percentile(samples, 0.95),
        p99: percentile(samples, 0.99),
        maximum: samples.last().copied(),
    }
}

pub(crate) fn aggregate_samples(samples: &mut [Samples]) -> Samples {
    let Some((base_index, _)) = samples
        .iter()
        .enumerate()
        .max_by_key(|(_, sample)| sample.latency_ms.len())
    else {
        return Samples::default();
    };
    let mut aggregate = Samples {
        latency_ms: std::mem::take(&mut samples[base_index].latency_ms),
        ..Samples::default()
    };
    for sample in samples {
        aggregate.merge(sample);
    }
    aggregate
}

#[must_use]
pub(crate) fn percentile(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() || !p.is_finite() || !(0.0..=1.0).contains(&p) {
        return None;
    }
    let rank = p * (sorted.len() - 1).to_f64()?;
    let low = *sorted.get(rank.floor().to_usize()?)?;
    let high = *sorted.get(rank.ceil().to_usize()?)?;
    Some(low + (high - low) * rank.fract())
}
#[must_use]
pub(crate) fn passes(s: &Summary, runtime_errors: usize) -> bool {
    runtime_errors == 0
        && s.latency.p95.is_some_and(|v| v < 500.0)
        && s.latency.p99.is_some_and(|v| v < 1000.0)
        && s.failure_rate.is_some_and(|v| v < 0.01)
}
pub(crate) fn endpoint_summaries(
    samples: &mut [Samples; 4],
    seconds: Option<f64>,
) -> Result<serde_json::Map<String, serde_json::Value>, serde_json::Error> {
    ENDPOINTS
        .into_iter()
        .zip(samples.iter_mut())
        .map(|(name, sample)| {
            let mut summary = serde_json::to_value(sample.summary())?;
            if let (Some(summary), serde_json::Value::Object(rates)) = (
                summary.as_object_mut(),
                connections::request_rates(sample, seconds),
            ) {
                summary.extend(rates);
            }
            Ok((name.into(), summary))
        })
        .collect()
}

// Local runs and merged workers use the same scoring and statistics path.
// Persist raw samples before calling this: aggregation consumes their buffers.
pub(crate) fn report(
    samples: &mut [Samples; 4],
    seconds: Option<f64>,
    runtime_errors: usize,
    completed: bool,
    mut report: serde_json::Value,
) -> Result<serde_json::Value, serde_json::Error> {
    use serde_json::json;
    let endpoints = endpoint_summaries(samples, seconds)?;
    let mut aggregate = aggregate_samples(samples);
    let summary = aggregate.summary();
    report["completed"] = json!(completed);
    report["runtime_errors"] = json!(runtime_errors);
    report["passed"] = json!(completed && passes(&summary, runtime_errors));
    report["aggregate"] = serde_json::to_value(summary)?;
    report["endpoints"] = serde_json::Value::Object(endpoints);
    report["request_rates"] = connections::request_rates(&aggregate, seconds);
    report["latency"] =
        json!("HTTP send through complete response body, excluding DNS and TCP connection setup");
    report["thresholds"] = json!({"p95_ms_lt": 500, "p99_ms_lt": 1000, "failure_rate_lt": 0.01});
    Ok(report)
}
