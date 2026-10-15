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
    avg_ms: Option<f64>,
    pub(crate) med_ms: Option<f64>,
    pub(crate) p95_ms: Option<f64>,
    pub(crate) p99_ms: Option<f64>,
    max_ms: Option<f64>,
    errors: errors::ErrorDetails,
}
impl Samples {
    pub(crate) fn summary(&mut self) -> Summary {
        self.latency_ms.sort_unstable_by(f64::total_cmp);
        let n = self.latency_ms.len();
        let count = n.to_f64();
        Summary {
            requests: n,
            failed: self.failed,
            failure_rate: count
                .filter(|_| n > 0)
                .and_then(|count| self.failed.to_f64().map(|failed| failed / count)),
            checks_failed: self.checks_failed,
            avg_ms: count
                .filter(|_| n > 0)
                .map(|count| self.latency_ms.iter().sum::<f64>() / count),
            med_ms: percentile(&self.latency_ms, 0.5),
            p95_ms: percentile(&self.latency_ms, 0.95),
            p99_ms: percentile(&self.latency_ms, 0.99),
            max_ms: self.latency_ms.last().copied(),
            errors: self.errors.clone(),
        }
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
    for (index, sample) in samples.iter_mut().enumerate() {
        aggregate.failed += sample.failed;
        aggregate.checks_failed += sample.checks_failed;
        aggregate.errors.merge(&sample.errors);
        if index != base_index {
            let mut latency = std::mem::take(&mut sample.latency_ms);
            aggregate.latency_ms.append(&mut latency);
        }
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
        && s.p95_ms.is_some_and(|v| v < 500.0)
        && s.p99_ms.is_some_and(|v| v < 1000.0)
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
