use super::{Samples, errors, samples::percentile};
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Default, Serialize, Deserialize)]
pub(super) struct Connections {
    pub successful_ms: Vec<f64>,
    pub failed_ms: Vec<f64>,
    pub cancelled: usize,
    pub errors: BTreeMap<String, usize>,
}

impl Connections {
    pub fn counts(&self) -> [usize; 3] {
        [
            self.successful_ms.len(),
            self.failed_ms.len(),
            self.cancelled,
        ]
    }
    pub fn merge(&mut self, mut other: Self) {
        self.successful_ms.append(&mut other.successful_ms);
        self.failed_ms.append(&mut other.failed_ms);
        self.cancelled = self.cancelled.saturating_add(other.cancelled);
        for (error, count) in other.errors {
            let total = self.errors.entry(error).or_default();
            *total = total.saturating_add(count);
        }
    }
    pub fn report(mut self, seconds: Option<f64>) -> Value {
        let established = self.successful_ms.len();
        let failed = self.failed_ms.len();
        let total = established + failed;
        let established_distribution = distribution(&mut self.successful_ms);
        let failed_distribution = distribution(&mut self.failed_ms);
        self.successful_ms.append(&mut self.failed_ms);
        json!({
            "scope": "client connection setup including DNS and TCP; pooled requests do not start a new attempt; an attempt may try multiple DNS addresses and source ports",
            "attempts": total, "established": established, "failed": failed, "cancelled": self.cancelled,
            "attempts_per_second": rate(total, seconds), "connections_per_second": rate(established, seconds), "failed_per_second": rate(failed, seconds),
            "setup_duration_ms": distribution(&mut self.successful_ms), "successful_setup_duration_ms": established_distribution, "failed_setup_duration_ms": failed_distribution, "errors": self.errors,
        })
    }
}

fn rate(count: usize, seconds: Option<f64>) -> Option<f64> {
    seconds
        .filter(|s| *s > 0.0)
        .and_then(|s| count.to_f64().map(|count| count / s))
}

fn distribution(samples: &mut [f64]) -> Value {
    samples.sort_unstable_by(f64::total_cmp);
    let average = (!samples.is_empty())
        .then(|| samples.len().to_f64())
        .flatten()
        .map(|count| samples.iter().sum::<f64>() / count);
    json!({"samples": samples.len(), "avg_ms": average,
        "med_ms": percentile(samples, 0.5), "p95_ms": percentile(samples, 0.95), "p99_ms": percentile(samples, 0.99), "max_ms": samples.last()})
}

pub(super) fn request_rates(samples: &Samples, seconds: Option<f64>) -> Value {
    let n = samples.latency_ms.len();
    json!({"requests_per_second": rate(n, seconds), "successful_requests_per_second": rate(n.saturating_sub(samples.failed), seconds), "failed_requests_per_second": rate(samples.failed, seconds)})
}

pub(super) struct ConnectionAttempt {
    metrics: Arc<Mutex<Connections>>,
    start: Instant,
    finished: bool,
}
impl ConnectionAttempt {
    pub(super) fn start(metrics: Arc<Mutex<Connections>>) -> Self {
        Self {
            metrics,
            start: Instant::now(),
            finished: false,
        }
    }

    pub(super) fn finish(&mut self, error: Option<String>) {
        let elapsed = self.start.elapsed().as_secs_f64() * 1000.0;
        let mut metrics = self.metrics.lock().unwrap();
        if let Some(error) = error {
            metrics.failed_ms.push(elapsed);
            let key = errors::sanitize_message_urls(&error);
            let key: String = key.chars().take(512).collect();
            let key = if metrics.errors.contains_key(&key) || metrics.errors.len() < 16 {
                key
            } else {
                "other connection errors".into()
            };
            let count = metrics.errors.entry(key).or_default();
            *count = count.saturating_add(1);
        } else {
            metrics.successful_ms.push(elapsed);
        }
        self.finished = true;
    }
}
impl Drop for ConnectionAttempt {
    fn drop(&mut self) {
        if !self.finished {
            {
                let mut metrics = self.metrics.lock().unwrap();
                metrics.cancelled = metrics.cancelled.saturating_add(1);
            }
            self.finish(Some(
                "connection setup cancelled before completion (for example, request timeout)"
                    .into(),
            ));
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_error_keys_redact_url_credentials_and_secrets() {
        let metrics = Arc::new(Mutex::new(Connections::default()));
        let mut attempt = ConnectionAttempt::start(metrics.clone());
        attempt.finish(Some(
            "connect to https://alice:secret@example.test/path?token=hidden#fragment failed".into(),
        ));

        let metrics = metrics.lock().unwrap();
        assert_eq!(metrics.errors.len(), 1);
        let key = metrics.errors.keys().next().expect("one error key");
        assert!(key.contains("https://example.test/path"));
        assert!(!key.contains("alice"));
        assert!(!key.contains("secret"));
        assert!(!key.contains("token=hidden"));
        assert!(!key.contains("fragment"));
    }

    #[test]
    fn corrupted_failure_counts_do_not_underflow_request_rates() {
        let samples = Samples {
            latency_ms: vec![1.0],
            failed: 2,
            ..Default::default()
        };

        let rates = request_rates(&samples, Some(1.0));
        assert_eq!(rates["successful_requests_per_second"], 0.0);
        assert_eq!(rates["failed_requests_per_second"], 2.0);
    }

    #[test]
    fn report_keeps_exact_distributions_without_copying_samples() {
        let report = Connections {
            successful_ms: vec![2.0, 1.0],
            failed_ms: vec![20.0, 10.0],
            ..Default::default()
        }
        .report(Some(2.0));

        assert_eq!(report["setup_duration_ms"]["samples"], 4);
        assert_eq!(report["setup_duration_ms"]["avg_ms"], 8.25);
        assert_eq!(report["setup_duration_ms"]["med_ms"], 6.0);
        assert!((report["setup_duration_ms"]["p95_ms"].as_f64().unwrap() - 18.5).abs() < 1e-12);
        assert_eq!(report["setup_duration_ms"]["max_ms"], 20.0);
        assert_eq!(report["successful_setup_duration_ms"]["med_ms"], 1.5);
        assert_eq!(report["failed_setup_duration_ms"]["med_ms"], 15.0);
    }

    #[test]
    fn merging_connection_counters_saturates_instead_of_wrapping() {
        let mut first = Connections {
            cancelled: usize::MAX,
            errors: BTreeMap::from([("connect".into(), usize::MAX)]),
            ..Default::default()
        };
        first.merge(Connections {
            cancelled: 1,
            errors: BTreeMap::from([("connect".into(), 1)]),
            ..Default::default()
        });

        assert_eq!(first.cancelled, usize::MAX);
        assert_eq!(first.errors["connect"], usize::MAX);
    }

    #[test]
    fn connection_attempt_records_success_failure_and_cancellation() {
        let metrics = Arc::new(Mutex::new(Connections::default()));
        let mut success = ConnectionAttempt::start(metrics.clone());
        success.finish(None);
        let mut failure = ConnectionAttempt::start(metrics.clone());
        failure.finish(Some("connection refused".into()));
        drop(ConnectionAttempt::start(metrics.clone()));
        let metrics = metrics.lock().unwrap();
        assert_eq!(metrics.successful_ms.len(), 1);
        assert_eq!(metrics.failed_ms.len(), 2);
        assert_eq!(metrics.cancelled, 1);
    }
}
