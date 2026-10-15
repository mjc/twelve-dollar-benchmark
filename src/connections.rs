use super::{
    errors,
    samples::{Samples, distribution as latency_distribution},
};
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
    let mut report = json!(latency_distribution(samples));
    report["samples"] = json!(samples.len());
    report
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
