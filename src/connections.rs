use super::{Samples, errors, percentile};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Instant,
};
use tower::{Layer, Service};

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
    pub fn report(&mut self, seconds: Option<f64>) -> Value {
        let mut all = self.successful_ms.clone();
        all.extend_from_slice(&self.failed_ms);
        let total = all.len();
        json!({
            "scope": "client connection setup including DNS and TCP; pooled requests do not start a new attempt; an attempt may try multiple DNS addresses and source ports",
            "attempts": total, "established": self.successful_ms.len(), "failed": self.failed_ms.len(), "cancelled": self.cancelled,
            "attempts_per_second": rate(total, seconds), "connections_per_second": rate(self.successful_ms.len(), seconds), "failed_per_second": rate(self.failed_ms.len(), seconds),
            "setup_duration_ms": distribution(&mut all), "successful_setup_duration_ms": distribution(&mut self.successful_ms), "failed_setup_duration_ms": distribution(&mut self.failed_ms), "errors": self.errors,
        })
    }
}

#[allow(clippy::cast_precision_loss)] // Rates are floating point report statistics.
fn rate(count: usize, seconds: Option<f64>) -> Option<f64> {
    seconds.filter(|s| *s > 0.0).map(|s| count as f64 / s)
}

#[allow(clippy::cast_precision_loss)] // Averages are represented as floating point milliseconds.
fn distribution(samples: &mut [f64]) -> Value {
    samples.sort_unstable_by(f64::total_cmp);
    json!({"samples": samples.len(), "avg_ms": (!samples.is_empty()).then(|| samples.iter().sum::<f64>() / samples.len() as f64),
        "med_ms": percentile(samples, 0.5), "p95_ms": percentile(samples, 0.95), "p99_ms": percentile(samples, 0.99), "max_ms": samples.last()})
}

pub(super) fn request_rates(samples: &Samples, seconds: Option<f64>) -> Value {
    let n = samples.latency_ms.len();
    json!({"requests_per_second": rate(n, seconds), "successful_requests_per_second": rate(n.saturating_sub(samples.failed), seconds), "failed_requests_per_second": rate(samples.failed, seconds)})
}

#[derive(Clone)]
pub(super) struct ConnectLayer(pub Arc<Mutex<Connections>>);
#[derive(Clone)]
pub(super) struct ConnectService<S> {
    inner: S,
    metrics: Arc<Mutex<Connections>>,
}
impl<S> Layer<S> for ConnectLayer {
    type Service = ConnectService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        ConnectService {
            inner,
            metrics: self.0.clone(),
        }
    }
}

struct Attempt {
    metrics: Arc<Mutex<Connections>>,
    start: Instant,
    finished: bool,
}
impl Attempt {
    fn finish(&mut self, error: Option<String>) {
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
impl Drop for Attempt {
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
impl<S, T> Service<T> for ConnectService<S>
where
    S: Service<T>,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Display + Debug + Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, S::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: T) -> Self::Future {
        let mut attempt = Attempt {
            metrics: self.metrics.clone(),
            start: Instant::now(),
            finished: false,
        };
        let future = self.inner.call(request);
        Box::pin(async move {
            let result = future.await;
            attempt.finish(
                result
                    .as_ref()
                    .err()
                    .map(|error| format!("{error}; {error:?}")),
            );
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_error_keys_redact_url_credentials_and_secrets() {
        let metrics = Arc::new(Mutex::new(Connections::default()));
        let mut attempt = Attempt {
            metrics: metrics.clone(),
            start: Instant::now(),
            finished: false,
        };
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

    #[tokio::test]
    async fn connection_layer_counts_reuse_reconnect_failure_and_cancelled_futures() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            for _ in 0..2 {
                let mut input = Vec::new();
                let mut byte = [0];
                while !input.ends_with(b"\r\n\r\n") {
                    assert_eq!(stream.read(&mut byte).unwrap(), 1);
                    input.push(byte[0]);
                }
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .unwrap();
            }
        });
        let metrics = Arc::new(Mutex::new(Connections::default()));
        let client = reqwest::Client::builder()
            .no_proxy()
            .connector_layer(ConnectLayer(metrics.clone()))
            .build()
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                client
                    .get(format!("http://{address}"))
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap(),
                "ok"
            );
        }
        server.join().unwrap();
        assert!(
            client
                .get(format!("http://{address}"))
                .send()
                .await
                .is_err()
        );
        let pending = Attempt {
            metrics: metrics.clone(),
            start: Instant::now(),
            finished: false,
        };
        drop(pending);
        let mut collected = metrics.lock().unwrap();
        assert_eq!(collected.successful_ms.len(), 1); // two requests reused one TCP connection
        assert_ne!(collected.failed_ms.len(), 0);
        assert_eq!(collected.cancelled, 1);
        let report = collected.report(Some(2.0));
        assert_eq!(report["connections_per_second"], 0.5);
        assert!(
            report["failed_setup_duration_ms"]["p99_ms"]
                .as_f64()
                .unwrap()
                >= 0.0
        );
    }
}
