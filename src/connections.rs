use super::{percentile, Samples};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
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
        self.cancelled += other.cancelled;
        for (error, count) in other.errors {
            *self.errors.entry(error).or_default() += count;
        }
    }
    pub fn report(&mut self, seconds: Option<f64>) -> Value {
        let mut all = self.successful_ms.clone();
        all.extend_from_slice(&self.failed_ms);
        let total = all.len();
        let rate = |n: usize| seconds.filter(|s| *s > 0.0).map(|s| n as f64 / s);
        json!({
            "scope": "client connection setup including DNS and TCP; pooled requests do not start a new attempt; an attempt may try multiple DNS addresses",
            "attempts": total, "established": self.successful_ms.len(), "failed": self.failed_ms.len(), "cancelled": self.cancelled,
            "attempts_per_second": rate(total), "connections_per_second": rate(self.successful_ms.len()), "failed_per_second": rate(self.failed_ms.len()),
            "setup_duration_ms": distribution(&mut all), "successful_setup_duration_ms": distribution(&mut self.successful_ms), "failed_setup_duration_ms": distribution(&mut self.failed_ms), "errors": self.errors,
        })
    }
}

fn distribution(samples: &mut [f64]) -> Value {
    samples.sort_unstable_by(f64::total_cmp);
    json!({"samples": samples.len(), "avg_ms": (!samples.is_empty()).then(|| samples.iter().sum::<f64>() / samples.len() as f64),
        "med_ms": percentile(samples, 0.5), "p95_ms": percentile(samples, 0.95), "p99_ms": percentile(samples, 0.99), "max_ms": samples.last()})
}

pub(super) fn request_rates(samples: &Samples, seconds: Option<f64>) -> Value {
    let n = samples.latency_ms.len();
    let rate = |count: usize| seconds.filter(|s| *s > 0.0).map(|s| count as f64 / s);
    json!({"requests_per_second": rate(n), "successful_requests_per_second": rate(n-samples.failed), "failed_requests_per_second": rate(samples.failed)})
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
            let key: String = error.chars().take(512).collect();
            let key = if metrics.errors.contains_key(&key) || metrics.errors.len() < 16 {
                key
            } else {
                "other connection errors".into()
            };
            *metrics.errors.entry(key).or_default() += 1;
        } else {
            metrics.successful_ms.push(elapsed);
        }
        self.finished = true;
    }
}
impl Drop for Attempt {
    fn drop(&mut self) {
        if !self.finished {
            self.metrics.lock().unwrap().cancelled += 1;
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
        assert!(client
            .get(format!("http://{address}"))
            .send()
            .await
            .is_err());
        let pending = Attempt {
            metrics: metrics.clone(),
            start: Instant::now(),
            finished: false,
        };
        drop(pending);
        let mut collected = metrics.lock().unwrap();
        assert_eq!(collected.successful_ms.len(), 1); // two requests reused one TCP connection
        assert!(!collected.failed_ms.is_empty());
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
