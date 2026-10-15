use crate::{
    errors,
    samples::{Metrics, Samples},
    transport,
};
use chrono::{SecondsFormat, Utc};
use rand::Rng;
use serde::Deserialize;
use std::{
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use twelve_dollar_benchmark::ENDPOINTS;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Endpoint {
    Feed,
    Post,
    Like,
    Create,
}

impl Endpoint {
    #[cfg(test)]
    const ALL: [Self; 4] = [Self::Feed, Self::Post, Self::Like, Self::Create];

    const fn name(self) -> &'static str {
        ENDPOINTS[self as usize]
    }

    fn method(self) -> http::Method {
        match self {
            Self::Feed | Self::Post => http::Method::GET,
            Self::Like | Self::Create => http::Method::POST,
        }
    }

    #[must_use]
    fn successful_status(self, status: u16) -> bool {
        match self {
            Self::Feed | Self::Post => status == 200,
            Self::Like => status == 200 || status == 201,
            Self::Create => status == 201,
        }
    }
}

#[derive(Deserialize)]
pub(super) struct Token {
    username: String,
    #[serde(rename = "token")]
    credential: String,
    #[serde(skip)]
    authorization: std::sync::OnceLock<http::HeaderValue>,
}
#[derive(Deserialize)]
struct Feed {
    posts: Vec<Post>,
}
#[derive(Deserialize)]
struct Post {
    id: u64,
}

pub(super) struct User {
    pub(super) client: transport::Client,
    pub(super) base: Arc<url::Url>,
    pub(super) index: usize,
    pub(super) source: Option<IpAddr>,
    pub(super) metrics: Metrics,
    pub(super) runtime_errors: Arc<AtomicUsize>,
    pub(super) runtime_details: Arc<Mutex<errors::ErrorDetails>>,
}

fn http_success(status: u16) -> bool {
    (200..400).contains(&status)
}
#[must_use]
fn sample_mut(samples: &mut [Samples; 4], endpoint: Endpoint) -> &mut Samples {
    &mut samples[endpoint as usize]
}

pub(super) async fn think(lo: f64, hi: f64) {
    let seconds = rand::rng().random_range(lo..hi);
    tokio::time::sleep(Duration::from_secs_f64(seconds)).await;
}

pub(super) fn parse_source_ips(value: &str) -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    let mut ips = Vec::new();
    if value.trim().is_empty() {
        return Ok(ips);
    }
    for entry in value.split(',').map(str::trim) {
        if let Some((prefix, range)) = entry.split_once(".[") {
            let range = range.strip_suffix(']').ok_or("invalid SOURCE_IPS range")?;
            let (first, last) = range.split_once('-').ok_or("invalid SOURCE_IPS range")?;
            let first: u8 = first.parse()?;
            let last: u8 = last.parse()?;
            if first > last {
                return Err("SOURCE_IPS ranges must be ascending".into());
            }
            let [first_octet, second_octet, third_octet, _] =
                format!("{prefix}.0").parse::<Ipv4Addr>()?.octets();
            for n in first..=last {
                ips.push(IpAddr::V4(Ipv4Addr::new(
                    first_octet,
                    second_octet,
                    third_octet,
                    n,
                )));
            }
        } else {
            ips.push(entry.parse()?);
        }
    }
    for ip in &ips {
        if ip.is_unspecified()
            || ip.is_multicast()
            || matches!(ip, IpAddr::V4(v4) if v4.is_broadcast())
        {
            return Err("SOURCE_IPS requires specific unicast IP addresses".into());
        }
    }
    Ok(ips)
}
pub(super) fn validate_source_ips(ips: &[IpAddr]) -> Result<(), Box<dyn std::error::Error>> {
    for ip in ips {
        // Bind only: no requests, interface changes, or dependency on the target.
        TcpListener::bind(SocketAddr::new(*ip, 0))
            .map_err(|error| format!("cannot bind SOURCE_IPS address {ip}: {error}"))?;
    }
    Ok(())
}

pub(super) fn load_tokens(path: &Path) -> Result<Vec<Token>, Box<dyn std::error::Error>> {
    let tokens: Vec<Token> = serde_json::from_slice(&fs::read(path)?)?;
    if tokens.is_empty()
        || tokens
            .iter()
            .any(|t| t.username.is_empty() || t.credential.is_empty())
    {
        return Err("TOKENS must contain a non-empty array of usernames and tokens".into());
    }
    for token in &tokens {
        let mut authorization =
            http::HeaderValue::from_str(&format!("Bearer {}", token.credential))
                .map_err(|_| "TOKENS contains an invalid bearer header value")?;
        authorization.set_sensitive(true);
        token
            .authorization
            .set(authorization)
            .map_err(|_| "unable to cache bearer header")?;
    }
    Ok(tokens)
}
pub(super) fn runtime_error(
    details: &Arc<Mutex<errors::ErrorDetails>>,
    count: &AtomicUsize,
    error: &(dyn std::error::Error + 'static),
    user_index: usize,
    url: &str,
    source: Option<IpAddr>,
) {
    count.fetch_add(1, Ordering::Relaxed);
    details.lock().unwrap().record_lazy(None, "runtime", || {
        errors::example(
            errors::ErrorLocation {
                endpoint: "runtime",
                path: "",
                user_index,
                destination: url,
                source_ip: source.map(|v| v.to_string()),
            },
            None,
            "runtime",
            errors::error_chain(error),
            None,
        )
    });
}

fn should_capture_http_excerpt(user: &User, endpoint: Endpoint) -> bool {
    sample_mut(&mut user.metrics.lock().unwrap(), endpoint)
        .errors
        .examples
        .len()
        < errors::MAX_ERROR_EXAMPLES
}

async fn request(
    user: &mut User,
    endpoint: Endpoint,
    path: &str,
    auth: Option<&Token>,
    body: Option<&serde_json::Value>,
) -> Option<transport::RetainedBody> {
    let body = match body.map(serde_json::to_vec).transpose() {
        Ok(body) => body.map(bytes::Bytes::from),
        Err(error) => {
            runtime_error(
                &user.runtime_details,
                &user.runtime_errors,
                &error,
                user.index,
                user.base.as_str(),
                user.source,
            );
            record_response(
                user,
                endpoint,
                path,
                &transport::Response::failed(0, "request_builder", error.to_string(), 0.0),
            );
            return None;
        }
    };
    let capture_excerpt = should_capture_http_excerpt(user, endpoint);
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        user.client.request(
            endpoint.method(),
            path,
            auth.map(|token| token.authorization.get().expect("preflighted token header")),
            body,
            endpoint == Endpoint::Feed,
            capture_excerpt,
        ),
    )
    .await;
    let response = match result {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => transport::Response::failed(
            0,
            "transport",
            error.to_string(),
            user.client.take_request_latency_ms(),
        ),
        Err(_) => {
            user.client.discard_connection();
            transport::Response::failed(
                0,
                "transport",
                "whole-request timeout after 60 seconds",
                user.client.take_request_latency_ms(),
            )
        }
    };
    record_response(user, endpoint, path, &response);
    response
        .retained
        .filter(|_| endpoint == Endpoint::Feed && endpoint.successful_status(response.status))
}

fn record_response(user: &User, endpoint: Endpoint, path: &str, response: &transport::Response) {
    let status = response.status;
    let latency_ms = response.latency_ms;
    let body_complete = response.failure.is_none();
    let bytes = response.retained.as_deref().or_else(|| {
        (!response.error_excerpt.is_empty()).then_some(response.error_excerpt.as_ref())
    });
    let failure = response
        .failure
        .as_ref()
        .map(|(kind, reason)| (*kind, reason.as_str()));
    let success = http_success(status) && body_complete;
    let mut metrics = user.metrics.lock().unwrap();
    let samples = sample_mut(&mut metrics, endpoint);
    samples.latency_ms.push(latency_ms);
    samples.failed += usize::from(!success);
    samples.checks_failed += usize::from(!endpoint.successful_status(status) || !body_complete);
    let error = if let Some((kind, reason)) = failure {
        Some((kind, Some(reason), None))
    } else if !http_success(status) {
        Some(("http", None, bytes))
    } else if !endpoint.successful_status(status) {
        Some(("check", None, None))
    } else {
        None
    };
    if let Some((kind, failure, body)) = error {
        samples
            .errors
            .record_lazy((status != 0).then_some(status), kind, || {
                errors::example(
                    errors::ErrorLocation {
                        endpoint: endpoint.name(),
                        path,
                        user_index: user.index,
                        destination: user.base.as_str(),
                        source_ip: user.source.map(|v| v.to_string()),
                    },
                    (status != 0).then_some(status),
                    kind,
                    failure.map_or_else(
                        || {
                            if kind == "http" {
                                format!("HTTP status {status}")
                            } else {
                                format!("unexpected HTTP status {status}")
                            }
                        },
                        errors::sanitize_message_urls,
                    ),
                    body,
                )
                .with_latency(latency_ms)
            });
    }
}

fn choose_post_id(feed: &Feed) -> Option<u64> {
    if feed.posts.is_empty() || feed.posts.iter().any(|post| post.id == 0) {
        return None;
    }
    feed.posts
        .get(rand::rng().random_range(0..feed.posts.len()))
        .map(|post| post.id)
}

pub(super) async fn browse(user: &mut User, tokens: &[Token], iteration: usize) {
    let Some(token) = tokens.get(user.index % tokens.len().max(1)) else {
        runtime_error(
            &user.runtime_details,
            &user.runtime_errors,
            &std::io::Error::other("validated token set is empty"),
            user.index,
            user.base.as_str(),
            user.source,
        );
        return;
    };
    let body = request(user, Endpoint::Feed, "/feed", None, None).await;
    let post_id = body
        .as_deref()
        .and_then(|body| serde_json::from_slice::<Feed>(body).ok())
        .as_ref()
        .and_then(choose_post_id);
    let invalid_feed = body.is_some() && post_id.is_none();
    drop(body);
    if invalid_feed {
        let mut metrics = user.metrics.lock().unwrap();
        let samples = sample_mut(&mut metrics, Endpoint::Feed);
        samples.checks_failed += 1;
        samples.errors.record_lazy(Some(200), "check", || {
            errors::example(
                errors::ErrorLocation {
                    endpoint: Endpoint::Feed.name(),
                    path: "/feed",
                    user_index: user.index,
                    destination: user.base.as_str(),
                    source_ip: user.source.map(|v| v.to_string()),
                },
                Some(200),
                "check",
                "feed body was empty or malformed",
                None,
            )
        });
    }
    think(3.0, 7.0).await;
    let Some(post_id) = post_id else {
        return;
    };
    request(
        user,
        Endpoint::Post,
        &format!("/posts/{post_id}"),
        None,
        None,
    )
    .await;
    think(3.0, 8.0).await;
    if rand::rng().random_bool(0.15) {
        request(
            user,
            Endpoint::Like,
            &format!("/posts/{post_id}/like"),
            Some(token),
            None,
        )
        .await;
    }
    if rand::rng().random_bool(0.02) {
        let body = serde_json::json!({"body": format!(
            "{} says hi at {} (VU {}, iter {})", token.username,
            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true), user.index.saturating_add(1), iteration
        )});
        request(user, Endpoint::Create, "/posts", Some(token), Some(&body)).await;
    }
    think(5.0, 15.0).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{connections, samples::percentile};
    use std::{
        io::{Read, Write},
        net::{SocketAddr, TcpStream},
        thread::JoinHandle,
    };
    fn serve(respond: impl FnOnce(TcpStream) + Send + 'static) -> (SocketAddr, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || respond(listener.accept().unwrap().0));
        (address, server)
    }

    fn read_headers(stream: &mut impl Read) -> Vec<u8> {
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = stream.read(&mut buffer).unwrap();
            assert_ne!(count, 0);
            request.extend_from_slice(&buffer[..count]);
        }
        request
    }

    fn user(base: &str, metrics: Metrics) -> User {
        let base = Arc::new(url::Url::parse(base).unwrap());
        User {
            client: transport::Client::new(
                transport::ParsedBase::new(base.clone()),
                None,
                Arc::new(Mutex::new(connections::Connections::default())),
            ),
            base,
            index: 7,
            source: None,
            metrics,
            runtime_errors: Arc::new(AtomicUsize::new(0)),
            runtime_details: Arc::new(Mutex::new(errors::ErrorDetails::default())),
        }
    }

    #[test]
    fn percentile_and_http_check_semantics_match_benchmark() {
        let mut samples = Samples {
            latency_ms: vec![1000.1, 499.9, 0.1, 500.1],
            ..Default::default()
        };
        let summary = samples.summary();
        assert_eq!(summary.latency.median, Some(500.0));
        assert!((summary.latency.p95.unwrap() - 925.1).abs() < 1e-9);
        assert!((summary.latency.p99.unwrap() - 985.1).abs() < 1e-9);
        assert_eq!(percentile(&[], 0.99), None);
        assert_eq!(percentile(&[1.0], f64::NAN), None);
        assert_eq!(percentile(&[1.0], 1.01), None);
        assert!(http_success(302));
        assert!(http_success(204));
        assert!(!Endpoint::Feed.successful_status(204));
        assert!(!Endpoint::Create.successful_status(200));
        assert!(Endpoint::Like.successful_status(200));
        assert!(Endpoint::Like.successful_status(201));
        assert_eq!(Endpoint::ALL.map(Endpoint::name), ENDPOINTS);
        for status in [0, 199, 400, 500] {
            assert!(!http_success(status));
        }
    }

    #[test]
    fn feed_post_selection_handles_empty_invalid_and_valid_lists() {
        assert_eq!(choose_post_id(&Feed { posts: vec![] }), None);
        assert_eq!(
            choose_post_id(&Feed {
                posts: vec![Post { id: 0 }]
            }),
            None
        );
        assert_eq!(
            choose_post_id(&Feed {
                posts: vec![Post { id: 42 }]
            }),
            Some(42)
        );
    }

    #[test]
    fn source_ip_parser_accepts_ranges_and_rejects_non_unicast_or_malformed_values() {
        assert_eq!(parse_source_ips("  ").unwrap(), Vec::<IpAddr>::new());
        assert_eq!(
            parse_source_ips("127.0.0.[1-3]").unwrap(),
            [1, 2, 3].map(|n| IpAddr::V4(Ipv4Addr::new(127, 0, 0, n)))
        );
        assert_eq!(
            parse_source_ips("192.0.2.10, 192.0.2.[11-12], ::1")
                .unwrap()
                .len(),
            4
        );
        for value in [
            "127.0.0.[5-1]",
            "127.0.0.[1-256]",
            "127.0.0.[1-5",
            "127.0.0.[-1-5]",
            "127.0.0.[1]",
            "127.0.[1-5]",
            "127.0.0.1,",
            "localhost",
            "0.0.0.0",
            "::",
            "224.0.0.1",
            "ff02::1",
            "255.255.255.255",
        ] {
            assert!(parse_source_ips(value).is_err(), "accepted {value}");
        }
    }

    #[test]
    fn token_file_preflights_bearer_headers() {
        let path = std::env::temp_dir().join(format!(
            "benchmark-tokens-{}-{}.json",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::write(&path, r#"[{"username":"u","token":"bad\nheader"}]"#).unwrap();
        assert!(load_tokens(&path).is_err());
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn empty_validated_token_set_is_a_runtime_error_not_a_panic() {
        let metrics = Arc::new(Mutex::new(std::array::from_fn(|_| Samples::default())));
        let mut user = user("http://127.0.0.1:3000", metrics);
        browse(&mut user, &[], 0).await;
        assert_eq!(user.runtime_errors.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn request_drains_write_response_and_preserves_auth_and_body_timing() {
        let (address, server) = serve(|mut stream| {
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut input = read_headers(&mut stream);
            let mut buf = [0; 1024];
            let headers = String::from_utf8_lossy(&input).to_ascii_lowercase();
            let length = headers
                .lines()
                .find_map(|l| {
                    l.strip_prefix("content-length:")?
                        .trim()
                        .parse::<usize>()
                        .ok()
                })
                .unwrap();
            let body_start = input.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
            while input.len() < body_start + length {
                let n = stream.read(&mut buf).unwrap();
                input.extend_from_slice(&buf[..n]);
            }
            assert!(headers.contains("authorization: bearer secret"));
            assert!(String::from_utf8_lossy(&input[body_start..]).contains("hello"));
            stream
                .write_all(
                    b"HTTP/1.1 201 Created\r\nContent-Length: 13\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            std::thread::sleep(Duration::from_millis(80));
            stream.write_all(b"complete body").unwrap();
        });
        let metrics: Metrics = Arc::new(Mutex::new(std::array::from_fn(|_| Samples::default())));
        let mut user = user(&format!("http://{address}"), metrics.clone());
        let token = Token {
            username: "u".into(),
            credential: "secret".into(),
            authorization: std::sync::OnceLock::new(),
        };
        let mut authorization = http::HeaderValue::from_static("Bearer secret");
        authorization.set_sensitive(true);
        token.authorization.set(authorization).unwrap();
        let payload = serde_json::json!({"body":"hello"});
        let response = request(
            &mut user,
            Endpoint::Create,
            "/posts",
            Some(&token),
            Some(&payload),
        )
        .await;
        assert!(response.is_none());
        server.join().unwrap();
        let mut data = metrics.lock().unwrap();
        let create = sample_mut(&mut data, Endpoint::Create);
        assert_eq!(create.latency_ms.len(), 1);
        assert!(
            create.latency_ms[0] >= 60.0,
            "body delay excluded from latency: {} ms",
            create.latency_ms[0]
        );
        assert_eq!(create.failed, 0);
        assert_eq!(user.runtime_errors.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn successful_feed_returns_bytes_and_non_feed_drain_reuses_connection() {
        let (address, server) = serve(|mut stream| {
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            assert!(read_headers(&mut stream).starts_with(b"POST /posts "));
            let body = vec![b'x'; 4096];
            write!(
                stream,
                "HTTP/1.1 201 Created\r\nContent-Length: {}\r\n\r\n",
                body.len()
            )
            .unwrap();
            for chunk in body.chunks(1024) {
                stream.write_all(chunk).unwrap();
                stream.flush().unwrap();
            }

            assert!(read_headers(&mut stream).starts_with(b"GET /feed "));
            let body = br#"{"posts":[{"id":1}]}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            stream.write_all(body).unwrap();
        });

        let metrics: Metrics = Arc::new(Mutex::new(std::array::from_fn(|_| Samples::default())));
        let mut user = user(&format!("http://{address}"), metrics);
        let write_response = request(&mut user, Endpoint::Create, "/posts", None, None).await;
        assert!(write_response.is_none());
        let feed_response = request(&mut user, Endpoint::Feed, "/feed", None, None)
            .await
            .unwrap();
        assert_eq!(&feed_response[..], br#"{"posts":[{"id":1}]}"#);
        server.join().unwrap();
    }

    #[tokio::test]
    async fn transport_failure_is_sampled_and_counted() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let metrics: Metrics = Arc::new(Mutex::new(std::array::from_fn(|_| Samples::default())));
        let mut user = user(&format!("http://{address}"), metrics.clone());
        let response = request(&mut user, Endpoint::Feed, "/feed", None, None).await;
        assert!(response.is_none());
        let mut data = metrics.lock().unwrap();
        let feed = sample_mut(&mut data, Endpoint::Feed);
        assert_eq!(feed.failed, 1);
        assert_eq!(feed.latency_ms, [0.0]);
        assert_eq!(feed.errors.counts["kind:transport"], 1);
    }

    #[tokio::test]
    async fn truncated_response_body_is_recorded_as_body_read_failure() {
        let (address, server) = serve(|mut stream| {
            read_headers(&mut stream);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\nConnection: close\r\n\r\nshort",
                )
                .unwrap();
        });
        let metrics = Arc::new(Mutex::new(std::array::from_fn(|_| Samples::default())));
        let mut user = user(&format!("http://{address}"), metrics.clone());
        assert!(
            request(&mut user, Endpoint::Feed, "/feed", None, None)
                .await
                .is_none()
        );
        server.join().unwrap();
        let mut metrics = metrics.lock().unwrap();
        let feed = sample_mut(&mut metrics, Endpoint::Feed);
        assert_eq!(feed.failed, 1);
        assert_eq!(feed.errors.counts["kind:body_read"], 1);
    }

    #[tokio::test]
    async fn http_failure_example_keeps_status_and_response_body() {
        let (address, server) = serve(|mut stream| {
            read_headers(&mut stream);
            stream.write_all(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 11\r\nConnection: close\r\n\r\nunavailable").unwrap();
        });
        let metrics = Arc::new(Mutex::new(std::array::from_fn(|_| Samples::default())));
        let mut user = user(&format!("http://{address}"), metrics.clone());
        assert!(
            request(&mut user, Endpoint::Feed, "/feed", None, None)
                .await
                .is_none()
        );
        server.join().unwrap();
        let mut metrics = metrics.lock().unwrap();
        let feed = sample_mut(&mut metrics, Endpoint::Feed);
        assert_eq!(feed.errors.counts["kind:http"], 1);
        let example = feed.errors.examples.first().unwrap();
        assert_eq!(example.status_code, Some(503));
        assert_eq!(example.body_excerpt.as_deref(), Some("unavailable"));
    }

    #[tokio::test]
    async fn http_failure_body_excerpt_is_bounded_and_marks_truncation() {
        let (address, server) = serve(|mut stream| {
            read_headers(&mut stream);
            let body = vec![b'x'; 4096];
            write!(
                stream,
                "HTTP/1.1 503 Unavailable\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            stream.write_all(&body).unwrap();
        });
        let metrics = Arc::new(Mutex::new(std::array::from_fn(|_| Samples::default())));
        let mut user = user(&format!("http://{address}"), metrics.clone());
        assert!(
            request(&mut user, Endpoint::Feed, "/feed", None, None)
                .await
                .is_none()
        );
        server.join().unwrap();
        let mut metrics = metrics.lock().unwrap();
        let feed = sample_mut(&mut metrics, Endpoint::Feed);
        let example = feed.errors.examples.first().unwrap();
        assert_eq!(example.body_excerpt.as_ref().unwrap().len(), 1024);
        assert!(example.body_truncated);
    }

    #[tokio::test]
    async fn clients_bind_source_ips_and_connector_tracks_reconnects() {
        let ips = parse_source_ips("127.0.0.[2-3]").unwrap();
        validate_source_ips(&ips).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut peers = Vec::new();
            for _ in 0..2 {
                let (mut stream, peer) = listener.accept().unwrap();
                peers.push(peer.ip());
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                read_headers(&mut stream);
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .unwrap();
            }
            peers
        });
        let connection_metrics = Arc::new(Mutex::new(connections::Connections::default()));
        for ip in &ips {
            let mut client = transport::Client::new(
                transport::ParsedBase::new(Arc::new(
                    url::Url::parse(&format!("http://{address}")).unwrap(),
                )),
                Some(*ip),
                connection_metrics.clone(),
            );
            let response = client
                .request(http::Method::GET, "/", None, None, true, false)
                .await
                .unwrap();
            let retained = response.retained.unwrap();
            assert_eq!(&*retained, b"ok".as_slice());
        }
        let peers = server.join().unwrap();
        assert_eq!(peers, ips);
        let mut report = connection_metrics.lock().unwrap();
        assert_eq!(report.successful_ms.len(), 2);
        assert_eq!(
            std::mem::take(&mut *report).report(Some(1.0))["established"],
            2
        );
    }
}
