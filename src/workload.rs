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
