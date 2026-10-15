//! k6 request semantics around the shared reqwest transport.
use crate::{ENDPOINTS, Metrics, RequestContext, Token, check_success, errors, http_success};
use goose::prelude::*;
use hyper_util::client::legacy::{REQUEST_TIMING, RequestTiming};
use reqwest::{
    Url,
    cookie::{CookieStore, Jar},
    header::{self, HeaderValue},
};
use std::time::Duration;

pub(crate) fn js_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".into(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(number) => {
            // JSON.parse uses IEEE-754 numbers, including for integer literals.
            let number = number.as_f64().expect("finite JSON number");
            if number == 0.0 {
                "0".into()
            } else if !(1e-6..1e21).contains(&number.abs()) {
                let value = format!("{number:e}");
                let (mantissa, exponent) = value.split_once('e').unwrap();
                let exponent: i32 = exponent.parse().unwrap();
                format!("{mantissa}e{exponent:+}")
            } else {
                number.to_string()
            }
        }
        serde_json::Value::Array(values) => values
            .iter()
            .map(|value| {
                if value.is_null() {
                    String::new()
                } else {
                    js_string(value)
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        serde_json::Value::Object(_) => "[object Object]".into(),
        value => value.to_string(),
    }
}

tokio::task_local! { static COOKIES: Jar; }

#[derive(Default)]
pub(crate) struct IterationCookies;
impl CookieStore for IterationCookies {
    fn set_cookies(&self, headers: &mut dyn Iterator<Item = &HeaderValue>, url: &Url) {
        let _ = COOKIES.try_with(|jar| jar.set_cookies(headers, url));
    }
    fn cookies(&self, url: &Url) -> Option<HeaderValue> {
        COOKIES.try_with(|jar| jar.cookies(url)).ok().flatten()
    }
}

pub(crate) async fn iteration<F: Future>(future: F) -> F::Output {
    COOKIES.scope(Jar::default(), future).await
}

struct PendingSample<'a> {
    metrics: &'a Metrics,
    endpoint: usize,
    timing: RequestTiming,
    destination: String,
    path: String,
    user_index: usize,
    source_ip: Option<String>,
    recorded: bool,
}
impl PendingSample<'_> {
    fn finish(
        &mut self,
        status: u16,
        failed: bool,
        kind: &str,
        reason: String,
        bytes: Option<&[u8]>,
    ) {
        let ms = self.timing.elapsed().as_secs_f64() * 1000.0;
        let mut all = self.metrics.lock().unwrap();
        let sample = &mut all[self.endpoint];
        sample.latency_ms.push(ms);
        sample.failed += usize::from(failed);
        if failed {
            sample.errors.record(
                errors::example(
                    errors::ExampleContext {
                        endpoint: ENDPOINTS[self.endpoint],
                        path: &self.path,
                        user_index: self.user_index,
                        destination: &self.destination,
                        source_ip: self.source_ip.clone(),
                    },
                    (status != 0).then_some(status),
                    kind,
                    reason,
                    bytes,
                )
                .with_latency(ms),
            );
        }
        self.recorded = true;
    }
}
impl Drop for PendingSample<'_> {
    fn drop(&mut self) {
        if !self.recorded {
            self.finish(0, true, "cancelled", "HTTP request cancelled".into(), None);
        }
    }
}

fn forwarding_auth(from: &Url, to: &Url) -> bool {
    let Some(from) = from.host_str() else {
        return false;
    };
    let Some(to) = to.host_str() else {
        return false;
    };
    from == to || (from.parse::<std::net::IpAddr>().is_err() && to.ends_with(&format!(".{from}")))
}

fn redirect_url(response: &reqwest::Response) -> Option<Url> {
    if !matches!(response.status().as_u16(), 301 | 302 | 303 | 307 | 308) {
        return None;
    }
    let location = response.headers().get(header::LOCATION)?.to_str().ok()?;
    if location.is_empty() {
        return None;
    }
    response.url().join(location).ok()
}

pub(crate) async fn request(
    user: &mut GooseUser,
    metrics: &Metrics,
    endpoint: usize,
    path: &str,
    auth: Option<&Token>,
    body: Option<&serde_json::Value>,
    context: RequestContext<'_>,
) -> Result<Option<Vec<u8>>, Box<TransactionError>> {
    let method = if endpoint < 2 {
        GooseMethod::Get
    } else {
        GooseMethod::Post
    };
    let mut builder = user.get_request_builder(&method, path)?;
    if let Some(token) = auth {
        builder = builder
            .bearer_auth(&token.token)
            .header(header::CONTENT_TYPE, "application/json");
    }
    if let Some(body) = body {
        builder = builder.json(body);
    }
    let initial = builder
        .build()
        .map_err(|e| Box::new(TransactionError::Reqwest(e)))?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let result =
        tokio::time::timeout_at(deadline, hops(user, metrics, endpoint, initial, context)).await;
    let (status, bytes) = match result {
        Ok(result) => result?,
        Err(_) => (0, None),
    };
    // A script check runs only for the final response, never intermediate hops.
    if !check_success(endpoint, status) {
        let mut all = metrics.lock().unwrap();
        all[endpoint].checks_failed += 1;
        if http_success(status) {
            all[endpoint].errors.record(errors::example(
                errors::ExampleContext {
                    endpoint: ENDPOINTS[endpoint],
                    path,
                    user_index: context.user_offset + user.weighted_users_index,
                    destination: user.base_url.as_str(),
                    source_ip: context
                        .source_ips
                        .get(user.weighted_users_index % context.source_ips.len().max(1))
                        .map(ToString::to_string),
                },
                Some(status),
                "check",
                format!("unexpected HTTP status {status}"),
                None,
            ));
        }
    }
    Ok(bytes.filter(|_| check_success(endpoint, status)))
}

async fn hops(
    user: &mut GooseUser,
    metrics: &Metrics,
    endpoint: usize,
    mut request: reqwest::Request,
    context: RequestContext<'_>,
) -> Result<(u16, Option<Vec<u8>>), Box<TransactionError>> {
    let initial_url = request.url().clone();
    let mut strip_sensitive = false;
    for followed in 0..=10 {
        let timing = RequestTiming::default();
        let mut sample = PendingSample {
            metrics,
            endpoint,
            timing: timing.clone(),
            destination: request.url().to_string(),
            path: request.url().path().to_owned(),
            user_index: context.user_offset + user.weighted_users_index,
            source_ip: context
                .source_ips
                .get(user.weighted_users_index % context.source_ips.len().max(1))
                .map(ToString::to_string),
            recorded: false,
        };
        let next_template = request
            .try_clone()
            .expect("benchmark bodies are replayable");
        let method = if request.method() == reqwest::Method::GET {
            GooseMethod::Get
        } else {
            GooseMethod::Post
        };
        let path = request.url().to_string();
        let goose_request = GooseRequest::builder()
            .method(method)
            .path(path.as_str())
            .name(ENDPOINTS[endpoint].split_once(' ').unwrap().1)
            .set_request_builder(reqwest::RequestBuilder::from_parts(
                user.client.clone(),
                request,
            ))
            .build();
        let result = REQUEST_TIMING
            .scope(timing, user.request(goose_request))
            .await;
        let mut response = match result {
            Ok(response) => response,
            Err(error) => {
                sample.finish(
                    0,
                    true,
                    "request_builder",
                    errors::error_chain(&error),
                    None,
                );
                return Err(error);
            }
        };
        let response_body = match response.response {
            Ok(response) => response,
            Err(error) => {
                sample.finish(
                    0,
                    true,
                    "transport",
                    errors::error_chain(&error.without_url()),
                    None,
                );
                return Ok((0, None));
            }
        };
        let status = response_body.status().as_u16();
        let next_url = (followed < 10)
            .then(|| redirect_url(&response_body))
            .flatten();
        if let Some(next_url) = next_url {
            let previous = next_template.url().clone();
            // Go drains only small intermediate bodies before closing them.
            let mut redirect_response = response_body;
            if redirect_response.content_length().is_none_or(|n| n <= 2048) {
                let mut remaining = 2048_usize;
                while remaining > 0 {
                    match redirect_response.chunk().await {
                        Ok(Some(chunk)) => remaining = remaining.saturating_sub(chunk.len()),
                        _ => break,
                    }
                }
            }
            drop(redirect_response);
            sample.finish(
                status,
                !http_success(status),
                "http",
                format!("HTTP status {status}"),
                None,
            );
            if !user.config.no_metrics {
                user.set_success(&mut response.request)?;
            }
            request = next_template;
            *request.url_mut() = next_url.clone();
            if matches!(status, 301..=303)
                && request.method() != reqwest::Method::GET
                && request.method() != reqwest::Method::HEAD
            {
                *request.method_mut() = reqwest::Method::GET;
                *request.body_mut() = None;
                for name in [
                    header::CONTENT_TYPE,
                    header::CONTENT_LENGTH,
                    header::TRANSFER_ENCODING,
                    header::CONTENT_ENCODING,
                    header::CONTENT_LANGUAGE,
                    header::CONTENT_LOCATION,
                ] {
                    request.headers_mut().remove(name);
                }
            }
            strip_sensitive |= !forwarding_auth(&initial_url, &next_url);
            if strip_sensitive {
                for name in [
                    "authorization",
                    "proxy-authorization",
                    "cookie",
                    "cookie2",
                    "www-authenticate",
                    "proxy-authenticate",
                ] {
                    request.headers_mut().remove(name);
                }
            }
            if !(previous.scheme() == "https" && next_url.scheme() == "http") {
                let mut referer = previous;
                let _ = referer.set_username("");
                let _ = referer.set_password(None);
                if let Ok(value) = HeaderValue::from_str(referer.as_str()) {
                    request.headers_mut().insert(header::REFERER, value);
                }
            } else {
                request.headers_mut().remove(header::REFERER);
            }
            continue;
        }
        let bytes = match response_body.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                sample.finish(
                    status,
                    true,
                    "body_read",
                    errors::error_chain(&error.without_url()),
                    None,
                );
                return Ok((0, None));
            }
        };
        // Stop before copying/diagnostic accounting: the complete HTTP body is here.
        sample.finish(
            status,
            !http_success(status),
            "http",
            format!("HTTP status {status}"),
            Some(&bytes),
        );
        if !user.config.no_metrics {
            if http_success(status) {
                user.set_success(&mut response.request)?;
            } else {
                let _ = user.set_failure("HTTP failure", &mut response.request, None, None);
            }
        }
        return Ok((status, Some(bytes.to_vec())));
    }
    unreachable!()
}
