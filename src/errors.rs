use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MAX_ERROR_EXAMPLES: usize = 100;
const MAX_REASON_BYTES: usize = 1024;
const MAX_BODY_BYTES: usize = 1024;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ErrorDetails {
    pub counts: BTreeMap<String, usize>,
    pub examples: Vec<ErrorExample>,
    pub omitted: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorExample {
    pub timestamp: String,
    pub endpoint: String,
    pub path: String,
    pub status_code: Option<u16>,
    pub kind: String,
    pub reason: String,
    pub body_excerpt: Option<String>,
    pub body_truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<f64>,
    pub user_index: usize,
    pub destination: String,
    pub source_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
}

impl ErrorExample {
    pub fn with_latency(mut self, latency_ms: f64) -> Self {
        self.latency_ms = Some(latency_ms);
        self
    }
}

impl ErrorDetails {
    pub fn record_lazy(
        &mut self,
        status_code: Option<u16>,
        kind: &str,
        make_example: impl FnOnce() -> ErrorExample,
    ) {
        self.increment_counts(status_code, kind);
        if self.examples.len() < MAX_ERROR_EXAMPLES {
            self.retain(make_example());
        } else {
            self.omitted = self.omitted.saturating_add(1);
        }
    }

    fn increment_counts(&mut self, status_code: Option<u16>, kind: &str) {
        let kind_count = self.counts.entry(format!("kind:{kind}")).or_default();
        *kind_count = kind_count.saturating_add(1);
        if matches!(kind, "http" | "check" | "body_read")
            && let Some(status) = status_code
        {
            let count = self.counts.entry(format!("status:{status}")).or_default();
            *count = count.saturating_add(1);
        }
    }

    fn retain(&mut self, mut example: ErrorExample) {
        if self.examples.len() >= MAX_ERROR_EXAMPLES {
            self.omitted = self.omitted.saturating_add(1);
            return;
        }
        normalize_example(&mut example);
        eprintln!(
            "BENCH_ERROR {}",
            serde_json::to_string(&example).unwrap_or_default()
        );
        self.examples.push(example);
    }

    pub fn merge(&mut self, other: &Self) {
        for (key, count) in &other.counts {
            let total = self.counts.entry(key.clone()).or_default();
            *total = total.saturating_add(*count);
        }
        let room = MAX_ERROR_EXAMPLES.saturating_sub(self.examples.len());
        self.examples
            .extend(other.examples.iter().take(room).cloned());
        self.omitted = self
            .omitted
            .saturating_add(other.omitted)
            .saturating_add(other.examples.len().saturating_sub(room));
    }
}

pub fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    sanitize_message_urls(&message)
}

pub(super) fn sanitize_message_urls(message: &str) -> String {
    let mut sanitized = String::with_capacity(message.len());
    let mut rest = message;
    while let Some(start) = ["http://", "https://"]
        .iter()
        .filter_map(|scheme| rest.find(scheme))
        .min()
    {
        sanitized.push_str(&rest[..start]);
        let url_start = &rest[start..];
        let end = url_start
            .find(char::is_whitespace)
            .unwrap_or(url_start.len());
        let raw_url = &url_start[..end];
        let (url, _) = sanitize_url(raw_url);
        sanitized.push_str(&url);
        rest = &url_start[end..];
    }
    sanitized.push_str(rest);
    sanitized
}

pub struct ErrorLocation<'a> {
    pub endpoint: &'a str,
    pub path: &'a str,
    pub user_index: usize,
    pub destination: &'a str,
    pub source_ip: Option<String>,
}

pub fn example(
    location: ErrorLocation<'_>,
    status_code: Option<u16>,
    kind: &str,
    reason: impl Into<String>,
    body: Option<&[u8]>,
) -> ErrorExample {
    let (destination, _) = sanitize_url(location.destination);
    let (path, _) = sanitize_path(location.path);
    let (body_excerpt, body_truncated) = body.map_or((None, false), |bytes| {
        let clipped = bytes.len() > MAX_BODY_BYTES;
        let text = String::from_utf8_lossy(&bytes[..bytes.len().min(MAX_BODY_BYTES)]);
        let (text, text_clipped) = truncate(&text, MAX_BODY_BYTES);
        (Some(text), clipped || text_clipped)
    });
    ErrorExample {
        timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        endpoint: location.endpoint.to_owned(),
        path,
        status_code,
        kind: kind.to_owned(),
        reason: truncate_owned(reason.into(), MAX_REASON_BYTES).0,
        body_excerpt,
        body_truncated,
        latency_ms: None,
        user_index: location.user_index,
        destination,
        source_ip: location.source_ip,
        worker: None,
    }
}

fn truncate(value: &str, max: usize) -> (String, bool) {
    if value.len() <= max {
        return (value.to_owned(), false);
    }
    let mut end = max;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (value[..end].to_owned(), true)
}

fn truncate_owned(mut value: String, max: usize) -> (String, bool) {
    if value.len() <= max {
        return (value, false);
    }
    let mut end = max;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    (value, true)
}

fn normalize_example(example: &mut ErrorExample) {
    example.reason = truncate_owned(std::mem::take(&mut example.reason), MAX_REASON_BYTES).0;
    if let Some(body) = example.body_excerpt.take() {
        let (body, clipped) = truncate_owned(body, MAX_BODY_BYTES);
        example.body_excerpt = Some(body);
        example.body_truncated |= clipped;
    }
}

fn sanitize_url(value: &str) -> (String, bool) {
    let Ok(mut url) = url::Url::parse(value) else {
        return (sanitize_unparsed_url(value), true);
    };
    let mut changed = false;
    if !url.username().is_empty() || url.password().is_some() {
        let _ = url.set_username("");
        let _ = url.set_password(None);
        changed = true;
    }
    if url.query().is_some() {
        url.set_query(None);
        changed = true;
    }
    if url.fragment().is_some() {
        url.set_fragment(None);
        changed = true;
    }
    (url.to_string(), changed)
}

fn sanitize_unparsed_url(value: &str) -> String {
    let without_query = value.split_once('?').map_or(value, |(url, _)| url);
    let Some((scheme, rest)) = without_query.split_once("://") else {
        return without_query.to_owned();
    };
    let authority_end = rest.find(['/', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    format!("{scheme}://{host}{}", &rest[authority_end..])
}

fn sanitize_path(value: &str) -> (String, bool) {
    match value.split_once('?') {
        Some((path, _)) => (path.to_owned(), true),
        None => (value.to_owned(), false),
    }
}
