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
    pub fn record(&mut self, mut example: ErrorExample) {
        *self
            .counts
            .entry(format!("kind:{}", example.kind))
            .or_default() += 1;
        if matches!(example.kind.as_str(), "http" | "check" | "body_read") {
            if let Some(status) = example.status_code {
                *self.counts.entry(format!("status:{status}")).or_default() += 1;
            }
        }
        example.reason = truncate(&example.reason, MAX_REASON_BYTES).0;
        if let Some(body) = example.body_excerpt.take() {
            let (body, clipped) = truncate(&body, MAX_BODY_BYTES);
            example.body_excerpt = Some(body);
            example.body_truncated |= clipped;
        }
        if self.examples.len() < MAX_ERROR_EXAMPLES {
            eprintln!(
                "BENCH_ERROR {}",
                serde_json::to_string(&example).unwrap_or_default()
            );
            self.examples.push(example);
        } else {
            self.omitted += 1;
        }
    }

    pub fn merge(&mut self, other: &Self) {
        for (key, count) in &other.counts {
            *self.counts.entry(key.clone()).or_default() += count;
        }
        let room = MAX_ERROR_EXAMPLES.saturating_sub(self.examples.len());
        self.examples
            .extend(other.examples.iter().take(room).cloned());
        self.omitted += other.omitted + other.examples.len().saturating_sub(room);
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

fn sanitize_message_urls(message: &str) -> String {
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

#[allow(clippy::too_many_arguments)] // Keep the report's explicit fields at this boundary.
pub fn example(
    endpoint: &str,
    path: &str,
    status_code: Option<u16>,
    kind: &str,
    reason: impl Into<String>,
    body: Option<&[u8]>,
    user_index: usize,
    destination: &str,
    source_ip: Option<String>,
) -> ErrorExample {
    let (destination, _) = sanitize_url(destination);
    let (path, _) = sanitize_path(path);
    let (body_excerpt, body_truncated) = body.map_or((None, false), |bytes| {
        let clipped = bytes.len() > MAX_BODY_BYTES;
        let text = String::from_utf8_lossy(&bytes[..bytes.len().min(MAX_BODY_BYTES)]);
        let (text, text_clipped) = truncate(&text, MAX_BODY_BYTES);
        (Some(text), clipped || text_clipped)
    });
    ErrorExample {
        timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        endpoint: endpoint.to_owned(),
        path,
        status_code,
        kind: kind.to_owned(),
        reason: truncate(&reason.into(), MAX_REASON_BYTES).0,
        body_excerpt,
        body_truncated,
        latency_ms: None,
        user_index,
        destination,
        source_ip,
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

fn sanitize_url(value: &str) -> (String, bool) {
    let Ok(mut url) = reqwest::Url::parse(value) else {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(kind: &str, status: Option<u16>) -> ErrorExample {
        example(
            "GET /feed",
            "/a?token=x",
            status,
            kind,
            "é".repeat(600),
            Some("x".repeat(1500).as_bytes()),
            4,
            "http://user:secret@example.test:3000?q=token",
            None,
        )
    }

    #[test]
    fn bounds_text_counts_examples_and_merge_omissions() {
        let mut details = ErrorDetails::default();
        for _ in 0..101 {
            details.record(sample("http", Some(503)));
        }
        assert_eq!(details.counts["kind:http"], 101);
        assert_eq!(details.counts["status:503"], 101);
        assert_eq!(details.examples.len(), 100);
        assert_eq!(details.omitted, 1);
        assert!(details.examples[0].reason.len() <= MAX_REASON_BYTES);
        assert!(details.examples[0].body_excerpt.as_ref().unwrap().len() <= MAX_BODY_BYTES);
        assert!(details.examples[0].body_truncated);
        assert_eq!(details.examples[0].path, "/a");
        assert_eq!(details.examples[0].destination, "http://example.test:3000/");
        let serialized = serde_json::to_string(&details.examples[0]).unwrap();
        assert!(!serialized.contains("secret"));
        assert!(!serialized.contains("token=x"));

        let mut merged = ErrorDetails::default();
        merged.merge(&details);
        merged.merge(&details);
        assert_eq!(merged.counts["status:503"], 202);
        assert_eq!(merged.examples.len(), 100);
        assert_eq!(merged.omitted, 102);
    }

    #[test]
    fn truncation_keeps_utf8_boundaries() {
        let (value, clipped) = truncate(&"🦀".repeat(300), MAX_REASON_BYTES);
        assert!(clipped);
        assert!(value.len() <= MAX_REASON_BYTES);
        assert!(value.is_char_boundary(value.len()));
    }

    #[test]
    fn error_chains_redact_url_credentials_and_queries() {
        #[derive(Debug)]
        struct TestError(&'static str);
        impl std::fmt::Display for TestError {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.0)
            }
        }
        impl std::error::Error for TestError {}

        let message = error_chain(&TestError(
            "failed at http://user:secret@example.test/a?token=x",
        ));
        assert_eq!(message, "failed at http://example.test/a");
    }

    #[test]
    fn url_redaction_handles_ipv6_parentheses_and_malformed_urls() {
        for (value, expected) in [
            (
                "http://user:secret@[2001:db8::1]/p?token=x",
                "http://[2001:db8::1]/p",
            ),
            (
                "https://user)secret@example.test/path?key=value",
                "https://example.test/path",
            ),
        ] {
            let message = sanitize_message_urls(value);
            assert_eq!(message, expected);
            assert!(!message.contains("secret"));
            assert!(!message.contains("token=x"));
            assert!(!message.contains("key=value"));
        }

        let malformed = "http://user:secret@[2001:db8::1?token=x";
        let sanitized = sanitize_message_urls(malformed);
        assert_eq!(sanitized, "http://[2001:db8::1");
        assert!(!sanitized.contains("secret"));
        assert!(!sanitized.contains("token=x"));
    }
}
