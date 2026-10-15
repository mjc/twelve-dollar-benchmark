use chrono::{SecondsFormat, Utc};
use num_traits::ToPrimitive;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    env, fs,
    io::{BufWriter, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    path::Path,
    process::ExitCode,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::task::{AbortHandle, JoinSet};
use twelve_dollar_benchmark::{ENDPOINTS, control, setting, telemetry};

mod config;
mod connections;
mod destinations;
mod errors;
mod runner;
mod scoring;
mod transport;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Endpoint {
    Feed,
    Post,
    Like,
    Create,
}

impl Endpoint {
    const ALL: [Self; 4] = [Self::Feed, Self::Post, Self::Like, Self::Create];

    const fn name(self) -> &'static str {
        let [feed, post, like, create] = ENDPOINTS;
        match self {
            Self::Feed => feed,
            Self::Post => post,
            Self::Like => like,
            Self::Create => create,
        }
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
struct Token {
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

#[derive(Default, Serialize, Deserialize)]
struct Samples {
    // ponytail: exact samples match k6 and use about 8 MB per million requests.
    latency_ms: Vec<f64>,
    failed: usize,
    checks_failed: usize,
    #[serde(default)]
    errors: errors::ErrorDetails,
}
type Metrics = Arc<Mutex<[Samples; 4]>>;

struct User {
    client: transport::Client,
    base: Arc<url::Url>,
    index: usize,
    source: Option<IpAddr>,
    metrics: Metrics,
    runtime_errors: Arc<AtomicUsize>,
    runtime_details: Arc<Mutex<errors::ErrorDetails>>,
}

#[derive(Serialize, Debug)]
struct Summary {
    requests: usize,
    failed: usize,
    failure_rate: Option<f64>,
    checks_failed: usize,
    avg_ms: Option<f64>,
    med_ms: Option<f64>,
    p95_ms: Option<f64>,
    p99_ms: Option<f64>,
    max_ms: Option<f64>,
    errors: errors::ErrorDetails,
}
impl Samples {
    fn summary(&mut self) -> Summary {
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

fn aggregate_samples(samples: &mut [Samples]) -> Samples {
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
fn percentile(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() || !p.is_finite() || !(0.0..=1.0).contains(&p) {
        return None;
    }
    let rank = p * (sorted.len() - 1).to_f64()?;
    let low = *sorted.get(rank.floor().to_usize()?)?;
    let high = *sorted.get(rank.ceil().to_usize()?)?;
    Some(low + (high - low) * rank.fract())
}
#[must_use]
fn passes(s: &Summary, runtime_errors: usize) -> bool {
    runtime_errors == 0
        && s.p95_ms.is_some_and(|v| v < 500.0)
        && s.p99_ms.is_some_and(|v| v < 1000.0)
        && s.failure_rate.is_some_and(|v| v < 0.01)
}
fn http_success(status: u16) -> bool {
    (200..400).contains(&status)
}
#[must_use]
fn sample_mut(samples: &mut [Samples; 4], endpoint: Endpoint) -> &mut Samples {
    let [feed, post, like, create] = samples;
    match endpoint {
        Endpoint::Feed => feed,
        Endpoint::Post => post,
        Endpoint::Like => like,
        Endpoint::Create => create,
    }
}

async fn think(lo: f64, hi: f64) {
    let seconds = rand::rng().random_range(lo..hi);
    tokio::time::sleep(Duration::from_secs_f64(seconds)).await;
}

fn parse_source_ips(value: &str) -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
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
fn validate_source_ips(ips: &[IpAddr]) -> Result<(), Box<dyn std::error::Error>> {
    for ip in ips {
        // Bind only: no requests, interface changes, or dependency on the target.
        TcpListener::bind(SocketAddr::new(*ip, 0))
            .map_err(|error| format!("cannot bind SOURCE_IPS address {ip}: {error}"))?;
    }
    Ok(())
}

fn load_tokens(path: &Path) -> Result<Vec<Token>, Box<dyn std::error::Error>> {
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
fn runtime_error(
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
            record_request_failure(user, endpoint, path, error.to_string());
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
    let (status, retained, excerpt, body_complete, failure, latency_ms) = match result {
        Ok(Ok(response)) => {
            let latency_ms = response.latency_ms;
            if let Some(error) = response.failure {
                (
                    response.status,
                    None,
                    response.error_excerpt,
                    false,
                    Some(("body_read", errors::sanitize_message_urls(&error))),
                    latency_ms,
                )
            } else {
                (
                    response.status,
                    response.retained,
                    response.error_excerpt,
                    true,
                    None,
                    latency_ms,
                )
            }
        }
        Ok(Err(error)) => (
            0,
            None,
            bytes::Bytes::new(),
            false,
            Some((
                "transport",
                errors::sanitize_message_urls(&error.to_string()),
            )),
            user.client.take_request_latency_ms(),
        ),
        Err(_) => {
            user.client.discard_connection();
            (
                0,
                None,
                bytes::Bytes::new(),
                false,
                Some((
                    "transport",
                    "whole-request timeout after 60 seconds".to_owned(),
                )),
                user.client.take_request_latency_ms(),
            )
        }
    };
    let body = retained
        .as_deref()
        .or_else(|| (!excerpt.is_empty()).then_some(excerpt.as_ref()));
    record_response(
        user,
        ResponseSample {
            endpoint,
            path,
            status,
            body,
            body_complete,
            failure,
            latency_ms,
        },
    );
    retained.filter(|_| endpoint == Endpoint::Feed && endpoint.successful_status(status))
}

struct ResponseSample<'a> {
    endpoint: Endpoint,
    path: &'a str,
    status: u16,
    body: Option<&'a [u8]>,
    body_complete: bool,
    failure: Option<(&'static str, String)>,
    latency_ms: f64,
}

fn record_request_failure(user: &User, endpoint: Endpoint, path: &str, reason: String) {
    record_response(
        user,
        ResponseSample {
            endpoint,
            path,
            status: 0,
            body: None,
            body_complete: false,
            failure: Some(("request_builder", reason)),
            latency_ms: 0.0,
        },
    );
}

fn record_response(user: &User, sample: ResponseSample<'_>) {
    let ResponseSample {
        endpoint,
        path,
        status,
        body: bytes,
        body_complete,
        failure,
        latency_ms,
    } = sample;
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
                    failure.unwrap_or_else(|| {
                        if kind == "http" {
                            format!("HTTP status {status}")
                        } else {
                            format!("unexpected HTTP status {status}")
                        }
                    }),
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

async fn browse(user: &mut User, tokens: &[Token], iteration: usize) {
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
    let Some(body) = request(user, Endpoint::Feed, "/feed", None, None).await else {
        think(3.0, 7.0).await;
        return;
    };
    let post_id = serde_json::from_slice::<Feed>(&body)
        .ok()
        .as_ref()
        .and_then(choose_post_id);
    drop(body);
    let Some(post_id) = post_id else {
        {
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
        return;
    };
    think(3.0, 7.0).await;
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

#[must_use]
fn global_user_index(offset: usize, local: usize) -> Option<usize> {
    offset.checked_add(local)
}

#[must_use]
fn ramp_offset(user: usize, users: usize, ramp: Duration) -> Duration {
    if users <= 1 {
        Duration::ZERO
    } else {
        let user = user.to_f64().expect("usize is representable as f64");
        let users = (users - 1).to_f64().expect("usize is representable as f64");
        let fraction = user / users;
        ramp.mul_f64(fraction)
    }
}

fn scheduled_start_delay(start_ms: i64, now_ms: i64) -> Result<Option<Duration>, &'static str> {
    let remaining = start_ms
        .checked_sub(now_ms)
        .ok_or("BENCH_START_AT difference exceeds supported timestamp range")?;
    if remaining < -250 {
        return Err("missed coordinated start");
    }
    if remaining > 0 {
        let millis = u64::try_from(remaining)
            .map_err(|_| "BENCH_START_AT delay exceeds supported duration range")?;
        Ok(Some(Duration::from_millis(millis)))
    } else {
        Ok(None)
    }
}

async fn wait_ramp_slot(deadline: Instant) {
    if Instant::now() < deadline {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    } else {
        tokio::task::yield_now().await;
    }
}

fn prepare_descriptor_limit(users: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid writable rlimit and the selector is supported on Unix.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut limit) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let required = (users as libc::rlim_t).saturating_add(256);
    let desired = required.max(524_288);
    if limit.rlim_cur < desired {
        limit.rlim_cur = desired.min(limit.rlim_max);
        // SAFETY: the pointer is valid and the new soft limit is within the hard limit.
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raw const limit) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    if limit.rlim_cur < desired {
        eprintln!(
            "Generator descriptor soft limit capped at {}; hard limit prevents reaching {} ({} users plus overhead)",
            limit.rlim_cur, desired, users
        );
    }
    if limit.rlim_cur < required {
        return Err(format!("Generator descriptor soft limit {} is below {} users plus process overhead; increase its hard limit before starting the load", limit.rlim_cur, users).into());
    }
    Ok(())
}

fn build_client(
    base: Arc<url::Url>,
    source: Option<IpAddr>,
    connections: Arc<Mutex<connections::Connections>>,
) -> transport::Client {
    transport::Client::new(base, source, connections)
}

struct Workload {
    ramp: Duration,
    hold: Duration,
    source_ips: Arc<Vec<IpAddr>>,
    targets: Arc<Vec<Arc<url::Url>>>,
    tokens: Arc<Vec<Token>>,
    user_offset: usize,
}

struct Signals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

struct RunContext {
    cfg: config::Config,
    workload: Workload,
    metrics: Metrics,
    connections: Arc<Mutex<connections::Connections>>,
    assigned: Arc<Mutex<BTreeSet<String>>>,
    runtime_errors: Arc<AtomicUsize>,
    runtime_details: Arc<Mutex<errors::ErrorDetails>>,
    signals: Signals,
    interrupted: bool,
    sampler: Option<telemetry::Sampler>,
}

struct LiveUsers {
    tasks: JoinSet<()>,
    stops: Arc<[AtomicBool]>,
    aborts: Vec<AbortHandle>,
    launched: usize,
    expected_cancellations: usize,
    maximum: usize,
    interrupted: bool,
}

struct RunResult {
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
    duration_s: f64,
    hold_started_at: Option<chrono::DateTime<Utc>>,
    hold_finished_at: Option<chrono::DateTime<Utc>>,
    maximum_users: usize,
    interrupted: bool,
    telemetry: Result<serde_json::Value, String>,
}

fn prepare_workload(cfg: &config::Config) -> Result<Workload, Box<dyn std::error::Error>> {
    if cfg.users == 0 {
        return Err("users must be positive".into());
    }
    let ramp = config::duration(&cfg.ramp_up)?;
    let hold = config::duration(&cfg.duration)?;
    prepare_descriptor_limit(cfg.users)?;
    hyper_util::client::legacy::connect::set_source_port_range(1024, 65535)?;
    let source_ips = Arc::new(parse_source_ips(&cfg.source_ips)?);
    validate_source_ips(&source_ips)?;
    let targets = Arc::new(
        destinations::urls(&cfg.base_url, &cfg.destination_ports)?
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );
    let capacity = 64512usize
        .saturating_mul(source_ips.len().max(1))
        .saturating_mul(targets.len());
    if cfg.users > capacity {
        return Err(format!(
            "{} users exceed generator's theoretical {capacity} source/destination tuples",
            cfg.users
        )
        .into());
    }
    let tokens = Arc::new(load_tokens(&cfg.tokens)?);
    let user_offset: usize = setting("BENCH_USER_OFFSET", "0").parse()?;
    if global_user_index(user_offset, cfg.users).is_none() {
        return Err("BENCH_USER_OFFSET plus user count exceeds the supported index range".into());
    }
    Ok(Workload {
        ramp,
        hold,
        source_ips,
        targets,
        tokens,
        user_offset,
    })
}

impl RunContext {
    async fn prepare(
        cfg: config::Config,
        workload: Workload,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let metrics: Metrics = Arc::new(Mutex::new(std::array::from_fn(|_| Samples::default())));
        let connections = Arc::new(Mutex::new(connections::Connections::default()));
        let assigned = Arc::new(Mutex::new(BTreeSet::new()));
        let runtime_errors = Arc::new(AtomicUsize::new(0));
        let runtime_details = Arc::new(Mutex::new(errors::ErrorDetails::default()));
        let signals = Signals {
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        };
        let mut context = Self {
            cfg,
            workload,
            metrics,
            connections,
            assigned,
            runtime_errors,
            runtime_details,
            signals,
            interrupted: false,
            sampler: None,
        };
        context.wait_for_scheduled_start().await?;
        context.start_sampler()?;
        Ok(context)
    }

    async fn wait_for_scheduled_start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let env_start = env::var("BENCH_START_AT")
            .ok()
            .map(|s| s.parse::<i64>())
            .transpose()?;
        if let Some(start) = env_start
            && let Some(delay) = scheduled_start_delay(start, Utc::now().timestamp_millis())?
        {
            self.interrupted = tokio::select! {
                () = tokio::time::sleep(delay) => false,
                _ = self.signals.interrupt.recv() => true,
                _ = self.signals.terminate.recv() => true,
            };
        }
        Ok(())
    }

    fn start_sampler(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let path = env::var("BENCH_TELEMETRY").unwrap_or_else(|_| "telemetry.jsonl".into());
        let sample_interval = self.cfg.sample_interval;
        let telemetry_metrics = self.metrics.clone();
        let telemetry_connections = self.connections.clone();
        self.sampler = Some(telemetry::Sampler::start(
            path.into(),
            std::collections::BTreeMap::from([("generator".to_owned(), vec![std::process::id()])]),
            sample_interval,
            Arc::new(move || {
                let metrics = telemetry_metrics.lock().unwrap();
                serde_json::json!({"requests":metrics.iter().map(|s|[s.latency_ms.len(),s.failed]).collect::<Vec<_>>(),"connections":telemetry_connections.lock().unwrap().counts()})
            }),
        )?);
        Ok(())
    }
}

async fn run(args: &[String]) -> Result<bool, Box<dyn std::error::Error>> {
    let cfg = config::parse(args)?;
    if cfg.help {
        println!("{}", config::HELP);
        return Ok(true);
    }
    let workload = prepare_workload(&cfg)?;
    let mut context = RunContext::prepare(cfg, workload).await?;
    let result = execute_load(&mut context).await?;
    write_report(&context, result)
}

async fn execute_load(context: &mut RunContext) -> Result<RunResult, Box<dyn std::error::Error>> {
    let started_at = Utc::now();
    let start = Instant::now();
    let mut users = launch_users(context, start).await?;
    let hold_started_at = (!users.interrupted).then(Utc::now);
    let hold_finished_at = finish_users(context, &mut users).await?;
    let telemetry = context
        .sampler
        .take()
        .ok_or("telemetry sampler was already finished")?
        .finish()
        .map_err(|error| error.to_string());
    Ok(RunResult {
        started_at,
        finished_at: Utc::now(),
        duration_s: start.elapsed().as_secs_f64(),
        hold_started_at,
        hold_finished_at,
        maximum_users: users.maximum,
        interrupted: users.interrupted,
        telemetry,
    })
}

async fn launch_users(
    context: &mut RunContext,
    start: Instant,
) -> Result<LiveUsers, Box<dyn std::error::Error>> {
    let mut users = LiveUsers {
        tasks: JoinSet::new(),
        stops: (0..context.cfg.users)
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>()
            .into(),
        aborts: Vec::with_capacity(context.cfg.users),
        launched: 0,
        expected_cancellations: 0,
        maximum: 0,
        interrupted: context.interrupted,
    };
    for local in 0..context.cfg.users {
        if users.interrupted {
            break;
        }
        let at = start
            .checked_add(ramp_offset(local, context.cfg.users, context.workload.ramp))
            .ok_or("ramp-up deadline exceeds the system clock range")?;
        tokio::select! {
            () = wait_ramp_slot(at) => {},
            _ = context.signals.interrupt.recv() => { users.interrupted = true; break; },
            _ = context.signals.terminate.recv() => { users.interrupted = true; break; },
        }
        let Some(index) = global_user_index(context.workload.user_offset, local) else {
            users.interrupted = true;
            break;
        };
        let target_index = destinations::index(
            index,
            context.workload.source_ips.len(),
            context.workload.targets.len(),
        );
        let Some(base) = context.workload.targets.get(target_index).cloned() else {
            runtime_error(
                &context.runtime_details,
                &context.runtime_errors,
                &std::io::Error::other("destination assignment exceeded configured targets"),
                index,
                &context.cfg.base_url,
                None,
            );
            users.interrupted = true;
            break;
        };
        let source = context
            .workload
            .source_ips
            .get(index % context.workload.source_ips.len().max(1))
            .copied();
        let client = build_client(base.clone(), source, context.connections.clone());
        let mut assigned = context.assigned.lock().unwrap();
        if !assigned.contains(base.as_str()) {
            assigned.insert(base.as_str().to_owned());
        }
        drop(assigned);
        users.maximum += 1;
        users.launched += 1;
        let stops = users.stops.clone();
        let stop_index = local;
        let tokens = context.workload.tokens.clone();
        let mut user = User {
            client,
            base,
            index,
            source,
            metrics: context.metrics.clone(),
            runtime_errors: context.runtime_errors.clone(),
            runtime_details: context.runtime_details.clone(),
        };
        let abort = users.tasks.spawn(async move {
            let mut iteration = 0;
            loop {
                if iteration == 0 {
                    think(0.0, 5.0).await;
                }
                browse(&mut user, &tokens, iteration).await;
                iteration += 1;
                if stops
                    .get(stop_index)
                    .is_some_and(|stop| stop.load(Ordering::Relaxed))
                {
                    break;
                }
            }
        });
        users.aborts.push(abort);
    }
    Ok(users)
}

async fn finish_users(
    context: &mut RunContext,
    users: &mut LiveUsers,
) -> Result<Option<chrono::DateTime<Utc>>, Box<dyn std::error::Error>> {
    let deadline = Instant::now()
        .checked_add(context.workload.hold)
        .ok_or("hold deadline exceeds the system clock range")?;
    let mut hold_finished_at = None;
    if !users.interrupted {
        users.interrupted = tokio::select! {
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                hold_finished_at = Some(Utc::now()); false
            },
            _ = context.signals.interrupt.recv() => true,
            _ = context.signals.terminate.recv() => true,
        };
    }
    if users.interrupted {
        stop_all(&users.stops);
    } else {
        ramp_down_users(context, deadline, users).await?;
    }
    drain_users(context, users).await;
    Ok(hold_finished_at)
}

async fn ramp_down_users(
    context: &mut RunContext,
    hold_deadline: Instant,
    users: &mut LiveUsers,
) -> Result<(), Box<dyn std::error::Error>> {
    enum Event {
        Deadline,
        Signal,
        Joined(Option<Result<(), tokio::task::JoinError>>),
    }

    if users.launched == 0 {
        return Ok(());
    }
    let mut soft_stopped = 0;
    let mut hard_cancelled = 0;
    let mut next_soft = Some(ramp_down_deadline(hold_deadline, 1, users.launched)?);
    let mut next_hard = None;

    while hard_cancelled < users.launched && !users.tasks.is_empty() {
        let deadline = match next_hard {
            Some(hard) if hard < next_soft.unwrap_or(hard) => hard,
            _ => next_soft
                .or(next_hard)
                .expect("ramp-down has pending events"),
        };
        let event = tokio::select! {
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => Event::Deadline,
            _ = context.signals.interrupt.recv() => Event::Signal,
            _ = context.signals.terminate.recv() => Event::Signal,
            result = users.tasks.join_next() => Event::Joined(result),
        };
        match event {
            Event::Signal => {
                users.interrupted = true;
                stop_all(&users.stops);
                break;
            }
            Event::Joined(result) => {
                if handle_user_result(context, users, result) {
                    break;
                }
                continue;
            }
            Event::Deadline => {}
        }

        let now = Instant::now();
        if next_hard.is_some_and(|hard| hard <= now) {
            if let Some(abort) = users.aborts.get(users.launched - hard_cancelled - 1) {
                abort.abort();
                users.expected_cancellations += 1;
            }
            hard_cancelled += 1;
            next_hard = if hard_cancelled < soft_stopped {
                Some(graceful_cancel_deadline(ramp_down_deadline(
                    hold_deadline,
                    hard_cancelled + 1,
                    users.launched,
                )?)?)
            } else {
                None
            };
        }
        if next_soft.is_some_and(|soft| soft <= now) {
            stop_one(&users.stops, users.launched - soft_stopped - 1);
            soft_stopped += 1;
            let hard_deadline =
                graceful_cancel_deadline(next_soft.expect("soft deadline was checked"))?;
            next_hard = Some(next_hard.map_or(hard_deadline, |current| current.min(hard_deadline)));
            next_soft = if soft_stopped < users.launched {
                Some(ramp_down_deadline(
                    hold_deadline,
                    soft_stopped + 1,
                    users.launched,
                )?)
            } else {
                None
            };
        }
    }
    Ok(())
}

fn handle_user_result(
    context: &RunContext,
    users: &mut LiveUsers,
    result: Option<Result<(), tokio::task::JoinError>>,
) -> bool {
    let Some(Err(error)) = result else {
        return false;
    };
    if error.is_cancelled() && users.expected_cancellations > 0 {
        users.expected_cancellations -= 1;
        return false;
    }
    users.interrupted = true;
    stop_all(&users.stops);
    runtime_error(
        &context.runtime_details,
        &context.runtime_errors,
        &error,
        context.workload.user_offset,
        &context.cfg.base_url,
        None,
    );
    true
}

fn graceful_cancel_deadline(soft_deadline: Instant) -> Result<Instant, Box<dyn std::error::Error>> {
    soft_deadline
        .checked_add(Duration::from_secs(10))
        .ok_or_else(|| "graceful ramp-down deadline exceeds the system clock range".into())
}

fn ramp_down_deadline(
    hold_deadline: Instant,
    step: usize,
    users: usize,
) -> Result<Instant, Box<dyn std::error::Error>> {
    let offset = ramp_offset(step, users + 1, Duration::from_secs(30));
    hold_deadline
        .checked_add(offset)
        .ok_or_else(|| "ramp-down deadline exceeds the system clock range".into())
}

fn stop_one(stops: &[AtomicBool], index: usize) {
    if let Some(stop) = stops.get(index) {
        stop.store(true, Ordering::Relaxed);
    }
}

fn stop_all(stops: &[AtomicBool]) {
    for stop in stops {
        stop.store(true, Ordering::Relaxed);
    }
}

async fn drain_users(context: &mut RunContext, users: &mut LiveUsers) {
    while !users.tasks.is_empty() {
        let result = tokio::select! {
            result = users.tasks.join_next() => result,
            _ = context.signals.interrupt.recv(), if !users.interrupted => { users.interrupted = true; stop_all(&users.stops); None },
            _ = context.signals.terminate.recv(), if !users.interrupted => { users.interrupted = true; stop_all(&users.stops); None },
        };
        handle_user_result(context, users, result);
    }
}

fn summary_path(context: &RunContext) -> std::path::PathBuf {
    env::var("SUMMARY")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| {
            context
                .cfg
                .output
                .as_ref()
                .map(|dir| dir.join("summary.json"))
        })
        .unwrap_or_else(|| std::path::PathBuf::from("results/summary.json"))
}

fn write_sample_artifacts(
    samples: &[Samples; 4],
    connections: &connections::Connections,
    samples_path: &Path,
    connections_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = BufWriter::new(fs::File::create(samples_path)?);
    serde_json::to_writer(&mut file, samples)?;
    file.flush()?;
    let mut file = BufWriter::new(fs::File::create(connections_path)?);
    serde_json::to_writer(&mut file, connections)?;
    file.flush()?;
    Ok(())
}

fn write_report(
    context: &RunContext,
    result: RunResult,
) -> Result<bool, Box<dyn std::error::Error>> {
    let path = summary_path(context);
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    if let Ok(destination) = env::var("BENCH_SAMPLES") {
        let cp = summary_path(context)
            .parent()
            .unwrap_or(Path::new("."))
            .join("connection-samples.json");
        write_sample_artifacts(
            &context.metrics.lock().unwrap(),
            &context.connections.lock().unwrap(),
            Path::new(&destination),
            &cp,
        )?;
    }
    let mut collected = context.metrics.lock().unwrap();
    let mut endpoints = serde_json::Map::new();
    let elapsed = Some(result.duration_s);
    for ((endpoint, name), samples) in Endpoint::ALL
        .into_iter()
        .zip(ENDPOINTS)
        .zip(collected.iter_mut())
    {
        debug_assert_eq!(endpoint.name(), name);
        let endpoint_summary = samples.summary();
        let rates = connections::request_rates(samples, elapsed);
        let mut summary = serde_json::to_value(endpoint_summary)?;
        if let (Some(summary), serde_json::Value::Object(rates)) = (summary.as_object_mut(), rates)
        {
            summary.extend(rates);
        }
        endpoints.insert(name.into(), summary);
    }
    let aggregate = aggregate_samples(&mut collected[..]);
    let (report, passed) = build_report(context, result, elapsed, aggregate, &endpoints);
    save_report(context, &report, passed)
}

fn build_report(
    context: &RunContext,
    result: RunResult,
    elapsed: Option<f64>,
    mut aggregate: Samples,
    endpoints: &serde_json::Map<String, serde_json::Value>,
) -> (serde_json::Value, bool) {
    let summary = aggregate.summary();
    let runtime_count = context.runtime_errors.load(Ordering::Relaxed);
    let completed = !result.interrupted;
    let passed = completed && passes(&summary, runtime_count);
    let connection_report =
        std::mem::take(&mut *context.connections.lock().unwrap()).report(elapsed);
    let report = serde_json::json!({
        "generator": "hyper-http1",
        "requested_users": context.cfg.users,
        "source_port_allocation": {
            "first": 1024,
            "last": 65535,
            "policy": "explicit per source/destination counters",
        },
        "maximum_users": result.maximum_users,
        "total_users": result.maximum_users,
        "duration_s": elapsed,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "hold_started_at": result.hold_started_at,
        "hold_finished_at": result.hold_finished_at,
        "planned_hold_s": context.workload.hold.as_secs_f64(),
        "hosts": &*context.assigned.lock().unwrap(),
        "configured_hosts": context.workload.targets.iter().map(|url| url.as_str()).collect::<Vec<_>>(),
        "destination_ports": context.workload.targets.iter().filter_map(|url| url.port_or_known_default()).collect::<Vec<_>>(),
        "assignment": "fixed per user, balanced over source IP and destination port combinations; SSH workers use global user offsets",
        "source_ips": context.workload.source_ips.as_ref(),
        "latency": "HTTP send through complete response body, excluding DNS and TCP connection setup",
        "percentiles": "k6 linear interpolation over unrounded milliseconds",
        "scope": "whole run including ramp-up, hold, ramp-down and drain",
        "coordinated_omission": "disabled for scoring; real requests only",
        "thresholds": {"p95_ms_lt": 500, "p99_ms_lt": 1000, "failure_rate_lt": 0.01},
        "completed": completed,
        "runtime_errors": runtime_count,
        "runtime_error_details": &*context.runtime_details.lock().unwrap(),
        "passed": passed,
        "aggregate": summary,
        "request_rates": connections::request_rates(&aggregate, elapsed),
        "connections": connection_report,
        "telemetry": match result.telemetry {
            Ok(value) => value,
            Err(error) => serde_json::json!({"unavailable_reason": error}),
        },
        "endpoints": endpoints,
    });
    (report, passed)
}

fn save_report(
    context: &RunContext,
    report: &serde_json::Value,
    passed: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    let output = serde_json::to_string_pretty(report)?;
    println!("\nScoring summary (full-body latency):\n{output}");
    let path = summary_path(context);
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, format!("{output}\n"))?;
    println!(
        "{} — summary: {}",
        if passed { "PASS" } else { "FAIL" },
        path.display()
    );
    Ok(passed)
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.as_slice() == ["--version"] || args.as_slice() == ["-V"] {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    if args.first().is_some_and(|a| {
        [
            "--supervise",
            "--stop-supervised",
            "--ready-supervised",
            "--start-supervised",
        ]
        .contains(&a.as_str())
    }) {
        let result = if let Some(p) = args.get(1) {
            match args.first().map(String::as_str) {
                Some("--supervise") => {
                    runner::supervise(Path::new(p), args.get(2..).unwrap_or_default())
                }
                Some("--stop-supervised") => control::stop_supervised(Path::new(p)),
                Some("--ready-supervised") => control::ready_supervised(Path::new(p)),
                Some("--start-supervised") => args.get(2).map_or_else(
                    || Err("missing coordinated start timestamp".into()),
                    |t| control::start_supervised(Path::new(p), t),
                ),
                _ => Err("unknown supervisor command".into()),
            }
        } else {
            Err("missing supervisor control path".into())
        };
        return match result {
            Ok(code) => ExitCode::from(code),
            Err(e) => {
                eprintln!("benchmark supervisor error: {e}");
                ExitCode::from(2)
            }
        };
    }
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("benchmark runtime error: {e}");
            return ExitCode::from(2);
        }
    };
    rt.block_on(async {
        if args.first().is_some_and(|a| a == "--orchestrate") {
            let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let flag = cancelled.clone();
            let signals = (
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()),
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()),
            );
            let (mut interrupt, mut terminate) = match signals {
                (Ok(interrupt), Ok(terminate)) => (interrupt, terminate),
                (Err(error), _) | (_, Err(error)) => {
                    eprintln!("benchmark signal handler error: {error}");
                    return ExitCode::from(2);
                }
            };
            tokio::spawn(async move {
                tokio::select! {
                    _ = interrupt.recv() => {},
                    _ = terminate.recv() => {},
                }
                flag.store(true, Ordering::Relaxed);
            });
            return match runner::run(args.get(1..).unwrap_or_default(), &cancelled) {
                Ok(c) => ExitCode::from(c),
                Err(e) => {
                    eprintln!("benchmark runner error: {e}");
                    ExitCode::from(2)
                }
            };
        }
        match run(&args).await {
            Ok(true) => ExitCode::SUCCESS,
            Ok(false) => ExitCode::from(1),
            Err(e) => {
                eprintln!("load test error: {e}");
                ExitCode::from(2)
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, Read, Write};

    fn user(base: &str, metrics: Metrics) -> User {
        let base = Arc::new(url::Url::parse(base).unwrap());
        User {
            client: build_client(
                base.clone(),
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
    fn thresholds_are_strict_empty_runs_fail_and_runtime_errors_fail() {
        let mut s = Samples::default();
        assert!(!passes(&s.summary(), 0));
        s.latency_ms = vec![10.0; 100];
        s.failed = 1;
        assert!(!passes(&s.summary(), 0)); // exactly one percent fails
        s.latency_ms.push(10.0);
        assert!(passes(&s.summary(), 0));
        s.failed = 0;
        let mut summary = s.summary();
        summary.p95_ms = Some(500.0);
        assert!(!passes(&summary, 0));
        summary.p95_ms = Some(499.0);
        summary.p99_ms = Some(1000.0);
        assert!(!passes(&summary, 0));
        summary.p99_ms = Some(999.0);
        assert!(!passes(&summary, 1));
        assert!(passes(&summary, 0));
    }

    #[test]
    fn aggregate_reuses_largest_endpoint_buffer_after_raw_samples_are_serialized() {
        let mut samples: [Samples; 4] = std::array::from_fn(|_| Samples::default());
        samples[2].latency_ms = vec![9.0, 1.0, 5.0];
        let raw = serde_json::to_value(&samples).unwrap();
        let original_buffer = samples[2].latency_ms.as_ptr();

        let mut aggregate = aggregate_samples(&mut samples);
        let summary = aggregate.summary();

        assert_eq!(raw[2]["latency_ms"], serde_json::json!([9.0, 1.0, 5.0]));
        assert_eq!(aggregate.latency_ms.as_ptr(), original_buffer);
        assert_eq!(summary.requests, 3);
        assert_eq!(summary.med_ms, Some(5.0));
        assert!(samples.iter().all(|sample| sample.latency_ms.is_empty()));
    }

    #[test]
    fn raw_sample_artifacts_survive_aggregation_and_connection_reporting() {
        let directory = env::temp_dir().join(format!("benchmark-samples-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let samples_path = directory.join("samples.json");
        let connections_path = directory.join("connection-samples.json");
        let mut samples: [Samples; 4] = std::array::from_fn(|_| Samples::default());
        samples[0].latency_ms = vec![3.0, 1.0];
        let connections = connections::Connections {
            successful_ms: vec![2.0],
            failed_ms: vec![4.0],
            ..Default::default()
        };

        write_sample_artifacts(&samples, &connections, &samples_path, &connections_path).unwrap();
        let mut aggregate = aggregate_samples(&mut samples);
        let _ = aggregate.summary();
        let report = connections.report(Some(1.0));

        let raw: [Samples; 4] =
            serde_json::from_reader(fs::File::open(&samples_path).unwrap()).unwrap();
        let raw_connections: connections::Connections =
            serde_json::from_reader(fs::File::open(&connections_path).unwrap()).unwrap();
        assert_eq!(raw[0].latency_ms, [3.0, 1.0]);
        assert_eq!(raw_connections.successful_ms, [2.0]);
        assert_eq!(raw_connections.failed_ms, [4.0]);
        assert_eq!(report["setup_duration_ms"]["samples"], 2);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn percentile_and_http_check_semantics_match_benchmark() {
        let mut samples = Samples {
            latency_ms: vec![1000.1, 499.9, 0.1, 500.1],
            ..Default::default()
        };
        let summary = samples.summary();
        assert_eq!(summary.med_ms, Some(500.0));
        assert!((summary.p95_ms.unwrap() - 925.1).abs() < 1e-9);
        assert!((summary.p99_ms.unwrap() - 985.1).abs() < 1e-9);
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
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut input = Vec::new();
            let mut buf = [0; 1024];
            while !input.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = stream.read(&mut buf).unwrap();
                assert_ne!(n, 0);
                input.extend_from_slice(&buf[..n]);
            }
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
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut stream = std::io::BufReader::new(stream);
            let mut line = String::new();
            stream.read_line(&mut line).unwrap();
            assert!(line.starts_with("POST /posts "));
            loop {
                line.clear();
                stream.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
            }
            let body = vec![b'x'; 4096];
            let stream = stream.get_mut();
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

            let mut stream = std::io::BufReader::new(stream);
            line.clear();
            stream.read_line(&mut line).unwrap();
            assert!(line.starts_with("GET /feed "));
            loop {
                line.clear();
                stream.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
            }
            let body = br#"{"posts":[{"id":1}]}"#;
            let stream = stream.get_mut();
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
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 512];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut buffer).unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&buffer[..count]);
            }
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
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 512];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut buffer).unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&buffer[..count]);
            }
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
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 512];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut buffer).unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&buffer[..count]);
            }
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
                let mut req = Vec::new();
                let mut b = [0; 512];
                while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = stream.read(&mut b).unwrap();
                    req.extend_from_slice(&b[..n]);
                }
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
            let mut client = build_client(
                Arc::new(url::Url::parse(&format!("http://{address}")).unwrap()),
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

    #[test]
    fn scheduled_start_rejects_overflow_and_missed_deadlines() {
        assert!(scheduled_start_delay(i64::MIN, 0).is_err());
        assert!(scheduled_start_delay(749, 1_000).is_err());
        assert_eq!(
            scheduled_start_delay(1_001, 1_000).unwrap(),
            Some(Duration::from_millis(1))
        );
        assert_eq!(scheduled_start_delay(1_000, 1_000).unwrap(), None);
    }

    #[test]
    fn stop_flags_use_local_indices_for_a_launched_prefix() {
        let flags: Arc<[AtomicBool]> = (0..5)
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>()
            .into();
        let launched = 3;
        let local_index = 2;
        let global_index = global_user_index(700, local_index).unwrap();
        stop_one(&flags, local_index);

        assert_eq!(launched, 3);
        assert_eq!(global_index, 702);
        assert!(flags[local_index].load(Ordering::Relaxed));
        assert!(!flags[3].load(Ordering::Relaxed));
    }

    #[test]
    fn stop_all_sets_every_flag() {
        let flags: Arc<[AtomicBool]> = (0..4)
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>()
            .into();

        stop_all(&flags);

        assert!(flags.iter().all(|flag| flag.load(Ordering::Relaxed)));
    }

    #[test]
    fn three_users_ramp_down_highest_local_user_first() {
        let hold_end = Instant::now();
        for (step, local_index, seconds) in [(1, 2, 10), (2, 1, 20), (3, 0, 30)] {
            let deadline = ramp_down_deadline(hold_end, step, 3).unwrap();
            assert_eq!(
                deadline.duration_since(hold_end),
                Duration::from_secs(seconds)
            );
            assert_eq!(3 - step, local_index);
        }
    }

    #[tokio::test]
    async fn grace_cancel_drops_active_sample_and_keeps_completed_samples() {
        let samples = Arc::new(Mutex::new(Vec::new()));
        let soft_stop = Arc::new(AtomicBool::new(false));
        let task_samples = samples.clone();
        let task_stop = soft_stop.clone();
        let mut tasks = JoinSet::new();
        let abort = tasks.spawn(async move {
            task_samples.lock().unwrap().push(1);
            if !task_stop.load(Ordering::Relaxed) {
                let response = std::future::pending::<Option<usize>>().await;
                if let Some(sample) = response {
                    task_samples.lock().unwrap().push(sample);
                }
            }
        });

        tokio::task::yield_now().await;
        assert_eq!(*samples.lock().unwrap(), [1]);
        soft_stop.store(true, Ordering::Relaxed);
        abort.abort();
        let result = tasks.join_next().await.unwrap();

        assert!(result.unwrap_err().is_cancelled());
        assert_eq!(*samples.lock().unwrap(), [1]);
    }

    #[tokio::test]
    async fn ramp_scheduler_spreads_thousands_of_launch_slots_over_the_interval() {
        let ramp = Duration::from_secs(1);
        assert_eq!(ramp_offset(0, 5000, ramp), Duration::ZERO);
        assert_eq!(ramp_offset(4999, 5000, ramp), ramp);
        assert!(ramp_offset(2500, 5000, ramp) > Duration::from_millis(499));
        assert_eq!(ramp_offset(0, usize::MAX, Duration::ZERO), Duration::ZERO);
        assert_eq!(global_user_index(usize::MAX, 0), Some(usize::MAX));
        assert_eq!(global_user_index(usize::MAX, 1), None);
        let start = Instant::now();
        for user in 0..5000 {
            wait_ramp_slot(start + ramp_offset(user, 5000, Duration::from_millis(50))).await;
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(45),
            "ramp elapsed only {elapsed:?}"
        );
        assert!(elapsed < Duration::from_secs(1), "ramp took {elapsed:?}");
    }
}
