use chrono::{SecondsFormat, Utc};
use goose::{
    metrics::{GooseCoordinatedOmissionMitigation, GooseMetrics},
    prelude::*,
    test_plan::TestPlanStepAction,
};
use gumdrop::Options;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    io::{BufWriter, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    path::Path,
    process::ExitCode,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

mod config;
mod connections;
mod destinations;
mod errors;
mod runner;
mod telemetry;

const ENDPOINTS: [&str; 4] = [
    "GET /feed",
    "GET /posts/:id",
    "POST /posts/:id/like",
    "POST /posts",
];

#[derive(Deserialize)]
struct Token {
    username: String,
    token: String,
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
    // ponytail: retain exact samples, like k6; about 8 MB per million requests.
    // Use a histogram if longer runs make sample storage a bottleneck.
    latency_ms: Vec<f64>,
    failed: usize,
    checks_failed: usize,
    #[serde(default)]
    errors: errors::ErrorDetails,
}

type Metrics = Arc<Mutex<[Samples; 4]>>;

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
        let requests = self.latency_ms.len();
        Summary {
            requests,
            failed: self.failed,
            failure_rate: (requests > 0).then(|| self.failed as f64 / requests as f64),
            checks_failed: self.checks_failed,
            avg_ms: (requests > 0).then(|| self.latency_ms.iter().sum::<f64>() / requests as f64),
            med_ms: percentile(&self.latency_ms, 0.5),
            p95_ms: percentile(&self.latency_ms, 0.95),
            p99_ms: percentile(&self.latency_ms, 0.99),
            max_ms: self.latency_ms.last().copied(),
            errors: self.errors.clone(),
        }
    }
}

// k6 TrendSink.P: linear interpolation at p * (n - 1), not Goose's rounded rank.
fn percentile(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = p * (sorted.len() - 1) as f64;
    let lower = sorted[rank.floor() as usize];
    let upper = sorted[rank.ceil() as usize];
    Some(lower + (upper - lower) * rank.fract())
}

fn passes(summary: &Summary, runtime_errors: usize) -> bool {
    runtime_errors == 0
        && summary.p95_ms.is_some_and(|ms| ms < 500.0)
        && summary.p99_ms.is_some_and(|ms| ms < 1000.0)
        && summary.failure_rate.is_some_and(|rate| rate < 0.01)
}

// Goose ignores returned transaction errors, including request-builder errors.
// Track both startup and browsing errors independently of HTTP samples.
fn track_errors(
    function: TransactionFunction,
    runtime_errors: Arc<AtomicUsize>,
    runtime_error_details: Arc<Mutex<errors::ErrorDetails>>,
    user_offset: usize,
    source_ips: Arc<Vec<IpAddr>>,
) -> TransactionFunction {
    Arc::new(move |user| {
        let function = function.clone();
        let runtime_errors = runtime_errors.clone();
        let runtime_error_details = runtime_error_details.clone();
        let source_ips = source_ips.clone();
        Box::pin(async move {
            let result = function(user).await;
            if let Err(error) = &result {
                runtime_errors.fetch_add(1, Ordering::Relaxed);
                runtime_error_details
                    .lock()
                    .unwrap()
                    .record(errors::example(
                        "transaction",
                        "",
                        None,
                        "runtime",
                        errors::error_chain(error),
                        None,
                        user_offset + user.weighted_users_index,
                        user.base_url.as_str(),
                        source_ips
                            .get(user.weighted_users_index % source_ips.len().max(1))
                            .map(ToString::to_string),
                    ));
            }
            result
        })
    })
}

fn run_duration_s(metrics: &GooseMetrics) -> Option<f64> {
    // Goose's duration counter stops before it waits for active users to finish.
    let first = metrics.history.first()?;
    let last = metrics.history.last()?;
    (last.timestamp - first.timestamp)
        .to_std()
        .ok()
        .map(|duration| duration.as_secs_f64())
}

fn run_completed(metrics: &GooseMetrics, interrupted: bool) -> bool {
    !interrupted
        && metrics
            .history
            .last()
            .is_some_and(|step| matches!(step.action, TestPlanStepAction::Finished))
        && !metrics
            .history
            .iter()
            .any(|step| matches!(step.action, TestPlanStepAction::Canceling))
}

fn http_success(status: u16) -> bool {
    // k6's default expected-response range; endpoint checks are separate.
    (200..400).contains(&status)
}

fn request_failure_kind(error: &TransactionError) -> &'static str {
    match error {
        TransactionError::Reqwest(error) if error.is_builder() => "request_builder",
        _ => "transport",
    }
}

fn check_success(endpoint: usize, status: u16) -> bool {
    match endpoint {
        0 | 1 => status == 200,
        2 => status == 200 || status == 201,
        3 => status == 201,
        _ => unreachable!(),
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
            let prefix = format!("{prefix}.0").parse::<Ipv4Addr>()?.octets();
            for last_octet in first..=last {
                ips.push(IpAddr::V4(Ipv4Addr::new(
                    prefix[0], prefix[1], prefix[2], last_octet,
                )));
            }
        } else {
            ips.push(entry.parse::<IpAddr>()?);
        }
    }
    for ip in &ips {
        if ip.is_unspecified()
            || ip.is_multicast()
            || matches!(ip, IpAddr::V4(ip) if ip.is_broadcast())
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

fn build_client(
    user_index: usize,
    config: &goose::config::GooseConfiguration,
    source_ips: &[IpAddr],
    connections: Arc<Mutex<connections::Connections>>,
) -> Result<reqwest::Client, reqwest::Error> {
    let timeout = config
        .timeout
        .as_deref()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
        .unwrap_or(60.0);
    reqwest::Client::builder()
        .no_proxy()
        .http1_only()
        .pool_idle_timeout(None)
        .pool_max_idle_per_host(1)
        .connector_layer(connections::ConnectLayer(connections))
        .local_address((!source_ips.is_empty()).then(|| source_ips[user_index % source_ips.len()]))
        .gzip(!config.no_gzip)
        .timeout(Duration::from_secs_f64(timeout))
        .build()
}

// Read every body before stopping the score timer, including errors and writes.
// Goose's native timer stops at headers and remains useful for diagnostics only.
#[allow(clippy::too_many_arguments)] // Keep request inputs visible at the call sites.
async fn request(
    user: &mut GooseUser,
    metrics: &Metrics,
    endpoint: usize,
    path: &str,
    auth: Option<&Token>,
    body: Option<&serde_json::Value>,
    user_offset: usize,
    source_ips: &[IpAddr],
) -> Result<Option<Vec<u8>>, Box<TransactionError>> {
    let method = if endpoint < 2 {
        GooseMethod::Get
    } else {
        GooseMethod::Post
    };
    let mut builder = match user.get_request_builder(&method, path) {
        Ok(builder) => builder,
        Err(error) => {
            metrics.lock().unwrap()[endpoint]
                .errors
                .record(errors::example(
                    ENDPOINTS[endpoint],
                    path,
                    None,
                    "request_builder",
                    errors::error_chain(&error),
                    None,
                    user_offset + user.weighted_users_index,
                    user.base_url.as_str(),
                    source_ips
                        .get(user.weighted_users_index % source_ips.len().max(1))
                        .map(ToString::to_string),
                ));
            return Err(error);
        }
    };
    if let Some(token) = auth {
        builder = builder
            .bearer_auth(&token.token)
            .header("Content-Type", "application/json");
    }
    if let Some(body) = body {
        builder = builder.json(body);
    }
    let goose_request = GooseRequest::builder()
        .method(method)
        .path(path)
        .name(ENDPOINTS[endpoint].split_once(' ').unwrap().1)
        .set_request_builder(builder)
        .build();
    let started = Instant::now();
    let mut response = match user.request(goose_request).await {
        Ok(response) => response,
        Err(error) => {
            let kind = request_failure_kind(&error);
            metrics.lock().unwrap()[endpoint]
                .errors
                .record(errors::example(
                    ENDPOINTS[endpoint],
                    path,
                    None,
                    kind,
                    errors::error_chain(&error),
                    None,
                    user_offset + user.weighted_users_index,
                    user.base_url.as_str(),
                    source_ips
                        .get(user.weighted_users_index % source_ips.len().max(1))
                        .map(ToString::to_string),
                ));
            return Err(error);
        }
    };
    let status = response.request.status_code;
    let mut captured_latency_ms = None;
    let bytes = match response.response {
        Ok(response) => match response.bytes().await {
            Ok(bytes) => Some(bytes.to_vec()),
            Err(error) => {
                let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
                captured_latency_ms = Some(latency_ms);
                metrics.lock().unwrap()[endpoint].errors.record(
                    errors::example(
                        ENDPOINTS[endpoint],
                        path,
                        Some(status),
                        "body_read",
                        errors::error_chain(&error.without_url()),
                        None,
                        user_offset + user.weighted_users_index,
                        user.base_url.as_str(),
                        source_ips
                            .get(user.weighted_users_index % source_ips.len().max(1))
                            .map(ToString::to_string),
                    )
                    .with_latency(latency_ms),
                );
                None
            }
        },
        Err(error) => {
            let kind = if error.is_builder() {
                "request_builder"
            } else {
                "transport"
            };
            let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
            captured_latency_ms = Some(latency_ms);
            metrics.lock().unwrap()[endpoint].errors.record(
                errors::example(
                    ENDPOINTS[endpoint],
                    path,
                    None,
                    kind,
                    errors::error_chain(&error.without_url()),
                    None,
                    user_offset + user.weighted_users_index,
                    user.base_url.as_str(),
                    source_ips
                        .get(user.weighted_users_index % source_ips.len().max(1))
                        .map(ToString::to_string),
                )
                .with_latency(latency_ms),
            );
            None
        }
    };
    let latency_ms =
        captured_latency_ms.unwrap_or_else(|| started.elapsed().as_secs_f64() * 1000.0);
    let success = http_success(status) && bytes.is_some();
    {
        let mut metrics = metrics.lock().unwrap();
        let samples = &mut metrics[endpoint];
        samples.latency_ms.push(latency_ms);
        samples.failed += usize::from(!success);
        samples.checks_failed += usize::from(!check_success(endpoint, status) || bytes.is_none());
        if !http_success(status) && bytes.is_some() {
            samples.errors.record(
                errors::example(
                    ENDPOINTS[endpoint],
                    path,
                    Some(status),
                    "http",
                    format!("HTTP status {status}"),
                    bytes.as_deref(),
                    user_offset + user.weighted_users_index,
                    user.base_url.as_str(),
                    source_ips
                        .get(user.weighted_users_index % source_ips.len().max(1))
                        .map(ToString::to_string),
                )
                .with_latency(latency_ms),
            );
        } else if bytes.is_some() && http_success(status) && !check_success(endpoint, status) {
            samples.errors.record(
                errors::example(
                    ENDPOINTS[endpoint],
                    path,
                    Some(status),
                    "check",
                    format!("unexpected HTTP status {status}"),
                    None,
                    user_offset + user.weighted_users_index,
                    user.base_url.as_str(),
                    source_ips
                        .get(user.weighted_users_index % source_ips.len().max(1))
                        .map(ToString::to_string),
                )
                .with_latency(latency_ms),
            );
        }
    }
    if !user.config.no_metrics && success {
        user.set_success(&mut response.request)?;
    } else if !user.config.no_metrics {
        // set_failure returns a transaction error. Keep browsing after failed
        // post/like/create requests, as the original k6 loop does.
        let _ = user.set_failure(
            "HTTP or response body failure",
            &mut response.request,
            None,
            None,
        );
    }
    Ok(bytes.filter(|_| check_success(endpoint, status)))
}

async fn browse(
    user: &mut GooseUser,
    tokens: &[Token],
    metrics: &Metrics,
    user_offset: usize,
    source_ips: &[IpAddr],
) -> TransactionResult {
    let user_index = user_offset + user.weighted_users_index;
    let token = &tokens[user_index % tokens.len()];
    let feed = request(
        user,
        metrics,
        0,
        "/feed",
        None,
        None,
        user_offset,
        source_ips,
    )
    .await?;
    let Some(feed) = feed else {
        think(3.0, 7.0).await;
        return Ok(());
    };
    let post_id = serde_json::from_slice::<Feed>(&feed)
        .ok()
        .filter(|feed| !feed.posts.is_empty() && feed.posts.iter().all(|post| post.id > 0))
        .map(|feed| feed.posts[rand::rng().random_range(0..feed.posts.len())].id);
    // A sleeping VU only needs the ID, not its complete feed response.
    drop(feed);
    let Some(post_id) = post_id else {
        // A malformed/empty feed cannot produce a post request. Surface this as
        // a check failure without changing k6's HTTP failure definition.
        {
            let mut metrics = metrics.lock().unwrap();
            metrics[0].checks_failed += 1;
            metrics[0].errors.record(errors::example(
                ENDPOINTS[0],
                "/feed",
                Some(200),
                "check",
                "feed body was empty or malformed",
                None,
                user_index,
                user.base_url.as_str(),
                source_ips
                    .get(user.weighted_users_index % source_ips.len().max(1))
                    .map(ToString::to_string),
            ));
        }
        think(3.0, 7.0).await;
        return Ok(());
    };
    think(3.0, 7.0).await;
    request(
        user,
        metrics,
        1,
        &format!("/posts/{post_id}"),
        None,
        None,
        user_offset,
        source_ips,
    )
    .await?;
    think(3.0, 8.0).await;
    if rand::rng().random_bool(0.15) {
        request(
            user,
            metrics,
            2,
            &format!("/posts/{post_id}/like"),
            Some(token),
            None,
            user_offset,
            source_ips,
        )
        .await?;
    }
    if rand::rng().random_bool(0.02) {
        let body = serde_json::json!({ "body": format!(
            "{} says hi at {} (VU {}, iter {})",
            token.username,
            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            user_index + 1,
            user.get_iterations(),
        ) });
        request(
            user,
            metrics,
            3,
            "/posts",
            Some(token),
            Some(&body),
            user_offset,
            source_ips,
        )
        .await?;
    }
    think(5.0, 15.0).await;
    Ok(())
}

fn load_tokens(path: &Path) -> Result<Vec<Token>, Box<dyn std::error::Error>> {
    let tokens: Vec<Token> = serde_json::from_slice(&fs::read(path)?)?;
    if tokens.is_empty()
        || tokens
            .iter()
            .any(|t| t.username.is_empty() || t.token.is_empty())
    {
        return Err("TOKENS must contain a non-empty array of usernames and tokens".into());
    }
    for token in &tokens {
        reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token.token))
            .map_err(|_| "TOKENS contains an invalid bearer header value")?;
    }
    Ok(tokens)
}

fn setting(name: &str, default: &str) -> String {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn test_plan(vus: usize, ramp_up: &str, duration: &str) -> Result<String, GooseError> {
    for (name, value) in [("RAMP_UP", ramp_up), ("DURATION", duration)] {
        if value.trim().is_empty() || value.contains([',', ';']) {
            return Err(GooseError::InvalidOption {
                option: name.into(),
                value: value.into(),
                detail: "duration must be a single timespan containing a number".into(),
            });
        }
    }
    let plan = format!("{vus},{ramp_up};{vus},{duration};0,30s");
    // Validate before set_default, which unwraps an invalid plan in Goose 0.18.1.
    let parsed: goose::test_plan::TestPlan = plan.parse()?;
    if parsed.steps[1].1 == 0 {
        // A zero-length k6 stage is skipped; a zero Goose hold never expires.
        return Ok(format!("{vus},{ramp_up};0,30s"));
    }
    Ok(plan)
}

fn prepare_descriptor_limit(users: usize) -> Result<(), Box<dyn std::error::Error>> {
    // Change only this generator's soft limit, within the existing hard limit.
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: limit is a valid writable rlimit and the selector is supported on Unix.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let required = (users as libc::rlim_t).saturating_add(256);
    let desired = required.max(524_288);
    if limit.rlim_cur < desired {
        limit.rlim_cur = desired.min(limit.rlim_max);
        // SAFETY: the pointer is valid and the new soft limit is within the hard limit.
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
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

fn native_metrics_requested(configuration: &goose::config::GooseConfiguration) -> bool {
    !configuration.request_log.is_empty()
        || !configuration.transaction_log.is_empty()
        || !configuration.scenario_log.is_empty()
        || !configuration.error_log.is_empty()
        || !configuration.debug_log.is_empty()
        || !configuration.report_file.is_empty()
        || configuration.running_metrics.is_some()
        || configuration
            .co_mitigation
            .as_ref()
            .is_some_and(|mode| *mode != GooseCoordinatedOmissionMitigation::Disabled)
}

async fn run() -> Result<bool, Box<dyn std::error::Error>> {
    // Parse Goose's CLI first so --help works without seed data.
    let configuration = goose::config::GooseConfiguration::parse_args_default_or_exit();
    if configuration.accept_invalid_certs {
        return Err(
            "--accept-invalid-certs is unavailable: this benchmark uses HTTP without TLS".into(),
        );
    }
    let native_metrics = native_metrics_requested(&configuration);
    let host = if configuration.host.is_empty() {
        setting("BASE_URL", "http://127.0.0.1:3000")
    } else {
        configuration.host.clone()
    };
    let port_setting = setting("DESTINATION_PORTS", "");
    let destination_ports = if port_setting.is_empty() {
        Vec::new()
    } else {
        config::parse_destination_ports(&port_setting)?
    };
    let targets = Arc::new(destinations::urls(&host, &destination_ports)?);
    let assigned_hosts = Arc::new(Mutex::new(std::collections::BTreeSet::new()));
    let attack = GooseAttack::initialize_with_config(configuration)?;
    let vus: usize = setting("VUS", "100").parse()?;
    if vus == 0 {
        return Err("VUS must be positive".into());
    }
    prepare_descriptor_limit(vus)?;
    let plan = test_plan(vus, &setting("RAMP_UP", "60s"), &setting("DURATION", "5m"))?;
    let stages = plan.parse::<goose::test_plan::TestPlan>()?;
    let planned_hold_s = if stages.steps.len() == 3 {
        stages.steps[1].1 / 1000
    } else {
        0
    };
    let source_ips = Arc::new(parse_source_ips(&setting("SOURCE_IPS", ""))?);
    validate_source_ips(&source_ips)?;
    let tokens = Arc::new(load_tokens(Path::new(&setting(
        "TOKENS",
        "seed/tokens.json",
    )))?);
    let user_offset: usize = setting("BENCH_USER_OFFSET", "0").parse()?;
    let metrics: Metrics = Arc::new(Mutex::new(std::array::from_fn(|_| Samples::default())));
    let connection_metrics = Arc::new(Mutex::new(connections::Connections::default()));
    let runtime_errors = Arc::new(AtomicUsize::new(0));
    let runtime_error_details = Arc::new(Mutex::new(errors::ErrorDetails::default()));
    let startup_ips = source_ips.clone();
    let client_ips = source_ips.clone();
    let client_connections = connection_metrics.clone();
    let attack = attack.set_user_client_factory(move |index, config| {
        build_client(index, config, &client_ips, client_connections.clone())
    });
    let startup_targets = targets.clone();
    let startup_hosts = assigned_hosts.clone();
    let startup: TransactionFunction = Arc::new(move |user| {
        let ips = startup_ips.clone();
        let targets = startup_targets.clone();
        let assigned_hosts = startup_hosts.clone();
        Box::pin(async move {
            let index = destinations::index(
                user_offset + user.weighted_users_index,
                ips.len(),
                targets.len(),
            );
            let target = &targets[index];
            user.set_base_url(target.as_str())?;
            assigned_hosts
                .lock()
                .unwrap()
                .insert(target.as_str().to_owned());
            think(0.0, 5.0).await;
            Ok(())
        })
    });
    let browse_metrics = metrics.clone();
    let browse_source_ips = source_ips.clone();
    let browsing: TransactionFunction = Arc::new(move |user| {
        let tokens = tokens.clone();
        let metrics = browse_metrics.clone();
        let source_ips = browse_source_ips.clone();
        Box::pin(async move { browse(user, &tokens, &metrics, user_offset, &source_ips).await })
    });
    if let Ok(start_at) = env::var("BENCH_START_AT") {
        let remaining = start_at.parse::<i64>()? - Utc::now().timestamp_millis();
        if remaining < -250 {
            return Err("missed coordinated start".into());
        }
        if remaining > 0 {
            tokio::time::sleep(Duration::from_millis(remaining as u64)).await;
        }
    }
    let counters = metrics.clone();
    let connector_counts = connection_metrics.clone();
    let roles = std::collections::BTreeMap::from([("generator".into(), vec![std::process::id()])]);
    let telemetry_path = env::var("BENCH_TELEMETRY").unwrap_or_else(|_| "telemetry.jsonl".into());
    let sample_interval: f64 = setting("BENCH_SAMPLE_INTERVAL", "1").parse()?;
    if !sample_interval.is_finite() || sample_interval < 0.1 {
        return Err("sample interval must be at least 0.1 seconds".into());
    }
    let sampler = telemetry::Sampler::start(
        telemetry_path.into(),
        roles,
        Duration::from_secs_f64(sample_interval),
        Arc::new(move || {
            let counters = counters.lock().unwrap();
            serde_json::json!({"requests": counters.iter().map(|s| [s.latency_ms.len(), s.failed]).collect::<Vec<_>>(), "connections": connector_counts.lock().unwrap().counts()})
        }),
    )?;
    let results = attack
        .set_default(GooseDefault::NoMetrics, !native_metrics)?
        .set_default(
            GooseDefault::Host,
            setting("BASE_URL", "http://127.0.0.1:3000").as_str(),
        )?
        .set_default(GooseDefault::TestPlan, plan.as_str())?
        .set_default(
            GooseDefault::CoordinatedOmissionMitigation,
            GooseCoordinatedOmissionMitigation::Disabled,
        )?
        .set_default(GooseDefault::NoTelnet, true)?
        .set_default(GooseDefault::NoWebSocket, true)?
        .register_scenario(
            scenario!("Feed browser")
                .register_transaction(
                    Transaction::new(track_errors(
                        startup,
                        runtime_errors.clone(),
                        runtime_error_details.clone(),
                        user_offset,
                        source_ips.clone(),
                    ))
                    .set_name("Start user")
                    .set_on_start(),
                )
                .register_transaction(
                    Transaction::new(track_errors(
                        browsing,
                        runtime_errors.clone(),
                        runtime_error_details.clone(),
                        user_offset,
                        source_ips.clone(),
                    ))
                    .set_name("Browse feed"),
                ),
        )
        .execute()
        .await?;
    let telemetry_report = sampler.finish().map_err(|error| error.to_string());
    let mut metrics = metrics.lock().unwrap();
    let mut aggregate = Samples::default();
    let mut endpoints = std::collections::BTreeMap::new();
    for (endpoint, samples) in ENDPOINTS.iter().zip(metrics.iter_mut()) {
        aggregate.latency_ms.extend_from_slice(&samples.latency_ms);
        aggregate.failed += samples.failed;
        aggregate.checks_failed += samples.checks_failed;
        aggregate.errors.merge(&samples.errors);
        let mut endpoint_summary = serde_json::to_value(samples.summary())?;
        if let serde_json::Value::Object(rates) =
            connections::request_rates(samples, run_duration_s(&results))
        {
            endpoint_summary.as_object_mut().unwrap().extend(rates);
        }
        endpoints.insert(*endpoint, endpoint_summary);
    }
    let summary = aggregate.summary();
    let runtime_errors = runtime_errors.load(Ordering::Relaxed);
    let completed = run_completed(&results, goose::is_killswitch_triggered());
    let passed = completed && passes(&summary, runtime_errors);
    let connection_report = connection_metrics
        .lock()
        .unwrap()
        .report(run_duration_s(&results));
    let report = serde_json::json!({
        "generator": "goose 0.18.1",
        "maximum_users": results.maximum_users,
        "total_users": results.total_users,
        "duration_s": run_duration_s(&results),
        "started_at": results.history.first().map(|step| step.timestamp),
        "finished_at": results.history.last().map(|step| step.timestamp),
        "hold_started_at": results.history.iter().find(|step| matches!(step.action, TestPlanStepAction::Maintaining)).map(|step| step.timestamp),
        "hold_finished_at": results.history.iter().find(|step| matches!(step.action, TestPlanStepAction::Decreasing)).map(|step| step.timestamp),
        "planned_hold_s": planned_hold_s,
        "hosts": &*assigned_hosts.lock().unwrap(),
        "configured_hosts": targets.iter().map(reqwest::Url::as_str).collect::<Vec<_>>(),
        "destination_ports": targets.iter().filter_map(reqwest::Url::port_or_known_default).collect::<Vec<_>>(),
        "assignment": "fixed per user, balanced over source IP and destination port combinations; SSH workers use global user offsets",
        "native_metrics": native_metrics,
        "source_ips": source_ips.as_ref(),
        "latency": "client wall time through complete response body, including connection setup",
        "percentiles": "k6 linear interpolation over unrounded milliseconds",
        "scope": "whole run including ramp-up, hold, ramp-down and drain",
        "coordinated_omission": "disabled for scoring; real requests only",
        "thresholds": { "p95_ms_lt": 500, "p99_ms_lt": 1000, "failure_rate_lt": 0.01 },
        "completed": completed,
        "runtime_errors": runtime_errors,
        "runtime_error_details": &*runtime_error_details.lock().unwrap(),
        "passed": passed,
        "aggregate": summary,
        "request_rates": connections::request_rates(&aggregate, run_duration_s(&results)),
        "connections": connection_report,
        "telemetry": match telemetry_report { Ok(value) => value, Err(error) => serde_json::json!({"unavailable_reason": error}) },
        "endpoints": endpoints,
    });
    let output = serde_json::to_string_pretty(&report)?;
    println!("\nScoring summary (full-body latency):\n{output}");
    let destination = setting("SUMMARY", "results/goose-summary.json");
    let path = Path::new(&destination);
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, format!("{output}\n"))?;
    if let Ok(destination) = env::var("BENCH_SAMPLES") {
        // Preserve real samples so distributed percentiles aren't averaged.
        let mut file = BufWriter::new(fs::File::create(destination)?);
        serde_json::to_writer(&mut file, &*metrics)?;
        file.flush()?;
        let destination = path
            .parent()
            .unwrap_or(Path::new("."))
            .join("connection-samples.json");
        let mut file = BufWriter::new(fs::File::create(destination)?);
        serde_json::to_writer(&mut file, &*connection_metrics.lock().unwrap())?;
        file.flush()?;
    }
    println!(
        "{} — summary: {}",
        if passed { "PASS" } else { "FAIL" },
        path.display()
    );
    Ok(passed)
}

fn observe_target(control: &Path) -> Result<u8, Box<dyn std::error::Error>> {
    let parse_pids = |name| -> Result<Vec<u32>, Box<dyn std::error::Error>> {
        let value = setting(name, "");
        if value.is_empty() {
            return Ok(Vec::new());
        }
        value
            .split(',')
            .map(|p| p.parse::<u32>().map_err(Into::into))
            .collect()
    };
    let interval: f64 = setting("BENCH_SAMPLE_INTERVAL", "1").parse()?;
    if !interval.is_finite() || interval < 0.1 {
        return Err("invalid sample interval".into());
    }
    telemetry::observe(
        control,
        parse_pids("BENCH_APPLICATION_PIDS")?,
        parse_pids("BENCH_PROXY_PIDS")?,
        config::parse_destination_ports(&setting(
            "BENCH_TARGET_PORTS",
            &setting("BENCH_TARGET_PORT", "3000"),
        ))?,
        Duration::from_secs_f64(interval),
    )
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.first().is_some_and(|arg| {
        [
            "--refresh-observer",
            "--observe",
            "--supervise",
            "--stop-supervised",
            "--ready-supervised",
            "--start-supervised",
        ]
        .contains(&arg.as_str())
    }) {
        let result = if let Some(path) = args.get(1) {
            if args[0] == "--refresh-observer" {
                runner::refresh_observer(Path::new(path))
            } else if args[0] == "--observe" {
                observe_target(Path::new(path))
            } else if args[0] == "--supervise" {
                runner::supervise(Path::new(path), &args[2..])
            } else if args[0] == "--stop-supervised" {
                runner::stop_supervised(Path::new(path))
            } else if args[0] == "--ready-supervised" {
                runner::ready_supervised(Path::new(path))
            } else if let Some(timestamp) = args.get(2) {
                runner::start_supervised(Path::new(path), timestamp)
            } else {
                Err("missing coordinated start timestamp".into())
            }
        } else {
            Err("missing supervisor control path".into())
        };
        return match result {
            Ok(code) => ExitCode::from(code),
            Err(error) => {
                eprintln!("benchmark supervisor error: {error}");
                ExitCode::from(2)
            }
        };
    }
    // Control/observer commands above need no async runtime or worker threads.
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("benchmark runtime error: {error}");
            return ExitCode::from(2);
        }
    };
    runtime.block_on(async {
        if args.first().is_some_and(|arg| arg == "--orchestrate") {
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
            return match runner::run(&args[1..], cancelled) {
                Ok(code) => ExitCode::from(code),
                Err(error) => {
                    eprintln!("benchmark runner error: {error}");
                    ExitCode::from(2)
                }
            };
        }
        match run().await {
            Ok(true) => ExitCode::SUCCESS,
            Ok(false) => ExitCode::from(1),
            Err(error) => {
                eprintln!("load test error: {error}");
                ExitCode::from(2)
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn client_factory_preserves_weighted_users_across_ramp_cycles() {
        use std::io::Read;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let host = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            for _ in 0..6 {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut headers = Vec::new();
                let mut buffer = [0; 1024];
                while !headers.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    let count = stream.read(&mut buffer).unwrap();
                    assert!(count > 0);
                    headers.extend_from_slice(&buffer[..count]);
                }
                let headers = String::from_utf8(headers).unwrap();
                let index = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("x-user-index")
                            .then(|| value.trim())
                    })
                    .unwrap();
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    index.len(),
                    index
                )
                .unwrap();
            }
        });
        let mut config = goose::config::GooseConfiguration::default();
        config.host = host;
        config.no_metrics = true;
        config.no_telnet = true;
        config.no_websocket = true;
        let built = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Mutex::new(Vec::new()));
        let startup = |scenario_index| -> TransactionFunction {
            let observed = started.clone();
            Arc::new(move |user| {
                let observed = observed.clone();
                Box::pin(async move {
                    let response = user.client.get(user.build_url("/")?).send().await?;
                    assert_eq!(
                        response.text().await?,
                        user.weighted_users_index.to_string()
                    );
                    observed
                        .lock()
                        .unwrap()
                        .push((user.weighted_users_index, scenario_index));
                    Ok(())
                })
            })
        };
        let idle: TransactionFunction = Arc::new(|_| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(())
            })
        });
        let construction = built.clone();
        let attack = GooseAttack::initialize_with_config(config.clone())
            .unwrap()
            .set_default(GooseDefault::TestPlan, "4,1s;2,1s;4,1s;0,1s")
            .unwrap()
            .set_user_client_factory(move |index, _| {
                construction.lock().unwrap().push(index);
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("x-user-index", index.to_string().parse().unwrap());
                reqwest::Client::builder()
                    .no_proxy()
                    .default_headers(headers)
                    .build()
            });
        assert!(built.lock().unwrap().is_empty());
        let metrics = attack
            .register_scenario(
                scenario!("A")
                    .register_transaction(Transaction::new(startup(0)).set_on_start())
                    .register_transaction(Transaction::new(idle.clone())),
            )
            .register_scenario(
                scenario!("B")
                    .set_weight(2)
                    .unwrap()
                    .register_transaction(Transaction::new(startup(1)).set_on_start())
                    .register_transaction(Transaction::new(idle)),
            )
            .execute()
            .await
            .unwrap();
        assert_eq!(*built.lock().unwrap(), vec![0, 1, 2, 3, 4, 5]);
        let mut started = started.lock().unwrap().clone();
        started.sort_unstable();
        assert_eq!(
            started,
            vec![(0, 1), (1, 1), (2, 0), (3, 1), (4, 1), (5, 0)]
        );
        assert_eq!(metrics.total_users, 6);
        assert_eq!(metrics.maximum_users, 4);
        server.join().unwrap();

        // A later factory error must stop users that are already running.
        let calls = Arc::new(AtomicUsize::new(0));
        let active = calls.clone();
        let tick: TransactionFunction = Arc::new(move |_| {
            let active = active.clone();
            Box::pin(async move {
                active.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(())
            })
        });
        let failed = GooseAttack::initialize_with_config(config)
            .unwrap()
            .set_default(GooseDefault::TestPlan, "2,1s;0,1s")
            .unwrap()
            .set_user_client_factory(|index, _| {
                let builder = reqwest::Client::builder().no_proxy();
                if index == 1 {
                    builder.user_agent("\n").build()
                } else {
                    builder.build()
                }
            })
            .register_scenario(
                scenario!("Factory failure").register_transaction(Transaction::new(tick)),
            )
            .execute()
            .await;
        assert!(failed.is_err());
        let stopped = calls.load(Ordering::Relaxed);
        assert!(stopped > 0);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(calls.load(Ordering::Relaxed), stopped);
    }

    #[test]
    fn native_metrics_are_enabled_only_for_requested_diagnostics() {
        let mut config = goose::config::GooseConfiguration::default();
        assert!(!native_metrics_requested(&config));
        config.request_log = "native.log".into();
        assert!(native_metrics_requested(&config));
        config.request_log.clear();
        config.report_file.push("native.html".into());
        assert!(native_metrics_requested(&config));
        config.report_file.clear();
        config.co_mitigation = Some(GooseCoordinatedOmissionMitigation::Disabled);
        assert!(!native_metrics_requested(&config));
        config.co_mitigation = Some(GooseCoordinatedOmissionMitigation::Average);
        assert!(native_metrics_requested(&config));
    }

    #[test]
    fn empty_environment_settings_use_k6_defaults() {
        let name = "FEED_LOAD_TEST_EMPTY_SETTING";
        env::remove_var(name);
        assert_eq!(setting(name, "default"), "default");
        env::set_var(name, "");
        assert_eq!(setting(name, "default"), "default");
        env::set_var(name, "configured");
        assert_eq!(setting(name, "default"), "configured");
        env::remove_var(name);
    }

    #[test]
    fn zero_hold_is_skipped_instead_of_running_forever() {
        assert_eq!(test_plan(100, "60s", "5m").unwrap(), "100,60s;100,5m;0,30s");
        for duration in ["0", "0s", "0m"] {
            assert_eq!(test_plan(100, "60s", duration).unwrap(), "100,60s;0,30s");
        }
        assert!(test_plan(100, "60s", "invalid").is_err());
        assert!(test_plan(100, "60s", " \t").is_err());
        assert!(test_plan(100, " ", "5m").is_err());
        assert!(test_plan(1, "0s;1,0s", "1s").is_err());
        assert!(test_plan(1, "0s", "1s;1,0s").is_err());
    }

    #[test]
    fn interruption_during_final_shutdown_cannot_pass() {
        use goose::test_plan::TestPlanHistory;
        let mut metrics = GooseMetrics::default();
        assert!(!run_completed(&metrics, false));
        metrics.history.push(TestPlanHistory {
            action: TestPlanStepAction::Finished,
            timestamp: Utc::now(),
            users: 0,
        });
        assert!(run_completed(&metrics, false));
        assert!(!run_completed(&metrics, true));
        metrics.history.insert(
            0,
            TestPlanHistory {
                action: TestPlanStepAction::Canceling,
                timestamp: Utc::now(),
                users: 1,
            },
        );
        assert!(!run_completed(&metrics, false));
    }

    #[test]
    fn interpolates_k6_percentiles_without_goose_buckets() {
        let mut samples = Samples {
            latency_ms: vec![1000.1, 499.9, 0.1, 500.1],
            ..Default::default()
        };
        let summary = samples.summary();
        assert_eq!(summary.med_ms, Some(500.0));
        assert!((summary.p95_ms.unwrap() - 925.1).abs() < 1e-9);
        assert!((summary.p99_ms.unwrap() - 985.1).abs() < 1e-9);
        assert_eq!(percentile(&[], 0.99), None);
        assert_eq!(percentile(&[0.25], 0.99), Some(0.25));
    }

    #[test]
    fn thresholds_are_strict_and_empty_runs_fail() {
        let mut samples = Samples::default();
        assert!(!passes(&samples.summary(), 0));
        samples.latency_ms = vec![10.0; 100];
        samples.failed = 1;
        assert!(!passes(&samples.summary(), 0)); // exactly 1%
        samples.latency_ms.push(10.0);
        let mut summary = samples.summary();
        assert!(passes(&summary, 0));
        summary.p95_ms = Some(500.0);
        assert!(!passes(&summary, 0));
        summary.p95_ms = Some(499.999);
        summary.p99_ms = Some(1000.0);
        assert!(!passes(&summary, 0));
        summary.p99_ms = Some(999.999);
        assert!(passes(&summary, 0));
    }

    #[test]
    fn http_failures_and_endpoint_checks_have_different_definitions() {
        assert!(http_success(302));
        assert!(http_success(204));
        assert!(!check_success(0, 204));
        assert!(!check_success(3, 200));
        assert!(check_success(2, 200));
        assert!(check_success(2, 201));
        for status in [0, 199, 400, 500] {
            assert!(!http_success(status));
        }
    }

    #[test]
    fn parses_source_ip_lists_and_inclusive_ipv4_ranges() {
        assert!(parse_source_ips("  ").unwrap().is_empty());
        assert_eq!(
            parse_source_ips("127.0.0.[1-5]").unwrap(),
            (1..=5)
                .map(|octet| IpAddr::V4(Ipv4Addr::new(127, 0, 0, octet)))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            parse_source_ips("192.0.2.10, 192.0.2.[11-12], ::1, 2001:db8::10").unwrap(),
            [
                "192.0.2.10",
                "192.0.2.11",
                "192.0.2.12",
                "::1",
                "2001:db8::10"
            ]
            .map(|ip| ip.parse::<IpAddr>().unwrap())
        );
        assert_eq!(parse_source_ips("127.0.0.[255-255]").unwrap().len(), 1);
    }

    #[test]
    fn rejects_invalid_source_ips_and_ranges() {
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

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn binds_users_round_robin_and_preserves_source_on_reconnect() {
        use std::io::{Read, Write};
        let ips = parse_source_ips("127.0.0.[1-5]").unwrap();
        validate_source_ips(&ips).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let host = format!("http://{}", listener.local_addr().unwrap());
        let expected_ips = ips.clone();
        let server = std::thread::spawn(move || {
            // Ten users wrap the five-address list; the last uses OS selection.
            for index in 0..=10 {
                let expected = if index < 10 {
                    expected_ips[index % expected_ips.len()]
                } else {
                    "127.0.0.1".parse().unwrap()
                };
                for requests in [2, 1] {
                    let (mut stream, peer) = listener.accept().unwrap();
                    assert_eq!(peer.ip(), expected);
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    for number in 0..requests {
                        let mut header = Vec::new();
                        let mut buffer = [0; 1024];
                        while !header.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                            let count = stream.read(&mut buffer).unwrap();
                            assert!(count > 0);
                            header.extend_from_slice(&buffer[..count]);
                        }
                        let connection = if number + 1 == requests {
                            "close"
                        } else {
                            "keep-alive"
                        };
                        write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: {connection}\r\n\r\nok").unwrap();
                        stream.flush().unwrap();
                    }
                }
            }
        });
        let mut config = goose::config::GooseConfiguration::default();
        config.timeout = Some("5".into());
        for index in 0..=10 {
            let mut user = GooseUser::single(host.parse().unwrap(), &config).unwrap();
            user.weighted_users_index = index;
            user.client = build_client(
                index,
                &config,
                if index < 10 { &ips } else { &[] },
                Arc::new(Mutex::new(connections::Connections::default())),
            )
            .unwrap();
            for _ in 0..3 {
                let response = user
                    .client
                    .get(format!("{host}/health"))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), 200);
                assert_eq!(response.text().await.unwrap(), "ok");
            }
        }
        server.join().unwrap();
    }

    #[test]
    fn rejects_malformed_bearer_headers_before_loading_users() {
        let path = env::temp_dir().join(format!("feed-load-tokens-{}.json", std::process::id()));
        for token in ["valid-token", "bad\ntoken", "bad\rtoken", "bad\0token"] {
            fs::write(
                &path,
                serde_json::to_vec(&serde_json::json!([
                    {"username": "test-user", "token": "valid-token"},
                    {"username": "test-user", "token": token},
                ]))
                .unwrap(),
            )
            .unwrap();
            assert_eq!(load_tokens(&path).is_ok(), token == "valid-token");
        }
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn suppressed_request_builder_errors_invalidate_healthy_scores() {
        let mut config = goose::config::GooseConfiguration::default();
        config.no_metrics = true;
        let mut user = GooseUser::single("http://127.0.0.1:1".parse().unwrap(), &config).unwrap();
        let runtime_errors = Arc::new(AtomicUsize::new(0));
        let metrics: Metrics = Arc::new(Mutex::new(std::array::from_fn(|_| Samples::default())));
        let request_metrics = metrics.clone();
        let function: TransactionFunction = Arc::new(move |user| {
            let metrics = request_metrics.clone();
            Box::pin(async move {
                let token = Token {
                    username: "test-user".into(),
                    token: "bad\ntoken".into(),
                };
                request(
                    user,
                    &metrics,
                    2,
                    "/posts/42/like",
                    Some(&token),
                    None,
                    0,
                    &[],
                )
                .await?;
                Ok(())
            })
        });
        let tracked = track_errors(
            function,
            runtime_errors.clone(),
            Arc::new(Mutex::new(errors::ErrorDetails::default())),
            0,
            Arc::new(Vec::new()),
        );
        // Bypass preflight to exercise an unexpected runtime construction error.
        assert!(tracked(&mut user).await.is_err());
        assert_eq!(runtime_errors.load(Ordering::Relaxed), 1);
        assert_eq!(
            metrics.lock().unwrap()[2].errors.counts["kind:request_builder"],
            1
        );
        let mut healthy_samples = Samples {
            latency_ms: vec![10.0; 100],
            ..Default::default()
        };
        let summary = healthy_samples.summary();
        assert!(passes(&summary, 0));
        assert!(!passes(&summary, runtime_errors.load(Ordering::Relaxed)));
    }

    #[test]
    fn reported_duration_includes_completion_after_scheduled_stop() {
        use goose::test_plan::TestPlanHistory;
        let started = Utc::now();
        let mut metrics = GooseMetrics::default();
        assert_eq!(run_duration_s(&metrics), None);
        metrics.duration = 13;
        metrics.history = vec![
            TestPlanHistory {
                action: TestPlanStepAction::Increasing,
                timestamp: started,
                users: 0,
            },
            TestPlanHistory {
                action: TestPlanStepAction::Decreasing,
                timestamp: started + chrono::TimeDelta::seconds(13),
                users: 50,
            },
            TestPlanHistory {
                action: TestPlanStepAction::Finished,
                timestamp: started + chrono::TimeDelta::milliseconds(17448),
                users: 0,
            },
        ];
        assert_eq!(run_duration_s(&metrics), Some(17.448));
    }

    #[tokio::test]
    async fn scores_full_bodies_auth_writes_and_transport_failures() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let host = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            for (path, status, body) in [
                ("/feed", "200 OK", r#"{"posts":[{"id":42}]}"#),
                (
                    "/posts/42",
                    "500 Internal Server Error",
                    r#"{"error":"failed"}"#,
                ),
                ("/posts/42/like", "201 Created", r#"{"liked":true}"#),
                ("/posts", "201 Created", r#"{"post":{"id":43}}"#),
                ("/feed", "503 Service Unavailable", "truncated"),
                ("/posts/42", "201 Created", "truncated"),
            ] {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut received = Vec::new();
                let mut chunk = [0; 1024];
                let header_end = loop {
                    let count = stream.read(&mut chunk).unwrap();
                    assert!(count > 0);
                    received.extend_from_slice(&chunk[..count]);
                    if let Some(end) = received.windows(4).position(|w| w == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let headers = String::from_utf8(received[..header_end].to_vec()).unwrap();
                let headers = headers.to_lowercase();
                assert!(headers.lines().next().unwrap().contains(path));
                if path.ends_with("/like") || path == "/posts" {
                    assert!(headers.contains("authorization: bearer test-token\r\n"));
                    assert!(headers.contains("content-type: application/json\r\n"));
                }
                if path == "/posts" {
                    let length: usize = headers
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length: "))
                        .unwrap()
                        .parse()
                        .unwrap();
                    while received.len() < header_end + length {
                        let count = stream.read(&mut chunk).unwrap();
                        assert!(count > 0);
                        received.extend_from_slice(&chunk[..count]);
                    }
                    let json: serde_json::Value =
                        serde_json::from_slice(&received[header_end..]).unwrap();
                    assert_eq!(json["body"], "test post");
                }
                let length = if body == "truncated" { 100 } else { body.len() };
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                stream.flush().unwrap();
                // Headers arrive first. The scoring latency must include this delay.
                std::thread::sleep(Duration::from_millis(80));
                stream.write_all(body.as_bytes()).unwrap();
            }
        });
        let mut config = goose::config::GooseConfiguration::default();
        config.no_metrics = true;
        config.co_mitigation = Some(GooseCoordinatedOmissionMitigation::Disabled);
        let mut user = GooseUser::single(host.parse().unwrap(), &config).unwrap();
        let metrics: Metrics = Arc::new(Mutex::new(std::array::from_fn(|_| Samples::default())));
        let token = Token {
            username: "test-user".into(),
            token: "test-token".into(),
        };
        let feed = request(&mut user, &metrics, 0, "/feed", None, None, 0, &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Feed>(&feed).unwrap().posts[0].id,
            42
        );
        assert!(
            request(&mut user, &metrics, 1, "/posts/42", None, None, 0, &[])
                .await
                .unwrap()
                .is_none()
        );
        assert!(request(
            &mut user,
            &metrics,
            2,
            "/posts/42/like",
            Some(&token),
            None,
            0,
            &[]
        )
        .await
        .unwrap()
        .is_some());
        assert!(request(
            &mut user,
            &metrics,
            3,
            "/posts",
            Some(&token),
            Some(&serde_json::json!({"body":"test post"})),
            0,
            &[],
        )
        .await
        .unwrap()
        .is_some());
        assert!(request(&mut user, &metrics, 0, "/feed", None, None, 0, &[])
            .await
            .unwrap()
            .is_none());
        assert!(
            request(&mut user, &metrics, 1, "/posts/42", None, None, 0, &[])
                .await
                .unwrap()
                .is_none()
        );
        server.join().unwrap();
        // The listener is now closed: connection failures still contribute a sample.
        assert!(request(&mut user, &metrics, 0, "/feed", None, None, 0, &[])
            .await
            .unwrap()
            .is_none());
        let metrics = metrics.lock().unwrap();
        assert_eq!(metrics[0].latency_ms.len(), 3);
        assert!(metrics[0].latency_ms[0] >= 80.0);
        assert_eq!(metrics[0].failed, 2);
        assert_eq!(metrics[0].checks_failed, 2);
        assert_eq!(metrics[1].failed, 2);
        assert_eq!(metrics[1].checks_failed, 2);
        assert_eq!(metrics[2].failed, 0);
        assert_eq!(metrics[3].failed, 0);
        assert_eq!(metrics[0].errors.counts["status:503"], 1);
        assert_eq!(metrics[0].errors.counts["kind:body_read"], 1);
        assert_eq!(metrics[0].errors.examples.len(), 2);
        let body_error = metrics[0]
            .errors
            .examples
            .iter()
            .find(|example| example.kind == "body_read")
            .unwrap();
        assert_eq!(body_error.status_code, Some(503));
        assert!(body_error.latency_ms.unwrap() >= 80.0);
        assert_eq!(metrics[1].errors.counts["status:500"], 1);
        assert_eq!(metrics[0].errors.counts["kind:transport"], 1);
        assert_eq!(metrics[1].errors.counts["status:201"], 1);
        assert_eq!(metrics[1].errors.counts["kind:body_read"], 1);
        assert!(!metrics[1].errors.counts.contains_key("kind:check"));
    }
}
