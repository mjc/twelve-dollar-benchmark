use chrono::{SecondsFormat, Utc};
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
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::task::JoinSet;
use twelve_dollar_benchmark::{ENDPOINTS, control, setting, telemetry};

mod config;
mod connections;
mod destinations;
mod errors;
mod runner;
mod scoring;

#[derive(Clone, Copy)]
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

    fn method(self) -> reqwest::Method {
        match self {
            Self::Feed | Self::Post => reqwest::Method::GET,
            Self::Like | Self::Create => reqwest::Method::POST,
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
    // ponytail: exact samples match k6 and use about 8 MB per million requests.
    latency_ms: Vec<f64>,
    failed: usize,
    checks_failed: usize,
    #[serde(default)]
    errors: errors::ErrorDetails,
}
type Metrics = Arc<Mutex<[Samples; 4]>>;

struct User {
    client: reqwest::Client,
    base: reqwest::Url,
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
    #[allow(clippy::cast_precision_loss)] // Report statistics are inherently floating point.
    fn summary(&mut self) -> Summary {
        self.latency_ms.sort_unstable_by(f64::total_cmp);
        let n = self.latency_ms.len();
        Summary {
            requests: n,
            failed: self.failed,
            failure_rate: (n > 0).then(|| self.failed as f64 / n as f64),
            checks_failed: self.checks_failed,
            avg_ms: (n > 0).then(|| self.latency_ms.iter().sum::<f64>() / n as f64),
            med_ms: percentile(&self.latency_ms, 0.5),
            p95_ms: percentile(&self.latency_ms, 0.95),
            p99_ms: percentile(&self.latency_ms, 0.99),
            max_ms: self.latency_ms.last().copied(),
            errors: self.errors.clone(),
        }
    }
}
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)] // p is bounded to [0, 1]; checked slice access validates rounded ranks.
fn percentile(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() || !p.is_finite() || !(0.0..=1.0).contains(&p) {
        return None;
    }
    let rank = p * (sorted.len() - 1) as f64;
    let low = *sorted.get(rank.floor() as usize)?;
    let high = *sorted.get(rank.ceil() as usize)?;
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
fn runtime_error(
    details: &Arc<Mutex<errors::ErrorDetails>>,
    count: &AtomicUsize,
    error: &(dyn std::error::Error + 'static),
    user_index: usize,
    url: &str,
    source: Option<IpAddr>,
) {
    count.fetch_add(1, Ordering::Relaxed);
    details.lock().unwrap().record(errors::example(
        "runtime",
        "",
        None,
        "runtime",
        errors::error_chain(error),
        None,
        user_index,
        url,
        source.map(|v| v.to_string()),
    ));
}

#[allow(clippy::too_many_lines)] // Keep full-body timing and sample accounting in one request path.
async fn request(
    user: &User,
    endpoint: Endpoint,
    path: &str,
    auth: Option<&Token>,
    body: Option<&serde_json::Value>,
) -> Option<Vec<u8>> {
    let url = match user.base.join(path) {
        Ok(url) => url,
        Err(error) => {
            runtime_error(
                &user.runtime_details,
                &user.runtime_errors,
                &error,
                user.index,
                user.base.as_str(),
                user.source,
            );
            sample_mut(&mut user.metrics.lock().unwrap(), endpoint)
                .errors
                .record(errors::example(
                    endpoint.name(),
                    path,
                    None,
                    "request_builder",
                    errors::error_chain(&error),
                    None,
                    user.index,
                    user.base.as_str(),
                    user.source.map(|v| v.to_string()),
                ));
            return None;
        }
    };
    let mut builder = user.client.request(endpoint.method(), url);
    if let Some(token) = auth {
        builder = builder
            .bearer_auth(&token.token)
            .header(reqwest::header::CONTENT_TYPE, "application/json");
    }
    if let Some(body) = body {
        builder = builder.json(body);
    }
    let started = Instant::now();
    let result = builder.send().await;
    if let Some(error) = result.as_ref().err().filter(|error| error.is_builder()) {
        runtime_error(
            &user.runtime_details,
            &user.runtime_errors,
            error,
            user.index,
            user.base.as_str(),
            user.source,
        );
    }
    let (status, bytes, failure) = match result {
        Ok(response) => {
            let status = response.status().as_u16();
            match response.bytes().await {
                Ok(bytes) => (status, Some(bytes.to_vec()), None),
                Err(error) => (
                    status,
                    None,
                    Some(("body_read", errors::error_chain(&error.without_url()))),
                ),
            }
        }
        Err(error) => (
            0,
            None,
            Some((
                if error.is_builder() {
                    "request_builder"
                } else {
                    "transport"
                },
                errors::error_chain(&error.without_url()),
            )),
        ),
    };
    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
    let success = http_success(status) && bytes.is_some();
    let mut metrics = user.metrics.lock().unwrap();
    let samples = sample_mut(&mut metrics, endpoint);
    samples.latency_ms.push(latency_ms);
    samples.failed += usize::from(!success);
    samples.checks_failed += usize::from(!endpoint.successful_status(status) || bytes.is_none());
    let example = if let Some((kind, reason)) = failure {
        Some((kind, reason, None))
    } else if !http_success(status) {
        Some(("http", format!("HTTP status {status}"), bytes.as_deref()))
    } else if !endpoint.successful_status(status) {
        Some(("check", format!("unexpected HTTP status {status}"), None))
    } else {
        None
    };
    if let Some((kind, reason, body)) = example {
        samples.errors.record(
            errors::example(
                endpoint.name(),
                path,
                (status != 0).then_some(status),
                kind,
                reason,
                body,
                user.index,
                user.base.as_str(),
                user.source.map(|v| v.to_string()),
            )
            .with_latency(latency_ms),
        );
    }
    bytes.filter(|_| endpoint.successful_status(status))
}

fn choose_post_id(feed: &Feed) -> Option<u64> {
    if feed.posts.is_empty() || feed.posts.iter().any(|post| post.id == 0) {
        return None;
    }
    feed.posts
        .get(rand::rng().random_range(0..feed.posts.len()))
        .map(|post| post.id)
}

async fn browse(user: &User, tokens: &[Token], iteration: usize) {
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
            samples.errors.record(errors::example(
                Endpoint::Feed.name(),
                "/feed",
                Some(200),
                "check",
                "feed body was empty or malformed",
                None,
                user.index,
                user.base.as_str(),
                user.source.map(|v| v.to_string()),
            ));
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
#[allow(clippy::cast_precision_loss)] // Duration scaling needs a floating point user fraction.
fn ramp_offset(user: usize, users: usize, ramp: Duration) -> Duration {
    if users <= 1 {
        Duration::ZERO
    } else {
        ramp.mul_f64(user as f64 / (users - 1) as f64)
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
    source: Option<IpAddr>,
    connections: Arc<Mutex<connections::Connections>>,
) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .no_proxy()
        .http1_only()
        .pool_idle_timeout(None)
        .pool_max_idle_per_host(1)
        .connector_layer(connections::ConnectLayer(connections))
        .local_address(source)
        .gzip(true)
        .timeout(Duration::from_secs(60))
        .build()
}

#[allow(clippy::too_many_lines)] // Keep launch, cancellation, ramp-down, and drain ordering explicit.
async fn run(args: &[String]) -> Result<bool, Box<dyn std::error::Error>> {
    let cfg = config::parse(args)?;
    if cfg.help {
        println!("{}", config::HELP);
        return Ok(true);
    }
    if cfg.users == 0 {
        return Err("users must be positive".into());
    }
    let ramp = config::duration(&cfg.ramp_up)?;
    let hold = config::duration(&cfg.duration)?;
    prepare_descriptor_limit(cfg.users)?;
    hyper_util::client::legacy::connect::set_source_port_range(1024, 65535)?;
    let source_ips = Arc::new(parse_source_ips(&cfg.source_ips)?);
    validate_source_ips(&source_ips)?;
    let targets = Arc::new(destinations::urls(&cfg.base_url, &cfg.destination_ports)?);
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
    let metrics: Metrics = Arc::new(Mutex::new(std::array::from_fn(|_| Samples::default())));
    let connections = Arc::new(Mutex::new(connections::Connections::default()));
    let assigned = Arc::new(Mutex::new(BTreeSet::new()));
    let runtime_errors = Arc::new(AtomicUsize::new(0));
    let runtime_details = Arc::new(Mutex::new(errors::ErrorDetails::default()));
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupted = false;
    let env_start = env::var("BENCH_START_AT")
        .ok()
        .map(|s| s.parse::<i64>())
        .transpose()?;
    if let Some(start) = env_start
        && let Some(delay) = scheduled_start_delay(start, Utc::now().timestamp_millis())?
    {
        interrupted = tokio::select! {
            () = tokio::time::sleep(delay) => false,
            _ = interrupt.recv() => true,
            _ = terminate.recv() => true,
        };
    }
    let telemetry_path = env::var("BENCH_TELEMETRY").unwrap_or_else(|_| "telemetry.jsonl".into());
    let sample_interval = cfg.sample_interval;
    let telemetry_metrics = metrics.clone();
    let telemetry_connections = connections.clone();
    let sampler = telemetry::Sampler::start(
        telemetry_path.into(),
        std::collections::BTreeMap::from([("generator".to_owned(), vec![std::process::id()])]),
        sample_interval,
        Arc::new(move || {
            let m = telemetry_metrics.lock().unwrap();
            serde_json::json!({"requests":m.iter().map(|s|[s.latency_ms.len(),s.failed]).collect::<Vec<_>>(),"connections":telemetry_connections.lock().unwrap().counts()})
        }),
    )?;
    let started_at = Utc::now();
    let start = Instant::now();
    let mut tasks = JoinSet::new();
    let mut stops = Vec::with_capacity(cfg.users);
    let mut max_users = 0usize;
    for local in 0..cfg.users {
        if interrupted {
            break;
        }
        let at = start
            .checked_add(ramp_offset(local, cfg.users, ramp))
            .ok_or("ramp-up deadline exceeds the system clock range")?;
        tokio::select! {
            () = wait_ramp_slot(at) => {},
            _ = interrupt.recv() => { interrupted = true; break; },
            _ = terminate.recv() => { interrupted = true; break; },
        }
        let Some(index) = global_user_index(user_offset, local) else {
            interrupted = true;
            break;
        };
        let target_index = destinations::index(index, source_ips.len(), targets.len());
        let Some(base) = targets.get(target_index).cloned() else {
            runtime_error(
                &runtime_details,
                &runtime_errors,
                &std::io::Error::other("destination assignment exceeded configured targets"),
                index,
                &cfg.base_url,
                None,
            );
            interrupted = true;
            break;
        };
        let source = source_ips.get(index % source_ips.len().max(1)).copied();
        let client = match build_client(source, connections.clone()) {
            Ok(client) => client,
            Err(error) => {
                runtime_error(
                    &runtime_details,
                    &runtime_errors,
                    &error,
                    index,
                    base.as_str(),
                    source,
                );
                interrupted = true;
                break;
            }
        };
        assigned.lock().unwrap().insert(base.as_str().to_owned());
        max_users += 1;
        let (tx, rx) = tokio::sync::watch::channel(false);
        stops.push(tx);
        let metrics = metrics.clone();
        let tokens = tokens.clone();
        let user = User {
            client,
            base,
            index,
            source,
            metrics,
            runtime_errors: runtime_errors.clone(),
            runtime_details: runtime_details.clone(),
        };
        tasks.spawn(async move {
            think(0.0, 5.0).await;
            let mut iteration = 0;
            while !*rx.borrow() {
                browse(&user, &tokens, iteration).await;
                iteration += 1;
            }
        });
    }
    let hold_started_at = (!interrupted).then(Utc::now);
    let hold_deadline = Instant::now()
        .checked_add(hold)
        .ok_or("hold deadline exceeds the system clock range")?;
    let mut hold_finished_at = None;
    if !interrupted {
        interrupted = tokio::select! {
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(hold_deadline)) => {
                hold_finished_at = Some(Utc::now()); false
            },
            _ = interrupt.recv() => true,
            _ = terminate.recv() => true,
        };
    }
    if interrupted {
        for tx in &stops {
            let _ = tx.send(true);
        }
    } else {
        for (n, tx) in stops.iter().enumerate() {
            let deadline = hold_deadline
                .checked_add(ramp_offset(n + 1, stops.len() + 1, Duration::from_secs(30)))
                .ok_or("ramp-down deadline exceeds the system clock range")?;
            interrupted = tokio::select! {
                () = wait_ramp_slot(deadline) => false,
                _ = interrupt.recv() => true,
                _ = terminate.recv() => true,
            };
            if interrupted {
                for stop in &stops {
                    let _ = stop.send(true);
                }
                break;
            }
            let _ = tx.send(true);
        }
    }
    while !tasks.is_empty() {
        let result = tokio::select! {
            result = tasks.join_next() => result,
            _ = interrupt.recv(), if !interrupted => { interrupted = true; for stop in &stops { let _ = stop.send(true); } None },
            _ = terminate.recv(), if !interrupted => { interrupted = true; for stop in &stops { let _ = stop.send(true); } None },
        };
        if let Some(Err(error)) = result {
            interrupted = true;
            for stop in &stops {
                let _ = stop.send(true);
            }
            runtime_error(
                &runtime_details,
                &runtime_errors,
                &error,
                user_offset,
                &cfg.base_url,
                None,
            );
        }
    }
    let telemetry_report = sampler.finish().map_err(|e| e.to_string());
    let finished_at = Utc::now();
    let run_duration_s = start.elapsed().as_secs_f64();
    let mut collected = metrics.lock().unwrap();
    let mut aggregate = Samples::default();
    let mut endpoints = serde_json::Map::new();
    let elapsed = Some(run_duration_s);
    for ((endpoint, name), samples) in Endpoint::ALL
        .into_iter()
        .zip(ENDPOINTS)
        .zip(collected.iter_mut())
    {
        debug_assert_eq!(endpoint.name(), name);
        aggregate.latency_ms.extend_from_slice(&samples.latency_ms);
        aggregate.failed += samples.failed;
        aggregate.checks_failed += samples.checks_failed;
        aggregate.errors.merge(&samples.errors);
        let mut summary = serde_json::to_value(samples.summary())?;
        if let (Some(summary), serde_json::Value::Object(rates)) = (
            summary.as_object_mut(),
            connections::request_rates(samples, elapsed),
        ) {
            summary.extend(rates);
        }
        endpoints.insert(name.into(), summary);
    }
    let summary = aggregate.summary();
    let runtime_count = runtime_errors.load(Ordering::Relaxed);
    let completed = !interrupted;
    let passed = completed && passes(&summary, runtime_count);
    let connection_report = connections.lock().unwrap().report(elapsed);
    let report = serde_json::json!({
        "generator": "reqwest",
        "requested_users": cfg.users,
        "source_port_allocation": {
            "first": 1024,
            "last": 65535,
            "policy": "explicit per source/destination counters",
        },
        "maximum_users": max_users,
        "total_users": max_users,
        "duration_s": elapsed,
        "started_at": started_at,
        "finished_at": finished_at,
        "hold_started_at": hold_started_at,
        "hold_finished_at": hold_finished_at,
        "planned_hold_s": hold.as_secs_f64(),
        "hosts": &*assigned.lock().unwrap(),
        "configured_hosts": targets.iter().map(reqwest::Url::as_str).collect::<Vec<_>>(),
        "destination_ports": targets.iter().filter_map(reqwest::Url::port_or_known_default).collect::<Vec<_>>(),
        "assignment": "fixed per user, balanced over source IP and destination port combinations; SSH workers use global user offsets",
        "source_ips": source_ips.as_ref(),
        "latency": "client wall time through complete response body, including connection setup",
        "percentiles": "k6 linear interpolation over unrounded milliseconds",
        "scope": "whole run including ramp-up, hold, ramp-down and drain",
        "coordinated_omission": "disabled for scoring; real requests only",
        "thresholds": {"p95_ms_lt": 500, "p99_ms_lt": 1000, "failure_rate_lt": 0.01},
        "completed": completed,
        "runtime_errors": runtime_count,
        "runtime_error_details": &*runtime_details.lock().unwrap(),
        "passed": passed,
        "aggregate": summary,
        "request_rates": connections::request_rates(&aggregate, elapsed),
        "connections": connection_report,
        "telemetry": match telemetry_report {
            Ok(value) => value,
            Err(error) => serde_json::json!({"unavailable_reason": error}),
        },
        "endpoints": endpoints,
    });
    let output = serde_json::to_string_pretty(&report)?;
    println!("\nScoring summary (full-body latency):\n{output}");
    let path = env::var("SUMMARY")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| cfg.output.as_ref().map(|dir| dir.join("summary.json")))
        .unwrap_or_else(|| std::path::PathBuf::from("results/summary.json"));
    let path = path.as_path();
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, format!("{output}\n"))?;
    if let Ok(destination) = env::var("BENCH_SAMPLES") {
        let mut file = BufWriter::new(fs::File::create(destination)?);
        serde_json::to_writer(&mut file, &*collected)?;
        file.flush()?;
        let cp = path
            .parent()
            .unwrap_or(Path::new("."))
            .join("connection-samples.json");
        let mut file = BufWriter::new(fs::File::create(cp)?);
        serde_json::to_writer(&mut file, &*connections.lock().unwrap())?;
        file.flush()?;
    }
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
    use std::io::{Read, Write};

    fn user(base: &str, metrics: Metrics) -> User {
        User {
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
            base: reqwest::Url::parse(base).unwrap(),
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
        let user = user("http://127.0.0.1:3000", metrics);
        browse(&user, &[], 0).await;
        assert_eq!(user.runtime_errors.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn request_reads_full_body_and_preserves_auth_and_json_write() {
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
        let user = user(&format!("http://{address}"), metrics.clone());
        let token = Token {
            username: "u".into(),
            token: "secret".into(),
        };
        let payload = serde_json::json!({"body":"hello"});
        let response = request(
            &user,
            Endpoint::Create,
            "/posts",
            Some(&token),
            Some(&payload),
        )
        .await;
        assert_eq!(response.unwrap().as_slice(), b"complete body".as_slice());
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
    async fn transport_failure_is_sampled_and_counted() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let metrics: Metrics = Arc::new(Mutex::new(std::array::from_fn(|_| Samples::default())));
        let user = user(&format!("http://{address}"), metrics.clone());
        let response = request(&user, Endpoint::Feed, "/feed", None, None).await;
        assert!(response.is_none());
        let mut data = metrics.lock().unwrap();
        let feed = sample_mut(&mut data, Endpoint::Feed);
        assert_eq!(feed.failed, 1);
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
        let user = user(&format!("http://{address}"), metrics.clone());
        assert!(
            request(&user, Endpoint::Feed, "/feed", None, None)
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
        let user = user(&format!("http://{address}"), metrics.clone());
        assert!(
            request(&user, Endpoint::Feed, "/feed", None, None)
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
            let client = build_client(Some(*ip), connection_metrics.clone()).unwrap();
            assert_eq!(
                client
                    .get(format!("http://{address}"))
                    .send()
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
                    .as_ref(),
                b"ok".as_slice()
            );
        }
        let peers = server.join().unwrap();
        assert_eq!(peers, ips);
        let mut report = connection_metrics.lock().unwrap();
        assert_eq!(report.successful_ms.len(), 2);
        assert_eq!(report.report(Some(1.0))["established"], 2);
    }

    #[tokio::test]
    async fn invalid_client_builder_counts_as_runtime_failure_and_fails_score() {
        let error = reqwest::Client::builder()
            .user_agent("\n")
            .build()
            .unwrap_err();
        let count = AtomicUsize::new(0);
        let details = Arc::new(Mutex::new(errors::ErrorDetails::default()));
        runtime_error(&details, &count, &error, 1, "http://127.0.0.1:3000", None);
        let mut samples = Samples {
            latency_ms: vec![1.0],
            ..Default::default()
        };
        assert!(!passes(&samples.summary(), count.load(Ordering::Relaxed)));
        assert_eq!(details.lock().unwrap().counts["kind:runtime"], 1);
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
