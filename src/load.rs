use crate::{
    config, connections, destinations, errors,
    samples::{self, Metrics, Samples},
    transport,
    workload::{
        Token, User, browse, load_tokens, parse_source_ips, runtime_error, think,
        validate_source_ips,
    },
};
use chrono::Utc;
use num_traits::ToPrimitive;
use std::{
    collections::BTreeSet,
    env, fs,
    io::{BufWriter, Write},
    net::IpAddr,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::task::{AbortHandle, JoinSet};
use twelve_dollar_benchmark::{setting, telemetry};

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
    let desired = required.max(1_048_576);
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

struct Workload {
    ramp: Duration,
    hold: Duration,
    source_ips: Arc<Vec<IpAddr>>,
    targets: Arc<Vec<Arc<transport::ParsedBase>>>,
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
            .map(|url| transport::ParsedBase::new(Arc::new(url)))
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

pub(super) async fn run(args: &[String]) -> Result<bool, Box<dyn std::error::Error>> {
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
        let client = transport::Client::new(base.clone(), source, context.connections.clone());
        let mut assigned = context.assigned.lock().unwrap();
        if !assigned.contains(base.url().as_str()) {
            assigned.insert(base.url().as_str().to_owned());
        }
        drop(assigned);
        users.maximum += 1;
        users.launched += 1;
        let stops = users.stops.clone();
        let stop_index = local;
        let tokens = context.workload.tokens.clone();
        let mut user = User {
            client,
            base: base.url().clone(),
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
    // Leave room for the supervisor to write a report before its 10 s hard kill.
    let mut abort_at = users
        .interrupted
        .then(|| tokio::time::Instant::now() + Duration::from_secs(5));
    while !users.tasks.is_empty() {
        let result = tokio::select! {
            result = users.tasks.join_next() => result,
            _ = context.signals.interrupt.recv(), if !users.interrupted => {
                users.interrupted = true;
                stop_all(&users.stops);
                abort_at = Some(tokio::time::Instant::now() + Duration::from_secs(5));
                None
            },
            _ = context.signals.terminate.recv(), if !users.interrupted => {
                users.interrupted = true;
                stop_all(&users.stops);
                abort_at = Some(tokio::time::Instant::now() + Duration::from_secs(5));
                None
            },
            () = async { tokio::time::sleep_until(abort_at.expect("enabled only when set")).await }, if abort_at.is_some() => {
                abort_at = None;
                users.tasks.abort_all();
                users.expected_cancellations = users.tasks.len();
                None
            },
        };
        if handle_user_result(context, users, result) && abort_at.is_none() {
            abort_at = Some(tokio::time::Instant::now() + Duration::from_secs(5));
        }
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
        let cp = path
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
    let report = build_report(context, result, &mut context.metrics.lock().unwrap())?;
    let passed = report["passed"] == true;
    let output = serde_json::to_string_pretty(&report)?;
    println!("\nScoring summary (full-body latency):\n{output}");
    fs::write(&path, format!("{output}\n"))?;
    println!(
        "{} — summary: {}",
        if passed { "PASS" } else { "FAIL" },
        path.display()
    );
    Ok(passed)
}

fn build_report(
    context: &RunContext,
    result: RunResult,
    samples: &mut [Samples; 4],
) -> Result<serde_json::Value, serde_json::Error> {
    let elapsed = Some(result.duration_s);
    let runtime_count = context.runtime_errors.load(Ordering::Relaxed);
    let completed = !result.interrupted;
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
        "configured_hosts": context.workload.targets.iter().map(|base| base.url().as_str()).collect::<Vec<_>>(),
        "destination_ports": context.workload.targets.iter().filter_map(|base| base.url().port_or_known_default()).collect::<Vec<_>>(),
        "assignment": "fixed per user, balanced over source IP and destination port combinations; SSH workers use global user offsets",
        "source_ips": context.workload.source_ips.as_ref(),
        "percentiles": "k6 linear interpolation over unrounded milliseconds",
        "scope": "whole run including ramp-up, hold, ramp-down and drain",
        "coordinated_omission": "disabled for scoring; real requests only",
        "runtime_error_details": &*context.runtime_details.lock().unwrap(),
        "connections": connection_report,
        "telemetry": match result.telemetry {
            Ok(value) => value,
            Err(error) => serde_json::json!({"unavailable_reason": error}),
        },
    });
    samples::report(samples, elapsed, runtime_count, completed, report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::samples::{aggregate_samples, passes};
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
        summary.latency.p95 = Some(500.0);
        assert!(!passes(&summary, 0));
        summary.latency.p95 = Some(499.0);
        summary.latency.p99 = Some(1000.0);
        assert!(!passes(&summary, 0));
        summary.latency.p99 = Some(999.0);
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
        assert_eq!(summary.latency.median, Some(5.0));
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
