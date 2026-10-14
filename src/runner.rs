use super::{
    ENDPOINTS, Samples, config, connections, destinations, errors::ErrorDetails, load_tokens,
    parse_source_ips, passes,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error,
    fs::{self, File},
    io::{BufReader, Read},
    net::IpAddr,
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Clone, Serialize)]
struct Worker {
    name: String,
    users: usize,
    user_offset: usize,
    remote_dir: Option<String>,
    exit_code: Option<i32>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct Report {
    maximum_users: usize,
    total_users: usize,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    hold_started_at: Option<DateTime<Utc>>,
    hold_finished_at: Option<DateTime<Utc>>,
    planned_hold_s: f64,
    hosts: BTreeSet<String>,
    source_ips: Vec<IpAddr>,
    runtime_errors: usize,
    #[serde(default)]
    runtime_error_details: ErrorDetails,
    completed: bool,
    aggregate: Value,
    #[serde(default)]
    connections: Value,
    #[serde(default)]
    telemetry: Value,
}

// Quote every argument for the remote POSIX shell, including user-supplied flags.
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn ssh(user: &str, ip: &str) -> Command {
    let mut cmd = Command::new("ssh");
    cmd.args([
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
        "-l",
        user,
        "--",
        ip,
    ]);
    cmd
}

fn destination(user: &str, ip: &str, remote_dir: &str) -> String {
    let host = if ip.contains(':') {
        format!("[{ip}]")
    } else {
        ip.into()
    };
    format!("{user}@{host}:{remote_dir}")
}

fn scp() -> Command {
    let mut cmd = Command::new("scp");
    cmd.args(["-q", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]);
    cmd
}

fn checked(cmd: &mut Command, cancelled: &AtomicBool) -> Result<()> {
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stderr = child.stderr.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut message = String::new();
        let _ = stderr.read_to_string(&mut message);
        let _ = sender.send(message);
    });
    let status = wait_child(&mut child, cancelled, Some(Duration::from_secs(120)));
    // A proxy descendant may retain stderr after its parent was terminated.
    let message = receiver
        .recv_timeout(Duration::from_secs(1))
        .unwrap_or_default();
    let status = status?;
    if !status.success() {
        return Err(format!("command failed ({}): {}", status, message.trim()).into());
    }
    Ok(())
}

fn execute(cmd: &mut Command, cancelled: &AtomicBool) -> Result<ExitStatus> {
    // The coordinator forwards one SIGINT; terminal Ctrl-C must not also reach Goose.
    let mut child = cmd.stdin(Stdio::null()).process_group(0).spawn()?;
    wait_child(&mut child, cancelled, None)
}

fn interrupt_child(child: &Child) -> Result<()> {
    // SAFETY: this is our live, unreaped child; its PID cannot be reused.
    let result = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

fn wait_child(
    child: &mut Child,
    cancelled: &AtomicBool,
    timeout: Option<Duration>,
) -> Result<ExitStatus> {
    let started = Instant::now();
    let mut stopping = None;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if cancelled.load(Ordering::Relaxed) {
            if timeout.is_none() && stopping.is_none() {
                interrupt_child(child)?;
                stopping = Some(Instant::now());
            }
            if timeout.is_some()
                || stopping.is_some_and(|at: Instant| at.elapsed() >= Duration::from_secs(10))
            {
                child.kill()?;
                return Ok(child.wait()?);
            }
        }
        if timeout.is_some_and(|limit| started.elapsed() >= limit) {
            child.kill()?;
            child.wait()?;
            return Err("transfer/control command timed out".into());
        }
        thread::sleep(Duration::from_millis(100));
    }
}

pub(super) fn supervise(control: &Path, args: &[String]) -> Result<u8> {
    let start_at = twelve_dollar_benchmark::control::wait_start(control)?;
    let mut child = Command::new(env::current_exe()?)
        .args(args)
        .env("BENCH_START_AT", start_at.to_string())
        .stdin(Stdio::null())
        .spawn()?;
    let mut stopping = None;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if control.exists() && stopping.is_none() {
            interrupt_child(&child)?;
            stopping = Some(Instant::now());
        }
        if stopping.is_some_and(|started| started.elapsed() >= Duration::from_secs(10)) {
            child.kill()?;
            break child.wait()?;
        }
        thread::sleep(Duration::from_millis(100));
    };
    fs::write(control.with_extension("done"), format!("{status}\n"))?;
    Ok(status.code().unwrap_or(2) as u8)
}

// A target observer uses the same start barrier as the generators, then stops
// after their final responses. Network counters retain their own measured window.
struct TargetMonitor {
    child: Option<Child>,
    binary: PathBuf,
    dir: PathBuf,
    control: PathBuf,
    remote: Option<(String, String, String)>,
    heartbeat_cancelled: Arc<AtomicBool>,
    heartbeat_stop: Option<mpsc::Sender<()>>,
    heartbeat: Option<thread::JoinHandle<()>>,
}
impl TargetMonitor {
    fn prepare(
        cfg: &config::Config,
        binary: &Path,
        output: &Path,
        cancelled: &AtomicBool,
    ) -> Result<Self> {
        let binary = binary.with_file_name("twelve-dollar-observer");
        let url = reqwest::Url::parse(&cfg.base_url)?;
        let host = url
            .host_str()
            .ok_or("target URL has no host")?
            .trim_matches(['[', ']']);

        let monitor_user = cfg.monitor_user.as_ref().or(cfg.user.as_ref());
        if monitor_user.is_none() && !host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback()) {
            return Err(
                "target process monitoring needs --monitor-user for a remote target".into(),
            );
        }
        let dir = output.join("target");
        fs::create_dir(&dir)?;
        let remote = monitor_user.map(|user| {
            (
                user.clone(),
                host.to_owned(),
                format!(
                    "/tmp/twelve-dollar-observer-target-{}-{}",
                    Utc::now().timestamp_millis(),
                    std::process::id()
                ),
            )
        });
        let control = dir.join("metrics");
        let assignments = [
            format!(
                "BENCH_APPLICATION_PIDS={}",
                cfg.application_pids
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            format!(
                "BENCH_PROXY_PIDS={}",
                cfg.proxy_pids
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            format!(
                "BENCH_TARGET_PORTS={}",
                cfg.destination_ports
                    .iter()
                    .map(u16::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            format!(
                "BENCH_SAMPLE_INTERVAL={}",
                cfg.sample_interval.as_secs_f64()
            ),
        ];
        let log = File::create(dir.join("output.log"))?;
        let mut command = if let Some((user, host, remote_dir)) = &remote {
            checked(
                ssh(user, host).arg(format!("umask 077; mkdir -- {}", quote(remote_dir))),
                cancelled,
            )?;
            checked(
                scp().arg("--").arg(&binary).arg(destination(
                    user,
                    host,
                    &format!("{remote_dir}/twelve-dollar-observer"),
                )),
                cancelled,
            )?;
            checked(
                ssh(user, host).arg(format!(
                    "{} --version",
                    quote(&format!("{remote_dir}/twelve-dollar-observer"))
                )),
                cancelled,
            )?;
            let mut command = ssh(user, host);
            command.arg(format!(
                "cd {} || exit 2; trap '' HUP; exec env {} ./twelve-dollar-observer --observe metrics",
                quote(remote_dir),
                assignments
                    .iter()
                    .map(|v| quote(v))
                    .collect::<Vec<_>>()
                    .join(" ")
            ));
            command
        } else {
            let mut command = Command::new(&binary);
            command.current_dir(&dir).arg("--observe").arg(&control);
            for assignment in &assignments {
                let (key, value) = assignment.split_once('=').unwrap();
                command.env(key, value);
            }
            command
        };
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .process_group(0)
            .spawn()?;
        Ok(Self {
            child: Some(child),
            binary,
            dir,
            control,
            remote,
            heartbeat_cancelled: Arc::new(AtomicBool::new(false)),
            heartbeat_stop: None,
            heartbeat: None,
        })
    }
    fn control_command(&self, action: &str, timestamp: Option<&str>) -> Command {
        if let Some((user, host, remote_dir)) = &self.remote {
            let mut cmd = ssh(user, host);
            cmd.arg(format!(
                "{} {} {} {}",
                quote(&format!("{remote_dir}/twelve-dollar-observer")),
                action,
                quote(&format!("{remote_dir}/metrics")),
                timestamp.map(quote).unwrap_or_default()
            ));
            cmd
        } else {
            let mut cmd = Command::new(&self.binary);
            cmd.arg(action).arg(&self.control);
            if let Some(timestamp) = timestamp {
                cmd.arg(timestamp);
            }
            cmd
        }
    }
    fn ready(&mut self, cancelled: &AtomicBool) -> Result<()> {
        if self.child.as_mut().unwrap().try_wait()?.is_some() {
            return Err("target observer exited before readiness; see target/output.log".into());
        }
        checked(
            &mut self.control_command("--ready-supervised", None),
            cancelled,
        )
    }
    fn start(&mut self, timestamp: &str, cancelled: &AtomicBool) -> Result<()> {
        checked(
            &mut self.control_command("--start-supervised", Some(timestamp)),
            cancelled,
        )?;
        let remote = self.remote.clone();
        let binary = self.binary.clone();
        let control = self.control.clone();
        let flag = self.heartbeat_cancelled.clone();
        let (sender, receiver) = mpsc::channel();
        self.heartbeat_stop = Some(sender);
        self.heartbeat = Some(thread::spawn(move || {
            while matches!(
                receiver.recv_timeout(Duration::from_secs(10)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ) {
                let mut command = if let Some((user, host, dir)) = &remote {
                    let mut command = ssh(user, host);
                    command.arg(format!(
                        "{} --refresh-observer {}",
                        quote(&format!("{dir}/twelve-dollar-observer")),
                        quote(&format!("{dir}/metrics"))
                    ));
                    command
                } else {
                    let mut command = Command::new(&binary);
                    command.arg("--refresh-observer").arg(&control);
                    command
                };
                if let Err(error) = checked(&mut command, &flag)
                    && !flag.load(Ordering::Relaxed)
                {
                    eprintln!("Target telemetry lease refresh failed: {error}");
                }
            }
        }));
        Ok(())
    }
    fn finish(&mut self) -> Result<Value> {
        self.heartbeat_cancelled.store(true, Ordering::Relaxed);
        if let Some(sender) = self.heartbeat_stop.take() {
            let _ = sender.send(());
        }
        if let Some(handle) = self.heartbeat.take() {
            let _ = handle.join();
        }
        let stop = checked(
            &mut self.control_command("--stop-supervised", None),
            &AtomicBool::new(false),
        );
        if let Some(mut child) = self.child.take() {
            let status = wait_child(
                &mut child,
                &AtomicBool::new(false),
                Some(Duration::from_secs(20)),
            )?;
            if !status.success() {
                return Err(format!("target observer exited: {status}").into());
            }
        }
        stop?;
        if let Some((user, host, remote_dir)) = &self.remote {
            for name in ["telemetry.jsonl", "telemetry-summary.json"] {
                checked(
                    scp()
                        .arg("--")
                        .arg(destination(user, host, &format!("{remote_dir}/{name}")))
                        .arg(self.dir.join(name)),
                    &AtomicBool::new(false),
                )?;
            }
        }
        Ok(serde_json::from_reader(BufReader::new(File::open(
            self.dir.join("telemetry-summary.json"),
        )?))?)
    }
}
impl Drop for TargetMonitor {
    fn drop(&mut self) {
        if self.child.is_some() {
            let _ = self.finish();
        }
    }
}

fn prepare_monitor(
    cfg: &config::Config,
    binary: &Path,
    output: &Path,
    cancelled: &AtomicBool,
) -> (Option<TargetMonitor>, Option<String>) {
    let result = TargetMonitor::prepare(cfg, binary, output, cancelled).and_then(|mut monitor| {
        monitor.ready(cancelled)?;
        Ok(monitor)
    });
    match result {
        Ok(monitor) => (Some(monitor), None),
        Err(error) => {
            eprintln!("Target telemetry unavailable: {error}");
            (None, Some(error.to_string()))
        }
    }
}

fn start_monitor(
    monitor: &mut Option<TargetMonitor>,
    unavailable: &mut Option<String>,
    start_at: &str,
    cancelled: &AtomicBool,
) {
    let error = monitor
        .as_mut()
        .and_then(|observer| observer.start(start_at, cancelled).err());
    if let Some(error) = error {
        eprintln!("Target telemetry unavailable: {error}");
        *unavailable = Some(error.to_string());
        *monitor = None;
    }
}

fn network_hosts(generators: &Value, target: &Value) -> Value {
    let mut hosts = BTreeMap::<String, Value>::new();
    let reports = generators
        .as_object()
        .into_iter()
        .flat_map(|map| map.iter())
        .map(|(name, value)| (name.as_str(), value))
        .chain(std::iter::once(("target", target)));
    for (name, report) in reports {
        if report["host_boot_id"].is_null() || report["network_namespace"].is_null() {
            continue;
        }
        let key = format!("{}:{}", report["host_boot_id"], report["network_namespace"]);
        let entry = hosts.entry(key).or_insert_with(|| json!({"observed_by": [], "network": report["network"], "duration_s": report["observed_duration_s"], "limits": report["limits"]}));
        entry["observed_by"]
            .as_array_mut()
            .unwrap()
            .push(json!(name));
        if report["observed_duration_s"].as_f64().unwrap_or(0.0)
            > entry["duration_s"].as_f64().unwrap_or(0.0)
        {
            entry["network"] = report["network"].clone();
            entry["duration_s"] = report["observed_duration_s"].clone();
            entry["limits"] = report["limits"].clone();
        }
    }
    json!({"scope": "host network-namespace interface counters, including background traffic; aliases on one host are deduplicated; interfaces and RX/TX are not summed because virtual links and loopback can count traffic twice", "hosts": hosts})
}

fn split_users(total: usize, count: usize) -> Result<Vec<(usize, usize)>> {
    if count == 0 || total < count {
        return Err("VUS must be at least the number of generator hosts".into());
    }
    let mut offset = 0;
    Ok((0..count)
        .map(|index| {
            let users = total / count + usize::from(index < total % count);
            let assignment = (users, offset);
            offset += users;
            assignment
        })
        .collect())
}

fn collect(output: &Path, workers: &mut [Worker]) -> Value {
    let mut samples: [Samples; 4] = std::array::from_fn(|_| Samples::default());
    let mut maximum_users = 0;
    let mut total_users = 0;
    let mut runtime_errors = 0;
    let mut runtime_error_details = ErrorDetails::default();
    let mut started: Option<DateTime<Utc>> = None;
    let mut finished: Option<DateTime<Utc>> = None;
    let mut hosts = BTreeSet::new();
    let mut source_ips = BTreeSet::new();
    let mut completed = true;
    let mut connection_data = connections::Connections::default();
    let mut telemetry = BTreeMap::new();
    let mut connection_available = true;
    let mut hold_start = None;
    let mut hold_end = None;
    let mut required_hold = 0.0_f64;
    for worker in workers.iter_mut() {
        let parsed = (|| -> Result<(Report, [Samples; 4])> {
            let dir = output.join(&worker.name);
            let report: Report =
                serde_json::from_reader(BufReader::new(File::open(dir.join("summary.json"))?))?;
            let raw: [Samples; 4] =
                serde_json::from_reader(BufReader::new(File::open(dir.join("samples.json"))?))?;
            if raw.iter().any(|s| {
                s.failed > s.latency_ms.len()
                    || s.latency_ms.iter().any(|ms| !ms.is_finite() || *ms < 0.0)
            }) {
                return Err("invalid latency samples or failure counts".into());
            }
            let requests: usize = raw.iter().map(|s| s.latency_ms.len()).sum();
            let failed: usize = raw.iter().map(|s| s.failed).sum();
            if report.aggregate["requests"].as_u64() != Some(requests as u64)
                || report.aggregate["failed"].as_u64() != Some(failed as u64)
                || report.finished_at < report.started_at
                || (worker.remote_dir.is_some()
                    && (report.maximum_users != worker.users || report.total_users != worker.users))
            {
                return Err("summary does not match raw samples or timestamps".into());
            }
            Ok((report, raw))
        })();
        match parsed {
            Ok((report, raw)) => {
                let connection_samples = output.join(&worker.name).join("connection-samples.json");
                match File::open(connection_samples)
                    .map_err(|error| error.to_string())
                    .and_then(|file| {
                        serde_json::from_reader::<_, connections::Connections>(BufReader::new(file))
                            .map_err(|error| error.to_string())
                    }) {
                    Ok(samples)
                        if samples.cancelled <= samples.failed_ms.len()
                            && samples
                                .successful_ms
                                .iter()
                                .chain(&samples.failed_ms)
                                .all(|ms| ms.is_finite() && *ms >= 0.0)
                            && report.connections["established"].as_u64()
                                == Some(samples.successful_ms.len() as u64)
                            && report.connections["failed"].as_u64()
                                == Some(samples.failed_ms.len() as u64) =>
                    {
                        connection_data.merge(samples)
                    }
                    _ => connection_available = false,
                }
                telemetry.insert(worker.name.clone(), report.telemetry);
                completed &= report.completed && matches!(worker.exit_code, Some(0 | 1));
                maximum_users += report.maximum_users;
                total_users += report.total_users;
                runtime_errors += report.runtime_errors;
                let mut runtime_details = report.runtime_error_details;
                for example in &mut runtime_details.examples {
                    example.worker = Some(worker.name.clone());
                }
                runtime_error_details.merge(&runtime_details);
                started = Some(started.map_or(report.started_at, |s| s.min(report.started_at)));
                finished = Some(finished.map_or(report.finished_at, |s| s.max(report.finished_at)));
                hosts.extend(report.hosts);
                source_ips.extend(report.source_ips);
                if report.planned_hold_s > 0.0 {
                    required_hold = required_hold.max(report.planned_hold_s);
                    if let (Some(begin), Some(end)) =
                        (report.hold_started_at, report.hold_finished_at)
                    {
                        hold_start =
                            Some(hold_start.map_or(begin, |s: DateTime<Utc>| s.max(begin)));
                        hold_end = Some(hold_end.map_or(end, |s: DateTime<Utc>| s.min(end)));
                    } else {
                        completed = false;
                    }
                }
                for (combined, mut received) in samples.iter_mut().zip(raw) {
                    for example in &mut received.errors.examples {
                        example.worker = Some(worker.name.clone());
                    }
                    combined.latency_ms.append(&mut received.latency_ms);
                    combined.failed += received.failed;
                    combined.checks_failed += received.checks_failed;
                    combined.errors.merge(&received.errors);
                }
            }
            Err(error) => {
                completed = false;
                worker.error.get_or_insert_with(|| error.to_string());
            }
        }
    }
    completed &= workers.iter().all(|worker| worker.error.is_none());
    let common_hold_s = hold_start
        .zip(hold_end)
        .and_then(|(s, f)| (f - s).to_std().ok())
        .map(|d| d.as_secs_f64());
    if required_hold > 0.0 {
        completed &= common_hold_s.is_some_and(|s| s + 0.25 >= required_hold);
    }
    let mut aggregate = Samples::default();
    let mut endpoints = std::collections::BTreeMap::new();
    for (endpoint_name, sample) in ENDPOINTS.iter().zip(samples.iter_mut()) {
        aggregate.latency_ms.extend_from_slice(&sample.latency_ms);
        aggregate.failed += sample.failed;
        aggregate.checks_failed += sample.checks_failed;
        aggregate.errors.merge(&sample.errors);
        let mut endpoint = serde_json::to_value(sample.summary()).unwrap();
        let duration_s = started
            .zip(finished)
            .and_then(|(s, f)| (f - s).to_std().ok())
            .map(|d| d.as_secs_f64());
        if let Value::Object(rates) = connections::request_rates(sample, duration_s) {
            endpoint.as_object_mut().unwrap().extend(rates);
        }
        endpoints.insert(*endpoint_name, endpoint);
    }
    let summary = aggregate.summary();
    let duration_s = started
        .zip(finished)
        .and_then(|(s, f)| (f - s).to_std().ok())
        .map(|d| d.as_secs_f64());
    json!({
        "generator": "goose 0.18.1", "workers": workers,
        "completed": completed, "passed": completed && passes(&summary, runtime_errors),
        "runtime_errors": runtime_errors, "maximum_users": maximum_users,
        "runtime_error_details": runtime_error_details,
        "total_users": total_users, "hosts": hosts, "source_ips": source_ips,
        "started_at": started, "finished_at": finished,
        "common_hold_s": common_hold_s, "required_hold_s": required_hold,
        "hold_started_at": hold_start, "hold_finished_at": hold_end,
        // An elapsed interval across all workers, not a sum of their durations.
        "duration_s": started.zip(finished).and_then(|(s, f)| (f-s).to_std().ok()).map(|d| d.as_secs_f64()),
        "latency": "client wall time through complete response body, including connection setup",
        "percentiles": "k6 linear interpolation over all workers' unrounded samples",
        "thresholds": { "p95_ms_lt": 500, "p99_ms_lt": 1000, "failure_rate_lt": 0.01 },
        "aggregate": summary, "endpoints": endpoints,
        "request_rates": connections::request_rates(&aggregate, duration_s),
        "connections": if connection_available { connection_data.report(duration_s) } else { json!({"unavailable_reason": "missing or inconsistent per-worker connection samples"}) },
        "generator_telemetry": telemetry,
    })
}

pub(super) fn render_score_table(report: &Value) -> String {
    let number = |value: Option<f64>, suffix: &str| {
        value
            .map(|n| format!("{n:.2}{suffix}"))
            .unwrap_or_else(|| "unavailable".into())
    };
    let label = report["implementation"]
        .as_str()
        .unwrap_or("target")
        .replace('|', "\\|")
        .replace(['\r', '\n'], " ");
    format!(
        "| Rank | PR | Implementation | Max users | Req/s | p50 | p95 | p99 | Errors | CPU | Peak memory | Limited by |\n\
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|\n\
| — | — | {label} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
        if report["score_is_lower_bound"] == true {
            format!("≥ {}", report["score"])
        } else {
            report["score"].to_string()
        },
        number(report["hold"]["requests_per_second"].as_f64(), ""),
        number(report["aggregate"]["med_ms"].as_f64(), " ms"),
        number(report["aggregate"]["p95_ms"].as_f64(), " ms"),
        number(report["aggregate"]["p99_ms"].as_f64(), " ms"),
        number(
            report["aggregate"]["failure_rate"]
                .as_f64()
                .map(|n| n * 100.0),
            "%"
        ),
        number(report["hold"]["service_cpu_percent"].as_f64(), "%"),
        number(
            report["hold"]["sampled_peak_memory_bytes"]
                .as_f64()
                .map(|n| n / 1_048_576.0),
            " MiB"
        ),
        report["limit_reached"]
            .as_str()
            .unwrap_or("not established")
    )
}

pub(super) fn render_report(report: &Value, compact: bool) -> String {
    let fmt = |value: &Value| {
        value
            .as_f64()
            .map(|n| format!("{n:.2}"))
            .unwrap_or_else(|| "unavailable".into())
    };
    let title = report["score"].as_u64().map_or_else(
        || "results".into(),
        |score| {
            let bound = if report["score_is_lower_bound"] == true {
                "≥ "
            } else {
                ""
            };
            format!("score: {bound}{score} VUs")
        },
    );
    let table = if report["score"].is_u64() {
        format!(
            "{}\nReq/s and CPU are means from samples inside the confirmed hold. Memory is sampled peak cgroup usage, including socket buffers and page cache. Latency and errors cover the complete confirmation run. Rank and PR are left blank; Limited by names a failing threshold, not an inferred resource bottleneck.",
            render_score_table(report)
        )
    } else {
        format!(
            "| Requested VUs | Reached VUs | p95 ms | p99 ms | HTTP errors % | Result |\n|---:|---:|---:|---:|---:|---|\n| {} | {} | {} | {} | {} | {} |",
            report["requested_users"],
            report["maximum_users"],
            fmt(&report["aggregate"]["p95_ms"]),
            fmt(&report["aggregate"]["p99_ms"]),
            fmt(&report["aggregate"]["failure_rate"]
                .as_f64()
                .map(|rate| json!(rate * 100.0))
                .unwrap_or(Value::Null)),
            if report["passed"] == true {
                "PASS"
            } else {
                "FAIL"
            }
        )
    };
    let mut text = format!(
        "# Benchmark {title}\n\n{table}\n\nThresholds: p95 < 500 ms, p99 < 1,000 ms, HTTP errors < 1%.\n{} requests over {} seconds, including ramp and drain.\n\n| Endpoint | Requests/s | Successful/s | Failed/s | p95 ms | p99 ms |\n|---|---:|---:|---:|---:|---:|\n",
        report["aggregate"]["requests"],
        fmt(&report["duration_s"])
    );
    text.push_str(&format!(
        "| All endpoints | {} | {} | {} | {} | {} |\n",
        fmt(&report["request_rates"]["requests_per_second"]),
        fmt(&report["request_rates"]["successful_requests_per_second"]),
        fmt(&report["request_rates"]["failed_requests_per_second"]),
        fmt(&report["aggregate"]["p95_ms"]),
        fmt(&report["aggregate"]["p99_ms"])
    ));
    if let Some(endpoints) = report["endpoints"].as_object() {
        for (name, endpoint) in endpoints {
            text.push_str(&format!(
                "| {name} | {} | {} | {} | {} | {} |\n",
                fmt(&endpoint["requests_per_second"]),
                fmt(&endpoint["successful_requests_per_second"]),
                fmt(&endpoint["failed_requests_per_second"]),
                fmt(&endpoint["p95_ms"]),
                fmt(&endpoint["p99_ms"])
            ));
        }
    }
    text.push_str(&format!(
        "\nFull-load hold: {} s / {} s required.\n",
        fmt(&report["common_hold_s"]),
        fmt(&report["required_hold_s"])
    ));
    text.push_str(&format!(
        "Source ports: {}–{}; theoretical tuple capacity {} before occupied ports and TIME_WAIT.\n",
        report["source_port_allocation"]["first"],
        report["source_port_allocation"]["last"],
        report["source_port_allocation"]["tuple_capacity"]
    ));
    if !compact {
        if let Some(endpoints) = report["endpoints"].as_object() {
            for (endpoint, details) in endpoints {
                if details["failed"].as_u64().unwrap_or(0) > 0
                    && details["errors"]["counts"]
                        .as_object()
                        .is_none_or(|counts| counts.is_empty())
                {
                    text.push_str(&format!(
                    "\nRequest errors — {}: {} failed requests; error details are unavailable in this legacy report.\n",
                    markdown_text(endpoint), details["failed"]
                ));
                    continue;
                }
                render_error_details(
                    &mut text,
                    &format!("Request errors — {}", markdown_text(endpoint)),
                    &details["errors"],
                );
            }
        }
        render_error_details(
            &mut text,
            "Runtime errors",
            &report["runtime_error_details"],
        );
    }
    let connections = &report["connections"];
    text.push_str(&format!("\n| Connection attempts/s | Established/s | Failed/s | Cancelled |\n|---:|---:|---:|---:|\n| {} | {} | {} | {} |\n",
        fmt(&connections["attempts_per_second"]), fmt(&connections["connections_per_second"]), fmt(&connections["failed_per_second"]), connections["cancelled"]));
    text.push_str(&format!("Connection attempts: {} ({} per second); {} established, {} failed, {} cancelled.\n\n| Setup outcome | Samples | Average ms | Median ms | p95 ms | p99 ms | Maximum ms |\n|---|---:|---:|---:|---:|---:|---:|\n",
        connections["attempts"], fmt(&connections["attempts_per_second"]), connections["established"], connections["failed"], connections["cancelled"]));
    for (name, key) in [
        ("All", "setup_duration_ms"),
        ("Successful", "successful_setup_duration_ms"),
        ("Failed (including cancelled)", "failed_setup_duration_ms"),
    ] {
        let setup = &connections[key];
        text.push_str(&format!(
            "| {name} | {} | {} | {} | {} | {} | {} |\n",
            setup["samples"],
            fmt(&setup["avg_ms"]),
            fmt(&setup["med_ms"]),
            fmt(&setup["p95_ms"]),
            fmt(&setup["p99_ms"]),
            fmt(&setup["max_ms"])
        ));
    }
    if let Some(reason) = connections["unavailable_reason"].as_str() {
        text.push_str(&format!("Connection metrics unavailable: {reason}\n"));
    }
    text.push_str("\nHost interface counters include background traffic. Packet counters may reflect offloads. Do not sum virtual links or both connection ends. Host aliases are deduplicated.\n\n| Observer(s) | Interface | RX Mbit/s | TX Mbit/s | RX packets/s | TX packets/s |\n|---|---|---:|---:|---:|---:|\n");
    if let Some(hosts) = report["network_hosts"]["hosts"].as_object() {
        for host in hosts.values() {
            let observers = host["observed_by"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            if let Some(interfaces) = host["network"].as_object() {
                for (name, network) in interfaces {
                    if compact
                        && network["receive_bytes"].as_u64() == Some(0)
                        && network["transmit_bytes"].as_u64() == Some(0)
                    {
                        continue;
                    }
                    let mbps = |key| {
                        network[key]
                            .as_f64()
                            .map(|n| json!(n / 1_000_000.0))
                            .unwrap_or(Value::Null)
                    };
                    text.push_str(&format!(
                        "| {observers} | {name} | {} | {} | {} | {} |\n",
                        fmt(&mbps("receive_bits_per_second")),
                        fmt(&mbps("transmit_bits_per_second")),
                        fmt(&network["receive_packets_per_second"]),
                        fmt(&network["transmit_packets_per_second"])
                    ));
                }
            }
        }
    }
    text.push_str("\nCPU includes all process threads; 100% is one core. Memory, descriptors and established sockets are sampled peaks.\n\n| Observer | Role / PID | CPU % | Peak RSS MiB | Peak FDs | FD soft / hard limit | Peak established sockets |\n|---|---|---:|---:|---:|---|---:|\n");
    let observations = report["generator_telemetry"]
        .as_object()
        .into_iter()
        .flat_map(|m| m.iter())
        .map(|(name, value)| (name.as_str(), value))
        .chain(std::iter::once(("target", &report["target_telemetry"])));
    for (observer, value) in observations {
        if let Some(reason) = value["unavailable_reason"].as_str() {
            text.push_str(&format!("| {observer} | {reason} | unavailable | unavailable | unavailable | unavailable | unavailable |\n"));
        }
        if let Some(roles) = value["processes"].as_object() {
            for (role, values) in roles {
                if compact && role == "observer" {
                    continue;
                }
                if let Some(reason) = values["unavailable_reason"].as_str() {
                    text.push_str(&format!("| {observer} | {role}: {reason} | unavailable | unavailable | unavailable | unavailable | unavailable |\n"));
                }
                for process in values["processes"].as_array().into_iter().flatten() {
                    let rss = process["peak_resident_bytes"]
                        .as_f64()
                        .map(|n| json!(n / 1048576.0))
                        .unwrap_or(Value::Null);
                    text.push_str(&format!(
                        "| {observer} | {role} / {} | {} | {} | {} | {} / {} | {} |\n",
                        process["pid"],
                        fmt(&process["cpu_percent_of_one_core"]),
                        fmt(&rss),
                        process["peak_open_descriptors"],
                        process["descriptor_limits"]["soft"]
                            .as_str()
                            .unwrap_or("unavailable"),
                        process["descriptor_limits"]["hard"]
                            .as_str()
                            .unwrap_or("unavailable"),
                        process["peak_established_sockets"]
                    ));
                }
            }
        }
    }
    if !compact {
        text.push_str("\nKernel counters are observations; conntrack count/max alone do not establish that these connections are tracked or that conntrack caused a limit.\n\n| Observer | Kernel setting | Value |\n|---|---|---|\n");
        for (observer, value) in report["generator_telemetry"]
            .as_object()
            .into_iter()
            .flat_map(|m| m.iter())
            .map(|(n, v)| (n.as_str(), v))
            .chain(std::iter::once(("target", &report["target_telemetry"])))
        {
            if let Some(limits) = value["limits"].as_object() {
                for (name, limit) in limits {
                    let display = limit["value"]
                        .as_str()
                        .or_else(|| limit["unavailable_reason"].as_str())
                        .unwrap_or("unavailable");
                    text.push_str(&format!(
                        "| {observer} | {name} | {} |\n",
                        markdown_text(display)
                    ));
                }
            }
            text.push_str(&format!("| {observer} | peak namespace established sockets | {} |\n| {observer} | peak conntrack count | {} |\n", value["peak_namespace_established_sockets"], value["peak_conntrack_count"]));
        }
    }
    text.push_str("\nExact latency/connection samples, resource time series, kernel limits and availability details are retained alongside aggregate.json.\n");
    text
}

fn markdown_text(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            '\n' | '\r' | '\t' => " ".into(),
            '&' => "&amp;".into(),
            '<' => "&lt;".into(),
            '>' => "&gt;".into(),
            '|' => "&#124;".into(),
            '\\' => "&#92;".into(),
            '*' => "&#42;".into(),
            '_' => "&#95;".into(),
            '[' => "&#91;".into(),
            ']' => "&#93;".into(),
            '#' => "&#35;".into(),
            '`' => "&#96;".into(),
            ch if ch.is_control() => " ".into(),
            ch => ch.to_string(),
        })
        .collect::<String>()
}

fn render_error_details(text: &mut String, title: &str, value: &Value) {
    let Some(counts) = value["counts"]
        .as_object()
        .filter(|counts| !counts.is_empty())
    else {
        return;
    };
    text.push_str(&format!("\n{title}:\n"));
    for (kind, count) in counts {
        text.push_str(&format!("- {}: {}\n", markdown_text(kind), count));
    }
    if let Some(examples) = value["examples"].as_array() {
        for example in examples.iter().take(5) {
            let worker = example["worker"].as_str().map(markdown_text);
            let endpoint = example["endpoint"]
                .as_str()
                .map(markdown_text)
                .unwrap_or_default();
            let path = example["path"]
                .as_str()
                .map(markdown_text)
                .unwrap_or_default();
            let kind = example["kind"]
                .as_str()
                .map(markdown_text)
                .unwrap_or_default();
            let reason = example["reason"]
                .as_str()
                .map(markdown_text)
                .unwrap_or_default();
            let status = example["status_code"]
                .as_u64()
                .map(|n| n.to_string())
                .unwrap_or_else(|| "no HTTP status".into());
            text.push_str(&format!(
                "- {}{} {} {}: {} ({})\n",
                worker
                    .as_deref()
                    .map(|name| format!("{name}: "))
                    .unwrap_or_default(),
                endpoint,
                path,
                status,
                kind,
                reason
            ));
            if let Some(body) = example["body_excerpt"].as_str() {
                let encoded =
                    serde_json::to_string(body).unwrap_or_else(|_| "\"unavailable\"".into());
                text.push_str(&format!(
                    "    {}{}\n",
                    markdown_text(&encoded),
                    if example["body_truncated"] == true {
                        " [truncated]"
                    } else {
                        ""
                    }
                ));
            }
        }
        let hidden = examples.len().saturating_sub(5);
        if hidden > 0 {
            text.push_str(&format!("- {hidden} stored examples not shown.\n"));
        }
    }
    if let Some(omitted) = value["omitted"].as_u64().filter(|n| *n > 0) {
        text.push_str(&format!("- {omitted} additional examples omitted.\n"));
    }
}

pub(super) fn run(args: &[String], cancelled: Arc<AtomicBool>) -> Result<u8> {
    let cfg = config::parse(args)?;
    if cfg.help {
        print!("{}", config::HELP);
        return Ok(0);
    }
    let username = cfg.user.clone();
    let options = cfg.diagnostics.clone();
    let binary = env::current_exe()?;
    if options.iter().any(|arg| {
        matches!(
            arg.as_str(),
            "--version" | "-V" | "--list" | "-l" | "--scenarios-list"
        )
    }) {
        return Ok(Command::new(binary)
            .args(options)
            .status()?
            .code()
            .unwrap_or(2) as u8);
    }
    if cfg.score {
        return super::scoring::run(&cfg, args, cancelled);
    }
    let ips = parse_source_ips(&cfg.source_ips)?;
    let source_ip_count = ips.len().max(1);
    let users = cfg.users;
    let targets = destinations::urls(&cfg.base_url, &cfg.destination_ports)?;
    let destination_ports = cfg
        .destination_ports
        .iter()
        .map(u16::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let tokens = fs::canonicalize(&cfg.tokens)?;
    load_tokens(&tokens)?;
    let output = cfg.output.clone().unwrap_or_else(|| {
        PathBuf::from(format!(
            "results/run-{}-{}",
            Utc::now().timestamp_millis(),
            std::process::id()
        ))
    });
    fs::create_dir_all(
        output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?;
    // Refuse reuse, so stale reports can never make a failed run pass.
    fs::create_dir(&output)?;
    let output = fs::canonicalize(output)?;
    println!("Results: {}", output.display());
    fs::write(
        output.join("aggregate.json"),
        "{\"completed\":false,\"passed\":false,\"status\":\"running\"}\n",
    )?;
    let mut target_unavailable = None;
    let mut monitor = None;
    let mut workers = Vec::new();
    if let Some(user) = &username {
        if user.is_empty()
            || user.starts_with('-')
            || !user
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "_.-".contains(c))
        {
            return Err("invalid SSH username".into());
        }
        if ips.iter().collect::<BTreeSet<_>>().len() != ips.len() {
            return Err("SSH generator addresses must be unique".into());
        }
        for (index, (ip, (count, offset))) in
            ips.iter().zip(split_users(users, ips.len())?).enumerate()
        {
            let name = format!("host-{:03}-{}", index + 1, ip.to_string().replace(':', "_"));
            let dir = output.join(&name);
            fs::create_dir(&dir)?;
            let remote_dir = format!(
                "/tmp/twelve-dollar-benchmark-{}-{}-{}",
                Utc::now().timestamp_millis(),
                std::process::id(),
                index
            );
            let mut worker = Worker {
                name,
                users: count,
                user_offset: offset,
                remote_dir: Some(remote_dir.clone()),
                exit_code: None,
                error: None,
            };
            let upload = (|| -> Result<()> {
                if cancelled.load(Ordering::Relaxed) {
                    return Err("interrupted".into());
                }
                checked(
                    ssh(user, &ip.to_string()).arg(format!(
                        "umask 077; mkdir -- {} && mkdir -- {}",
                        quote(&remote_dir),
                        quote(&format!("{remote_dir}/results"))
                    )),
                    &cancelled,
                )?;
                checked(
                    scp().arg("--").arg(&binary).arg(destination(
                        user,
                        &ip.to_string(),
                        &format!("{remote_dir}/twelve-dollar-benchmark"),
                    )),
                    &cancelled,
                )?;
                checked(
                    scp().arg("--").arg(&tokens).arg(destination(
                        user,
                        &ip.to_string(),
                        &format!("{remote_dir}/tokens.json"),
                    )),
                    &cancelled,
                )?;
                // Check the copied executable before launching any load on any host.
                checked(
                    ssh(user, &ip.to_string()).arg(format!(
                        "{} --version",
                        quote(&format!("{remote_dir}/twelve-dollar-benchmark"))
                    )),
                    &cancelled,
                )?;
                Ok(())
            })();
            if let Err(error) = upload {
                worker.error = Some(error.to_string());
            }
            workers.push(worker);
        }
        // Upload all hosts first; a failed copy must not start a partial load.
        if workers.iter().all(|worker| worker.error.is_none()) {
            let controls: Vec<_> = workers
                .iter()
                .zip(&ips)
                .map(|(worker, ip)| (ip.to_string(), worker.remote_dir.clone().unwrap()))
                .collect();
            let mut joins = Vec::new();
            for (worker, ip) in workers.into_iter().zip(ips) {
                let output = output.clone();
                let user = user.clone();
                let options = options.clone();
                let cancelled = cancelled.clone();
                let base_url = cfg.base_url.clone();
                let destination_ports = destination_ports.clone();
                let ramp = cfg.ramp_up.clone();
                let duration = cfg.duration.clone();
                let sample_interval = cfg.sample_interval.as_secs_f64();
                joins.push(thread::spawn(move || {
                    let mut worker = worker;
                    let dir = output.join(&worker.name);
                    let remote_dir = worker.remote_dir.as_ref().unwrap();
                    let result = (|| -> Result<()> {
                        let assignments = [
                            format!("BASE_URL={base_url}"),
                            format!("DESTINATION_PORTS={destination_ports}"),
                            format!("VUS={}", worker.users),
                            format!("RAMP_UP={ramp}"),
                            format!("DURATION={duration}"),
                            format!("BENCH_USER_OFFSET={}", worker.user_offset),
                            "TOKENS=../tokens.json".into(),
                            "SUMMARY=summary.json".into(),
                            "BENCH_SAMPLES=samples.json".into(),
                            "BENCH_TELEMETRY=telemetry.jsonl".into(),
                            format!("BENCH_SAMPLE_INTERVAL={sample_interval}"),
                            "SOURCE_IPS=".into(),
                        ];
                        let command = format!(
                            "cd {} || exit 2; trap '' HUP; exec env {} ../twelve-dollar-benchmark --supervise ../cancel {}",
                            quote(&format!("{remote_dir}/results")),
                            assignments
                                .iter()
                                .map(|v| quote(v))
                                .collect::<Vec<_>>()
                                .join(" "),
                            options
                                .iter()
                                .map(|v| quote(v))
                                .collect::<Vec<_>>()
                                .join(" ")
                        );
                        let log = File::create(dir.join("output.log"))?;
                        let status = execute(
                            ssh(&user, &ip.to_string())
                                .arg(command)
                                .stdout(Stdio::from(log.try_clone()?))
                                .stderr(Stdio::from(log)),
                            &cancelled,
                        )?;
                        worker.exit_code = status.code();
                        if cancelled.load(Ordering::Relaxed) || !matches!(worker.exit_code, Some(0 | 1)) {
                            checked(ssh(&user, &ip.to_string()).arg(format!("{} --stop-supervised {}", quote(&format!("{remote_dir}/twelve-dollar-benchmark")), quote(&format!("{remote_dir}/cancel")))), &AtomicBool::new(false))?;
                        }

                        // Collect reports even when the benchmark failed a threshold.
                        let remote = destination(&user, &ip.to_string(), remote_dir);
                        checked(
                            scp()
                                .arg("-r")
                                .arg("--")
                                .arg(format!("{remote}/results/."))
                                .arg(&dir),
                            &AtomicBool::new(false),
                        )?;
                        if !matches!(worker.exit_code, Some(0 | 1)) {
                            return Err(format!("remote execution failed: {status}").into());
                        }
                        Ok(())
                    })();
                    if let Err(error) = result {
                        worker.error = Some(error.to_string());
                    }
                    worker
                }));
            }
            let coordination = (|| -> Result<()> {
                for (ip, dir) in &controls {
                    checked(
                        ssh(user, ip).arg(format!(
                            "{} --ready-supervised {}",
                            quote(&format!("{dir}/twelve-dollar-benchmark")),
                            quote(&format!("{dir}/cancel"))
                        )),
                        &cancelled,
                    )?;
                }
                (monitor, target_unavailable) = prepare_monitor(&cfg, &binary, &output, &cancelled);
                // All SSH sessions are ready before any generator starts.
                let start_at = (Utc::now().timestamp_millis() + 5000).to_string();
                start_monitor(&mut monitor, &mut target_unavailable, &start_at, &cancelled);
                let publications = thread::scope(|scope| {
                    let mut handles = Vec::new();
                    for (ip, dir) in &controls {
                        let start_at = &start_at;
                        let cancelled = &cancelled;
                        handles.push(scope.spawn(move || {
                            checked(
                                ssh(user, ip).arg(format!(
                                    "{} --start-supervised {} {}",
                                    quote(&format!("{dir}/twelve-dollar-benchmark")),
                                    quote(&format!("{dir}/cancel")),
                                    quote(start_at)
                                )),
                                cancelled,
                            )
                            .map_err(|error| error.to_string())
                        }));
                    }
                    handles
                        .into_iter()
                        .map(|handle| {
                            handle
                                .join()
                                .unwrap_or_else(|_| Err("start publication panicked".into()))
                        })
                        .collect::<Vec<_>>()
                });
                for result in publications {
                    result?;
                }
                Ok(())
            })();
            if let Err(error) = coordination {
                eprintln!("coordinated start failed: {error}");
                cancelled.store(true, Ordering::Relaxed);
            }
            workers = joins
                .into_iter()
                .map(|join| join.join().expect("worker thread panicked"))
                .collect();
        }
    } else {
        let dir = output.join("local");
        fs::create_dir(&dir)?;
        let log = File::create(dir.join("output.log"))?;
        (monitor, target_unavailable) = prepare_monitor(&cfg, &binary, &output, &cancelled);
        let start_at = (Utc::now().timestamp_millis() + 1000).to_string();
        start_monitor(&mut monitor, &mut target_unavailable, &start_at, &cancelled);
        let status = execute(
            Command::new(&binary)
                .args(options)
                .current_dir(&dir)
                .env("TOKENS", tokens)
                .env("BASE_URL", &cfg.base_url)
                .env("DESTINATION_PORTS", &destination_ports)
                .env("VUS", users.to_string())
                .env("RAMP_UP", &cfg.ramp_up)
                .env("DURATION", &cfg.duration)
                .env("SOURCE_IPS", &cfg.source_ips)
                .env(
                    "BENCH_SAMPLE_INTERVAL",
                    cfg.sample_interval.as_secs_f64().to_string(),
                )
                .env("SUMMARY", dir.join("summary.json"))
                .env("BENCH_SAMPLES", dir.join("samples.json"))
                .env("BENCH_TELEMETRY", dir.join("telemetry.jsonl"))
                .env("BENCH_USER_OFFSET", "0")
                .env("BENCH_START_AT", start_at)
                .stdout(Stdio::from(log.try_clone()?))
                .stderr(Stdio::from(log)),
            &cancelled,
        );
        let (exit_code, error) = match status {
            Ok(status) => (
                status.code(),
                if matches!(status.code(), Some(0 | 1)) {
                    None
                } else {
                    Some(format!("local execution failed: {status}"))
                },
            ),
            Err(error) => (None, Some(error.to_string())),
        };
        workers.push(Worker {
            name: "local".into(),
            users,
            user_offset: 0,
            remote_dir: None,
            exit_code,
            error,
        });
    }
    let target_telemetry = match monitor.as_mut() {
        Some(monitor) => monitor.finish().unwrap_or_else(|error| {
            eprintln!("Target telemetry unavailable: {error}");
            json!({"unavailable_reason": error.to_string(), "shutdown_unconfirmed": true})
        }),
        None => {
            json!({"unavailable_reason": target_unavailable.unwrap_or_else(|| "target observer unavailable".into())})
        }
    };
    let mut aggregate = collect(&output, &mut workers);
    aggregate["network_hosts"] =
        network_hosts(&aggregate["generator_telemetry"], &target_telemetry);
    aggregate["target_telemetry"] = target_telemetry;
    aggregate["implementation"] = json!(cfg.label.as_deref().unwrap_or(&cfg.base_url));
    let hold = (|| -> Result<Value> {
        let start = serde_json::from_value(aggregate["hold_started_at"].clone())?;
        let finish = serde_json::from_value(aggregate["hold_finished_at"].clone())?;
        let mut requests_per_second = 0.0;
        for worker in &workers {
            let hold = twelve_dollar_benchmark::telemetry::summarize_hold(
                &output.join(&worker.name).join("telemetry.jsonl"),
                start,
                finish,
            )?;
            requests_per_second += hold["requests_per_second"]
                .as_f64()
                .ok_or("hold request counters unavailable")?;
        }
        let mut target = twelve_dollar_benchmark::telemetry::summarize_hold(
            &output.join("target/telemetry.jsonl"),
            start,
            finish,
        )
        .unwrap_or_else(|error| json!({"unavailable_reason": error.to_string()}));
        target["requests_per_second"] = json!(requests_per_second);
        Ok(target)
    })();
    aggregate["hold"] =
        hold.unwrap_or_else(|error| json!({"unavailable_reason": error.to_string()}));

    aggregate["destination_ports"] = json!(cfg.destination_ports);
    aggregate["requested_users"] = json!(cfg.users);
    aggregate["source_port_allocation"] = json!({"first": 1024, "last": 65535,
        "policy": "explicit ports, shared per source/destination counters; no host sysctl changes",
        "tuple_capacity": 64512 * cfg.destination_ports.len() * if cfg.user.is_none() { source_ip_count } else { workers.len() },
        "capacity_scope": "upper bound before existing sockets and TIME_WAIT; assumes one source address per SSH generator"});
    aggregate["configured_hosts"] =
        json!(targets.iter().map(reqwest::Url::as_str).collect::<Vec<_>>());
    if cancelled.load(Ordering::Relaxed) {
        aggregate["completed"] = json!(false);
        aggregate["passed"] = json!(false);
        aggregate["interrupted"] = json!(true);
    }
    fs::write(
        output.join("aggregate.json"),
        format!("{}\n", serde_json::to_string_pretty(&aggregate)?),
    )?;
    fs::write(output.join("report.md"), render_report(&aggregate, false))?;
    // Keep the previous SUMMARY destination available alongside the run directory.
    if let Some(legacy) = env::var("SUMMARY").ok().filter(|s| !s.is_empty()) {
        let path = Path::new(&legacy);
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        fs::write(
            path,
            format!("{}\n", serde_json::to_string_pretty(&aggregate)?),
        )?;
    }
    print!("{}", render_report(&aggregate, true));
    println!("Full report: {}", output.join("report.md").display());
    println!(
        "{} — {}",
        if aggregate["passed"] == true {
            "PASS"
        } else {
            "FAIL"
        },
        output.join("aggregate.json").display()
    );
    if workers.iter().any(|worker| worker.error.is_some()) {
        return Ok(2);
    }
    Ok(if aggregate["passed"] == true { 0 } else { 1 })
}

#[cfg(test)]
mod tests {
    use super::super::errors::example;
    use super::*;

    fn fixture(name: &str, samples: Vec<f64>, offset: i64) -> (PathBuf, Worker) {
        let dir = env::temp_dir().join(format!(
            "twelve-dollar-benchmark-aggregate-{name}-{}",
            std::process::id()
        ));
        fs::create_dir_all(dir.join(name)).unwrap();
        let mut raw: [Samples; 4] = std::array::from_fn(|_| Samples::default());
        raw[0].latency_ms = samples;
        let summary = raw[0].summary();
        let start = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
            + chrono::TimeDelta::seconds(offset);
        fs::write(
            dir.join(name).join("summary.json"),
            json!({
                "maximum_users": 1, "total_users": 1,
                "started_at": start, "finished_at": start + chrono::TimeDelta::seconds(10),
                "hold_started_at": start, "hold_finished_at": start + chrono::TimeDelta::seconds(5),
                "planned_hold_s": 5, "hosts": ["http://127.0.0.1:3000"], "source_ips": [],
                "runtime_errors": 0, "completed": true, "aggregate": summary,
            })
            .to_string(),
        )
        .unwrap();
        serde_json::to_writer(
            File::create(dir.join(name).join("samples.json")).unwrap(),
            &raw,
        )
        .unwrap();
        (
            dir,
            Worker {
                name: name.into(),
                users: 1,
                user_offset: 0,
                remote_dir: Some("/tmp/fixture".into()),
                exit_code: Some(1),
                error: None,
            },
        )
    }

    #[test]
    fn combined_percentiles_weight_samples_and_require_overlapping_hold() {
        let (dir, a) = fixture("weighted-a", vec![10.0; 99], 0);
        let (other, b) = fixture("weighted-b", vec![1000.0], 0);
        fs::rename(other.join(&b.name), dir.join(&b.name)).unwrap();
        let mut workers = vec![a, b];
        let result = collect(&dir, &mut workers);
        assert_eq!(result["passed"], true); // the slow host's own p99 fails, combined p99 passes
        assert!((result["aggregate"]["p99_ms"].as_f64().unwrap() - 19.9).abs() < 1e-9);
        assert_eq!(result["aggregate"]["requests"], 100);
        assert_eq!(result["duration_s"], 10.0);
        assert_eq!(result["common_hold_s"], 5.0);
        let (late, c) = fixture("weighted-c", vec![10.0], 20);
        fs::rename(late.join(&c.name), dir.join(&c.name)).unwrap();
        workers.push(c);
        let result = collect(&dir, &mut workers);
        assert_eq!(result["passed"], false);
        assert_eq!(result["completed"], false);
        assert_eq!(result["duration_s"], 30.0);
        fs::remove_dir_all(dir).unwrap();
        fs::remove_dir(other).unwrap();
        fs::remove_dir(late).unwrap();
    }

    #[test]
    fn missing_or_inconsistent_worker_data_cannot_pass() {
        let (dir, mut worker) = fixture("invalid", vec![10.0], 0);
        worker.users = 2;
        let result = collect(&dir, std::slice::from_mut(&mut worker));
        assert_eq!(result["passed"], false);
        assert!(worker.error.is_some());
        worker.users = 1;
        worker.error = None;
        fs::remove_file(dir.join(&worker.name).join("samples.json")).unwrap();
        let result = collect(&dir, std::slice::from_mut(&mut worker));
        assert_eq!(result["completed"], false);
        assert!(worker.error.is_some());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn worker_error_details_merge_counts_and_keep_bounded_host_attribution() {
        let (dir, first) = fixture("errors-a", vec![10.0], 0);
        let (other, second) = fixture("errors-b", vec![11.0], 0);
        fs::rename(other.join(&second.name), dir.join(&second.name)).unwrap();
        for worker in [&first, &second] {
            let path = dir.join(&worker.name).join("samples.json");
            let mut raw: [Samples; 4] =
                serde_json::from_reader(File::open(&path).unwrap()).unwrap();
            raw[0].latency_ms = vec![10.0; 101];
            raw[0].failed = 101;
            for _ in 0..101 {
                raw[0].errors.record(example(
                    "GET /feed",
                    "/feed",
                    Some(503),
                    "http",
                    "queue full",
                    Some(b"busy"),
                    worker.user_offset,
                    "http://fixture.test:3000",
                    Some("192.0.2.1".into()),
                ));
            }
            let mut aggregate = Samples {
                latency_ms: vec![10.0; 101],
                failed: 101,
                ..Samples::default()
            };
            aggregate.errors.merge(&raw[0].errors);
            let summary_path = dir.join(&worker.name).join("summary.json");
            let mut summary: Value =
                serde_json::from_reader(File::open(&summary_path).unwrap()).unwrap();
            summary["aggregate"] = serde_json::to_value(aggregate.summary()).unwrap();
            serde_json::to_writer(File::create(summary_path).unwrap(), &summary).unwrap();
            serde_json::to_writer(File::create(path).unwrap(), &raw).unwrap();
        }

        let mut workers = vec![first, second];
        let result = collect(&dir, &mut workers);
        let errors = &result["endpoints"]["GET /feed"]["errors"];
        assert_eq!(errors["counts"]["kind:http"], 202);
        assert_eq!(errors["counts"]["status:503"], 202);
        assert_eq!(errors["examples"].as_array().unwrap().len(), 100);
        assert_eq!(errors["omitted"], 102);
        assert_eq!(result["aggregate"]["errors"]["counts"]["status:503"], 202);
        assert!(
            errors["examples"]
                .as_array()
                .unwrap()
                .iter()
                .all(|e| e["worker"].is_string())
        );
        assert_eq!(result["aggregate"]["failed"], 202);
        fs::remove_dir_all(dir).unwrap();
        fs::remove_dir_all(other).unwrap();
    }
    #[test]
    fn total_users_split_evenly_with_disjoint_token_offsets() {
        assert_eq!(split_users(10, 3).unwrap(), [(4, 0), (3, 4), (3, 7)]);
        assert!(split_users(2, 3).is_err());
        assert!(split_users(100, 0).is_err());
    }
    #[test]
    fn remote_arguments_are_posix_quoted_and_ipv6_scp_is_bracketed() {
        assert_eq!(quote("a'b $(id)"), "'a'\\''b $(id)'");
        assert_eq!(
            destination("user", "::1", "/tmp/run"),
            "user@[::1]:/tmp/run"
        );
    }

    #[test]
    fn stderr_inherited_by_descendants_does_not_block_command_completion() {
        let started = Instant::now();
        checked(
            Command::new("sh").args(["-c", "sleep 5 >&2 &"]),
            &AtomicBool::new(false),
        )
        .unwrap();
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    #[test]
    fn cancelled_control_children_are_stopped_without_generator_grace() {
        let mut child = Command::new("sh")
            .args(["-c", "exec sleep 30"])
            .spawn()
            .unwrap();
        let started = Instant::now();
        let status = wait_child(
            &mut child,
            &AtomicBool::new(true),
            Some(Duration::from_secs(120)),
        )
        .unwrap();
        assert!(!status.success());
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
