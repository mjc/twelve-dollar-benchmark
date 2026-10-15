mod host;
mod summary;
#[cfg(test)]
mod tests;

use host::{
    Interface, Process, SystemMemory, discover_roles, interfaces, limits, process,
    service_resources, socket_inodes, system_memory, tcp_counters, ticks_per_second,
};
pub use summary::summarize_hold;
use summary::{interval_report, summarize};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    error::Error,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};
pub type Result<T> = std::result::Result<T, Box<dyn Error>>;
pub type Roles = BTreeMap<String, Vec<u32>>;

#[derive(Clone, Serialize, Deserialize)]
struct Point {
    timestamp: String,
    elapsed_s: f64,
    host_boot_id: Option<String>,
    network_namespace: Option<String>,
    sample_interval_s: f64,
    interfaces: Option<BTreeMap<String, Interface>>,
    processes: BTreeMap<String, Vec<Process>>,
    limits: Value,
    activity: Value,
    #[serde(default)]
    tcp: Option<BTreeMap<String, u64>>,
    #[serde(default)]
    interval_rates: Value,
    #[serde(default)]
    service: Value,
    #[serde(default)]
    system_memory: Option<SystemMemory>,
    errors: Vec<String>,
}

pub struct Sampler {
    stop: mpsc::Sender<()>,
    thread: Option<thread::JoinHandle<std::result::Result<(), String>>>,
    path: PathBuf,
}
impl Sampler {
    /// Start periodic host and process sampling and wait for the baseline sample.
    ///
    /// # Errors
    /// Returns an error if the output file cannot be created or the baseline fails.
    pub fn start(
        path: PathBuf,
        roles: Roles,
        interval: Duration,
        activity: Arc<dyn Fn() -> Value + Send + Sync>,
    ) -> Result<Self> {
        let file = File::create(&path)?;
        let (stop, receiver) = mpsc::channel();
        let (ready, received_ready) = mpsc::sync_channel(1);
        let thread = thread::spawn(move || {
            let mut ready = Some(ready);
            // sysconf accepts these constant selectors and has no pointer arguments.
            let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
            let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
            if ticks <= 0 || page <= 0 {
                return Err("unable to read kernel tick/page sizes".into());
            }
            let ticks = ticks_per_second(ticks);
            let page = u64::try_from(page).map_err(|_| "invalid kernel page size".to_owned())?;
            let mut writer = BufWriter::new(file);
            let started = Instant::now();
            let mut identities = BTreeMap::new();
            let mut final_sample = false;
            let mut previous: Option<Point> = None;
            loop {
                let mut errors = Vec::new();
                let interfaces = fs::read_to_string("/proc/net/dev")
                    .map_err(|e| e.to_string())
                    .and_then(|text| interfaces(&text).map_err(|e| e.to_string()))
                    .map_err(|e| errors.push(format!("network: {e}")))
                    .ok();
                let mut processes = BTreeMap::new();
                let sockets = socket_inodes();
                for (role, pids) in &roles {
                    let mut data = Vec::new();
                    for pid in pids {
                        match process(*pid, ticks, page, &sockets) {
                            Ok(data_point) => {
                                let original =
                                    identities.entry(*pid).or_insert(data_point.start_ticks);
                                if *original == data_point.start_ticks {
                                    data.push(data_point);
                                } else {
                                    errors.push(format!(
                                        "{role} PID {pid} was reused; later process excluded"
                                    ));
                                }
                            }
                            Err(error) => errors.push(format!("{role} PID {pid}: {error}")),
                        }
                    }
                    processes.insert(role.clone(), data);
                }
                let tcp = fs::read_to_string("/proc/net/snmp")
                    .map_err(|e| e.to_string())
                    .and_then(|text| tcp_counters(&text).map_err(|e| e.to_string()))
                    .map_err(|e| errors.push(format!("TCP counters: {e}")))
                    .ok();
                let system_memory = system_memory()
                    .map_err(|e| errors.push(format!("system memory: {e}")))
                    .ok();
                let mut point = Point {
                    timestamp: Utc::now().to_rfc3339(),
                    elapsed_s: started.elapsed().as_secs_f64(),
                    host_boot_id: fs::read_to_string("/proc/sys/kernel/random/boot_id")
                        .ok()
                        .map(|s| s.trim().to_owned()),
                    network_namespace: fs::read_link("/proc/self/ns/net")
                        .ok()
                        .map(|s| s.to_string_lossy().into_owned()),
                    sample_interval_s: interval.as_secs_f64(),
                    interfaces,
                    processes,
                    limits: limits(),
                    activity: activity(),
                    service: service_resources(&roles),
                    system_memory,
                    tcp,
                    interval_rates: Value::Null,
                    errors,
                };
                if let Some(begin) = &previous {
                    point.interval_rates = interval_report(begin, &point);
                }
                serde_json::to_writer(&mut writer, &point).map_err(|e| e.to_string())?;
                writer.write_all(b"\n").map_err(|e| e.to_string())?;
                writer.flush().map_err(|e| e.to_string())?;
                previous = Some(point);
                if let Some(sender) = ready.take() {
                    let _ = sender.send(());
                }
                if final_sample {
                    break;
                }
                match receiver.recv_timeout(interval) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => final_sample = true,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
            // A final point captures counters since the last regular sample.
            // Stop is requested after native requests finish; the summary uses the last available interval.
            Ok(())
        });
        received_ready
            .recv()
            .map_err(|_| "telemetry sampler failed before its baseline")?;
        Ok(Self {
            stop,
            thread: Some(thread),
            path,
        })
    }
    /// Stop sampling and summarize the collected measurements.
    ///
    /// # Errors
    /// Returns an error if the sampler thread or summary data cannot be read.
    ///
    pub fn finish(mut self) -> Result<Value> {
        let _ = self.stop.send(());
        let handle = self
            .thread
            .take()
            .ok_or("telemetry sampler thread was already joined")?;
        let result = handle.join().map_err(|_| "telemetry sampler panicked")?;
        result.map_err(|e| -> Box<dyn Error> { e.into() })?;
        summarize(&self.path)
    }
}
impl Drop for Sampler {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

/// Run the target-side telemetry observer until its control lease ends.
///
/// # Errors
/// Returns an error if telemetry sampling or summary output fails.
pub fn observe(
    control: &Path,
    application_pids: Vec<u32>,
    proxy_pids: Vec<u32>,
    ports: &[u16],
    interval: Duration,
) -> Result<u8> {
    let discovery = discover_roles(ports);
    let discovery_error = discovery.as_ref().err().map(ToString::to_string);
    let mut roles = discovery.unwrap_or_default();
    if !application_pids.is_empty() {
        roles.insert("application".into(), application_pids);
    }
    if !proxy_pids.is_empty() {
        roles
            .entry("application".into())
            .or_default()
            .retain(|pid| !proxy_pids.contains(pid));
        roles.insert("proxy".into(), proxy_pids);
    }
    roles.entry("application".into()).or_default();
    roles.entry("proxy".into()).or_default();
    roles.insert("observer".into(), vec![std::process::id()]);
    let result = (|| {
        let lease_seconds: f64 = crate::setting("BENCH_OBSERVER_LEASE_SECONDS", "120").parse()?;
        if !lease_seconds.is_finite() || lease_seconds <= 0.0 {
            return Err("invalid observer lease duration".into());
        }
        let lease_duration = Duration::try_from_secs_f64(lease_seconds)?;
        let observer_started = Instant::now();
        let start_at = crate::control::wait_start(control)?;
        while Utc::now().timestamp_millis() < start_at {
            if control.exists() {
                return Err("target observation cancelled before start".into());
            }
            if observer_lease_expired(control, observer_started, lease_duration) {
                save_observer_summary(&json!({
                    "stopped_by_lease_timeout": true,
                    "observer_lease_seconds": lease_seconds,
                    "unavailable_reason": "coordinator lease expired before the coordinated start"
                }))?;
                return Ok(0);
            }
            thread::sleep(Duration::from_millis(10));
        }
        let sampler = Sampler::start(
            PathBuf::from("telemetry.jsonl"),
            roles,
            interval,
            Arc::new(|| Value::Null),
        )?;
        // Coordinator heartbeats renew this lease through startup, load and drain.
        // SSH disconnection alone cannot leave an observer running indefinitely.
        while !control.exists()
            && !observer_lease_expired(control, observer_started, lease_duration)
        {
            thread::sleep(Duration::from_millis(100));
        }
        let timed_out = !control.exists();
        let mut summary = sampler.finish()?;
        summary["process_discovery"] = json!({"method": "listening socket owners; recognized proxy processes and their observed local TCP backends",
            "limitation": "custom proxies, remote backends, Unix sockets and idle proxy pools may not be identifiable; optional PID overrides remain available",
            "error": discovery_error});
        if summary["processes"]["proxy"]["processes"]
            .as_array()
            .is_some_and(Vec::is_empty)
        {
            summary["processes"]["proxy"]["unavailable_reason"] =
                json!("no recognized proxy detected among target listener processes");
        }
        summary["stopped_by_lease_timeout"] = json!(timed_out);
        summary["observer_lease_seconds"] = json!(lease_seconds);
        if timed_out {
            summary["unavailable_reason"] =
                json!("coordinator lease expired before target observation stopped");
        }
        save_observer_summary(&summary)?;
        Ok(0)
    })();
    fs::write(
        control.with_extension("done"),
        if result.is_ok() {
            "observation complete\n"
        } else {
            "observation failed\n"
        },
    )?;
    result
}

fn observer_lease_expired(control: &Path, started: Instant, duration: Duration) -> bool {
    let elapsed = fs::metadata(control.with_extension("lease"))
        .and_then(|metadata| metadata.modified())
        .map_or_else(
            |_| started.elapsed(),
            |modified| modified.elapsed().unwrap_or(Duration::ZERO),
        );
    elapsed >= duration
}

fn save_observer_summary(summary: &Value) -> Result<()> {
    let mut output = BufWriter::new(File::create("telemetry-summary.json")?);
    serde_json::to_writer_pretty(&mut output, summary)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}
