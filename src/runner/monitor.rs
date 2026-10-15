use super::{
    Result,
    process::{
        checked, checked_with_timeout, destination, log_output, quote, remote_command, scp, ssh,
        upload, wait_child,
    },
};
use crate::config;
use chrono::Utc;
use serde_json::Value;
use std::{
    fs::{self, File},
    io::BufReader,
    net::IpAddr,
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

const OBSERVER_BINARY: &str = "twelve-dollar-observer";
fn remote_observer_path(directory: &str) -> String {
    format!("{directory}/{OBSERVER_BINARY}")
}

fn observer_command(
    remote: Option<&(String, String, String)>,
    binary: &Path,
    control: &Path,
    action: &str,
    timestamp: Option<&str>,
) -> Command {
    if let Some((user, host, directory)) = remote {
        let path = format!("{directory}/metrics");
        let mut args = vec![action, &path];
        args.extend(timestamp);
        remote_command(user, host, &remote_observer_path(directory), &args)
    } else {
        let mut command = Command::new(binary);
        command.arg(action).arg(control);
        command.args(timestamp);
        command
    }
}

// A target observer uses the same start barrier as the generators, then stops
// after their final responses. Network counters retain their own measured window.
pub(super) struct TargetMonitor {
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
        let url = url::Url::parse(&cfg.base_url)?;
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
                    "/tmp/twelve-dollar-benchmark-target-{}-{}",
                    Utc::now().timestamp_millis(),
                    std::process::id()
                ),
            )
        });
        let control = dir.join("metrics");
        let assignments = target_assignments(cfg);
        let observer_binary = binary.with_file_name(OBSERVER_BINARY);
        let log = File::create(dir.join("output.log"))?;
        let mut command = if let Some((user, host, remote_dir)) = &remote {
            checked(
                ssh(user, host).arg(format!("umask 077; mkdir -- {}", quote(remote_dir))),
                cancelled,
            )?;
            upload(
                user,
                host,
                &observer_binary,
                &remote_observer_path(remote_dir),
                cancelled,
            )?;
            checked(
                &mut remote_command(
                    user,
                    host,
                    &remote_observer_path(remote_dir),
                    &["--version"],
                ),
                cancelled,
            )?;
            let mut command = ssh(user, host);
            command.arg(format!(
                "cd {} || exit 2; trap '' HUP; exec env {} ./{OBSERVER_BINARY} --observe metrics",
                quote(remote_dir),
                assignments
                    .iter()
                    .map(|(name, value)| format!("{name}={}", quote(value)))
                    .collect::<Vec<_>>()
                    .join(" ")
            ));
            command
        } else {
            let mut command = Command::new(&observer_binary);
            command.current_dir(&dir).arg("--observe").arg(&control);
            for (name, value) in &assignments {
                command.env(name, value);
            }
            command
        };
        log_output(&mut command, log)?;
        let child = command.stdin(Stdio::null()).process_group(0).spawn()?;
        Ok(Self {
            child: Some(child),
            binary: observer_binary,
            dir,
            control,
            remote,
            heartbeat_cancelled: Arc::new(AtomicBool::new(false)),
            heartbeat_stop: None,
            heartbeat: None,
        })
    }
    fn control_command(&self, action: &str, timestamp: Option<&str>) -> Command {
        observer_command(
            self.remote.as_ref(),
            &self.binary,
            &self.control,
            action,
            timestamp,
        )
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
                let mut command = observer_command(
                    remote.as_ref(),
                    &binary,
                    &control,
                    "--refresh-observer",
                    None,
                );
                // Bound a stalled refresh so the next heartbeat can retry well before lease expiry.
                if let Err(error) =
                    checked_with_timeout(&mut command, &flag, Duration::from_secs(15))
                    && !flag.load(Ordering::Relaxed)
                {
                    eprintln!("Target telemetry lease refresh failed: {error}");
                }
            }
        }));
        Ok(())
    }
    pub(super) fn finish(&mut self) -> Result<Value> {
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
            let status = wait_child(&mut child, || false, Some(Duration::from_secs(20)))?;
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

fn target_assignments(cfg: &config::Config) -> [(&'static str, String); 4] {
    [
        (
            "BENCH_APPLICATION_PIDS",
            cfg.application_pids
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "BENCH_PROXY_PIDS",
            cfg.proxy_pids
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "BENCH_TARGET_PORTS",
            cfg.destination_ports
                .iter()
                .map(u16::to_string)
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "BENCH_SAMPLE_INTERVAL",
            cfg.sample_interval.as_secs_f64().to_string(),
        ),
    ]
}

pub(super) fn prepare_monitor(
    cfg: &config::Config,
    binary: &Path,
    output: &Path,
    cancelled: &AtomicBool,
) -> std::result::Result<TargetMonitor, String> {
    TargetMonitor::prepare(cfg, binary, output, cancelled)
        .and_then(|mut monitor| {
            monitor.ready(cancelled)?;
            Ok(monitor)
        })
        .map_err(|error| {
            eprintln!("Target telemetry unavailable: {error}");
            error.to_string()
        })
}

pub(super) fn start_monitor(
    monitor: &mut std::result::Result<TargetMonitor, String>,
    start_at: &str,
    cancelled: &AtomicBool,
) {
    if let Ok(observer) = monitor
        && let Err(error) = observer.start(start_at, cancelled)
    {
        eprintln!("Target telemetry unavailable: {error}");
        *monitor = Err(error.to_string());
    }
}
