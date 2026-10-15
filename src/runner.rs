mod aggregate;
use aggregate::finish_run;
mod monitor;
mod process;
use monitor::{TargetMonitor, prepare_monitor, start_monitor};
pub(crate) use process::supervise;
use process::{checked, destination, execute, log_output, quote, remote_command, scp, ssh, upload};

use crate::{
    config, destinations,
    workload::{load_tokens, parse_source_ips},
};
use chrono::Utc;
use serde::Serialize;
use std::{
    collections::BTreeSet,
    env,
    error::Error,
    fs,
    net::IpAddr,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
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

// Coordinates worker setup, execution, collection, and publication as one run lifecycle.
pub(super) fn run(args: &[String], cancelled: &Arc<AtomicBool>) -> Result<u8> {
    let cfg = config::parse(args)?;
    if cfg.help {
        print!("{}", config::HELP);
        return Ok(0);
    }
    let binary = env::current_exe()?;
    if cfg.score {
        return super::scoring::run(&cfg, args, cancelled);
    }
    let ips = parse_source_ips(&cfg.source_ips)?;
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
    let setup = WorkerSetup {
        cfg: &cfg,
        binary: &binary,
        ips: &ips,
        destination_ports: &destination_ports,
        tokens: &tokens,
        output: &output,
        cancelled,
    };
    let run = run_workers(&setup)?;
    finish_run(&setup, run, &targets)
}

struct WorkerSetup<'a> {
    cfg: &'a config::Config,
    binary: &'a Path,
    ips: &'a [IpAddr],
    destination_ports: &'a str,
    tokens: &'a Path,
    output: &'a Path,
    cancelled: &'a Arc<AtomicBool>,
}

struct WorkerRun {
    monitor: std::result::Result<TargetMonitor, String>,
    workers: Vec<Worker>,
}

fn run_workers(setup: &WorkerSetup<'_>) -> Result<WorkerRun> {
    if let Some(user) = setup.cfg.user.as_deref() {
        let workers = upload_remote_workers(setup, user)?;
        if workers.iter().all(|worker| worker.error.is_none()) {
            run_remote_workers(setup, user, workers)
        } else {
            Ok(WorkerRun {
                monitor: Err("target observer unavailable".into()),
                workers,
            })
        }
    } else {
        run_local_worker(setup)
    }
}

fn upload_remote_workers(setup: &WorkerSetup<'_>, user: &str) -> Result<Vec<Worker>> {
    let WorkerSetup {
        binary,
        ips,
        tokens,
        output,
        cancelled,
        ..
    } = setup;
    if ips.iter().collect::<BTreeSet<_>>().len() != ips.len() {
        return Err("SSH generator addresses must be unique".into());
    }
    let mut workers = Vec::new();
    for (index, (ip, (count, offset))) in ips
        .iter()
        .zip(split_users(setup.cfg.users, ips.len())?)
        .enumerate()
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
                cancelled,
            )?;
            upload(
                user,
                &ip.to_string(),
                binary,
                &format!("{remote_dir}/twelve-dollar-benchmark"),
                cancelled,
            )?;
            upload(
                user,
                &ip.to_string(),
                tokens,
                &format!("{remote_dir}/tokens.json"),
                cancelled,
            )?;
            checked(
                &mut remote_command(
                    user,
                    &ip.to_string(),
                    &format!("{remote_dir}/twelve-dollar-benchmark"),
                    &["--version"],
                ),
                cancelled,
            )?;
            Ok(())
        })();
        if let Err(error) = upload {
            worker.error = Some(error.to_string());
        }
        workers.push(worker);
    }
    Ok(workers)
}

fn run_remote_workers(
    setup: &WorkerSetup<'_>,
    user: &str,
    workers: Vec<Worker>,
) -> Result<WorkerRun> {
    let controls = workers
        .iter()
        .zip(setup.ips)
        .map(|(worker, ip)| {
            worker
                .remote_dir
                .as_ref()
                .map(|dir| (ip.to_string(), dir.clone()))
                .ok_or_else(|| "remote worker has no directory".into())
        })
        .collect::<Result<Vec<_>>>()?;
    thread::scope(|scope| {
        let joins = workers
            .into_iter()
            .zip(setup.ips)
            .map(|(worker, ip)| scope.spawn(move || execute_remote_worker(worker, setup, user, ip)))
            .collect::<Vec<_>>();
        let mut run = WorkerRun {
            monitor: Err("target observer unavailable".into()),
            workers: Vec::new(),
        };
        if let Err(error) = coordinate_remote_start(setup, user, &controls, &mut run) {
            eprintln!("coordinated start failed: {error}");
            setup.cancelled.store(true, Ordering::Relaxed);
        }
        run.workers = joins
            .into_iter()
            .map(|join| join.join().expect("worker thread panicked"))
            .collect();
        Ok(run)
    })
}

fn coordinate_remote_start(
    setup: &WorkerSetup<'_>,
    user: &str,
    controls: &[(String, String)],
    run: &mut WorkerRun,
) -> Result<()> {
    for (ip, dir) in controls {
        checked(
            &mut remote_command(
                user,
                ip,
                &format!("{dir}/twelve-dollar-benchmark"),
                &["--ready-supervised", &format!("{dir}/cancel")],
            ),
            setup.cancelled,
        )?;
    }
    run.monitor = prepare_monitor(setup.cfg, setup.binary, setup.output, setup.cancelled);
    let start_at = (Utc::now().timestamp_millis() + 5000).to_string();
    start_monitor(&mut run.monitor, &start_at, setup.cancelled);
    publish_remote_start(user, controls, &start_at, setup.cancelled)
}

fn publish_remote_start(
    user: &str,
    controls: &[(String, String)],
    start_at: &str,
    cancelled: &AtomicBool,
) -> Result<()> {
    let publications = thread::scope(|scope| {
        let handles = controls
            .iter()
            .map(|(ip, dir)| {
                scope.spawn(move || {
                    checked(
                        &mut remote_command(
                            user,
                            ip,
                            &format!("{dir}/twelve-dollar-benchmark"),
                            &["--start-supervised", &format!("{dir}/cancel"), start_at],
                        ),
                        cancelled,
                    )
                    .map_err(|error| error.to_string())
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|_| Err("start publication panicked".into()))
            })
            .collect::<Vec<_>>()
    });
    for publication in publications {
        publication?;
    }
    Ok(())
}

fn worker_environment(
    setup: &WorkerSetup<'_>,
    users: usize,
    offset: usize,
    sources: &str,
) -> Vec<(&'static str, String)> {
    vec![
        ("BASE_URL", setup.cfg.base_url.clone()),
        ("DESTINATION_PORTS", setup.destination_ports.to_owned()),
        ("VUS", users.to_string()),
        ("RAMP_UP", setup.cfg.ramp_up.clone()),
        ("DURATION", setup.cfg.duration.clone()),
        ("BENCH_USER_OFFSET", offset.to_string()),
        (
            "BENCH_SAMPLE_INTERVAL",
            setup.cfg.sample_interval.as_secs_f64().to_string(),
        ),
        ("SOURCE_IPS", sources.to_owned()),
    ]
}

fn execute_remote_worker(
    mut worker: Worker,
    setup: &WorkerSetup<'_>,
    user: &str,
    ip: &IpAddr,
) -> Worker {
    let host = ip.to_string();
    let result = (|| -> Result<()> {
        let remote_dir = worker
            .remote_dir
            .as_deref()
            .ok_or("remote worker has no directory")?;
        let dir = setup.output.join(&worker.name);
        let assignments = worker_environment(setup, worker.users, worker.user_offset, "")
            .into_iter()
            .map(|(name, value)| format!("{name}={value}"))
            .chain([
                "TOKENS=../tokens.json".into(),
                "SUMMARY=summary.json".into(),
                "BENCH_SAMPLES=samples.json".into(),
                "BENCH_TELEMETRY=telemetry.jsonl".into(),
            ])
            .map(|value| quote(&value))
            .collect::<Vec<_>>()
            .join(" ");
        let command_text = format!(
            "cd {} || exit 2; trap '' HUP; exec env {} ../twelve-dollar-benchmark --supervise ../cancel",
            quote(&format!("{remote_dir}/results")),
            assignments
        );
        let mut command = ssh(user, &host);
        command.arg(command_text);
        log_output(&mut command, fs::File::create(dir.join("output.log"))?)?;
        let status = execute(&mut command, setup.cancelled)?;
        worker.exit_code = status.code();
        if setup.cancelled.load(Ordering::Relaxed) || !matches!(worker.exit_code, Some(0 | 1)) {
            let mut stop = remote_command(
                user,
                &host,
                &format!("{remote_dir}/twelve-dollar-benchmark"),
                &["--stop-supervised", &format!("{remote_dir}/cancel")],
            );
            checked(&mut stop, &AtomicBool::new(false))?;
        }
        let remote = destination(user, &host, remote_dir);
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
}

fn run_local_worker(setup: &WorkerSetup<'_>) -> Result<WorkerRun> {
    let dir = setup.output.join("local");
    fs::create_dir(&dir)?;
    let mut command = Command::new(setup.binary);
    log_output(&mut command, fs::File::create(dir.join("output.log"))?)?;
    let mut monitor = prepare_monitor(setup.cfg, setup.binary, setup.output, setup.cancelled);
    let start_at = (Utc::now().timestamp_millis() + 1000).to_string();
    start_monitor(&mut monitor, &start_at, setup.cancelled);
    command
        .current_dir(&dir)
        .envs(worker_environment(
            setup,
            setup.cfg.users,
            0,
            &setup.cfg.source_ips,
        ))
        .env("TOKENS", setup.tokens)
        .env("SUMMARY", dir.join("summary.json"))
        .env("BENCH_SAMPLES", dir.join("samples.json"))
        .env("BENCH_TELEMETRY", dir.join("telemetry.jsonl"))
        .env("BENCH_START_AT", start_at);
    let status = execute(&mut command, setup.cancelled);
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
    Ok(WorkerRun {
        monitor,
        workers: vec![Worker {
            name: "local".into(),
            users: setup.cfg.users,
            user_offset: 0,
            remote_dir: None,
            exit_code,
            error,
        }],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn total_users_split_evenly_with_disjoint_token_offsets() {
        assert_eq!(split_users(10, 3).unwrap(), [(4, 0), (3, 4), (3, 7)]);
        assert!(split_users(2, 3).is_err());
        assert!(split_users(100, 0).is_err());
    }
}
