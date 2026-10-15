use super::{Result, Worker, WorkerRun, WorkerSetup};
use crate::{
    connections,
    errors::ErrorDetails,
    report::render_report,
    samples::{self, Samples},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, File},
    io::BufReader,
    net::IpAddr,
    path::Path,
    sync::atomic::Ordering,
};

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
        let parsed = read_worker(output, worker);
        match parsed {
            Ok((report, raw)) => {
                if let Some(received) = read_connection_samples(output, worker, &report) {
                    connection_data.merge(received);
                } else {
                    connection_available = false;
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
                    combined.merge(&mut received);
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
    let duration_s = started
        .zip(finished)
        .and_then(|(s, f)| (f - s).to_std().ok())
        .map(|d| d.as_secs_f64());
    let report = json!({
        "generator": "twelve-dollar-benchmark", "workers": workers,
        "maximum_users": maximum_users,
        "runtime_error_details": runtime_error_details,
        "total_users": total_users, "hosts": hosts, "source_ips": source_ips,
        "started_at": started, "finished_at": finished,
        "common_hold_s": common_hold_s, "required_hold_s": required_hold,
        "hold_started_at": hold_start, "hold_finished_at": hold_end,
        // An elapsed interval across all workers, not a sum of their durations.
        "duration_s": duration_s,
        "percentiles": "k6 linear interpolation over all workers' unrounded samples",
        "connections": if connection_available { connection_data.report(duration_s) } else { json!({"unavailable_reason": "missing or inconsistent per-worker connection samples"}) },
        "generator_telemetry": telemetry,
    });
    samples::report(&mut samples, duration_s, runtime_errors, completed, report).unwrap()
}

fn read_worker(output: &Path, worker: &Worker) -> Result<(Report, [Samples; 4])> {
    let dir = output.join(&worker.name);
    let report: Report =
        serde_json::from_reader(BufReader::new(File::open(dir.join("summary.json"))?))?;
    let raw: [Samples; 4] =
        serde_json::from_reader(BufReader::new(File::open(dir.join("samples.json"))?))?;
    if raw.iter().any(|s| {
        s.failed > s.latency_ms.len() || s.latency_ms.iter().any(|ms| !ms.is_finite() || *ms < 0.0)
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
}

fn read_connection_samples(
    output: &Path,
    worker: &Worker,
    report: &Report,
) -> Option<connections::Connections> {
    let path = output.join(&worker.name).join("connection-samples.json");
    let samples = serde_json::from_reader::<_, connections::Connections>(BufReader::new(
        File::open(path).ok()?,
    ))
    .ok()?;
    (samples.cancelled <= samples.failed_ms.len()
        && samples
            .successful_ms
            .iter()
            .chain(&samples.failed_ms)
            .all(|ms| ms.is_finite() && *ms >= 0.0)
        && report.connections["established"].as_u64() == Some(samples.successful_ms.len() as u64)
        && report.connections["failed"].as_u64() == Some(samples.failed_ms.len() as u64))
    .then_some(samples)
}

pub(super) fn finish_run(
    setup: &WorkerSetup<'_>,
    mut run: WorkerRun,
    targets: &[url::Url],
) -> Result<u8> {
    let WorkerSetup {
        cfg,
        output,
        cancelled,
        ips,
        ..
    } = setup;
    let workers = &mut run.workers;
    let source_ip_count = ips.len().max(1);
    let target_telemetry = match &mut run.monitor {
        Ok(monitor) => monitor.finish().unwrap_or_else(|error| {
            eprintln!("Target telemetry unavailable: {error}");
            json!({"unavailable_reason": error.to_string(), "shutdown_unconfirmed": true})
        }),
        Err(reason) => json!({"unavailable_reason": reason}),
    };
    let mut aggregate = collect(output, workers);
    aggregate["network_hosts"] =
        network_hosts(&aggregate["generator_telemetry"], &target_telemetry);
    aggregate["target_telemetry"] = target_telemetry;
    aggregate["implementation"] = json!(cfg.label.as_deref().unwrap_or(&cfg.base_url));
    let hold = (|| -> Result<Value> {
        let start = serde_json::from_value(aggregate["hold_started_at"].clone())?;
        let finish = serde_json::from_value(aggregate["hold_finished_at"].clone())?;
        let mut requests_per_second = 0.0;
        for worker in workers.iter() {
            let summary = twelve_dollar_benchmark::telemetry::summarize_hold(
                &output.join(&worker.name).join("telemetry.jsonl"),
                start,
                finish,
            )?;
            requests_per_second += summary["requests_per_second"]
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
    aggregate["configured_hosts"] = json!(targets.iter().map(url::Url::as_str).collect::<Vec<_>>());
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
    Ok(u8::from(aggregate["passed"] != true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::example;
    use std::path::PathBuf;
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
                    crate::errors::ErrorLocation {
                        endpoint: "GET /feed",
                        path: "/feed",
                        user_index: worker.user_offset,
                        destination: "http://fixture.test:3000",
                        source_ip: Some("192.0.2.1".into()),
                    },
                    Some(503),
                    "http",
                    "queue full",
                    Some(b"busy"),
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
}
