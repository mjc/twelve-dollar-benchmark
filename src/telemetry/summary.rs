use super::{Point, Process, Result, host::SystemMemory};
use chrono::Utc;
use num_traits::ToPrimitive;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

pub(super) fn checked_sum(values: impl IntoIterator<Item = Option<u64>>) -> Option<u64> {
    values
        .into_iter()
        .try_fold(0_u64, |total, value| total.checked_add(value?))
}

#[derive(Default)]
struct SystemMemoryPeaks {
    total_bytes: Option<u64>,
    min_available_bytes: Option<u64>,
    peak_used_bytes: Option<u64>,
    peak_used_including_reclaimable_bytes: Option<u64>,
}

impl SystemMemoryPeaks {
    fn observe(&mut self, sample: Option<&SystemMemory>) {
        let Some(sample) = sample else { return };
        self.total_bytes.get_or_insert(sample.total_bytes);
        if let Some(available) = sample.available_bytes {
            self.min_available_bytes = Some(
                self.min_available_bytes
                    .map_or(available, |current| current.min(available)),
            );
            self.peak_used_bytes = self
                .peak_used_bytes
                .max(sample.total_bytes.checked_sub(available));
        }
        self.peak_used_including_reclaimable_bytes = self
            .peak_used_including_reclaimable_bytes
            .max(sample.total_bytes.checked_sub(sample.free_bytes));
    }

    fn report(&self) -> Value {
        json!({
            "total_bytes": self.total_bytes,
            "min_available_bytes": self.min_available_bytes,
            "peak_total_minus_available_bytes": self.peak_used_bytes,
            "peak_total_minus_free_bytes_including_reclaimable": self.peak_used_including_reclaimable_bytes,
            "scope": "whole observed system from /proc/meminfo; MemAvailable is never inferred when missing; MemFree-only usage includes reclaimable cache"
        })
    }
}

/// Summarize counters between samples wholly inside the full-load hold.
///
/// # Errors
/// Returns an error if the file or samples are malformed, or no valid interval exists.
pub fn summarize_hold(
    path: &Path,
    start: chrono::DateTime<Utc>,
    finish: chrono::DateTime<Utc>,
) -> Result<Value> {
    let mut first: Option<Point> = None;
    let mut last: Option<Point> = None;
    let mut peak_memory = None;
    let mut system_memory = SystemMemoryPeaks::default();
    for line in BufReader::new(File::open(path)?).lines() {
        let point: Point = serde_json::from_str(&line?)?;
        let at = chrono::DateTime::parse_from_rfc3339(&point.timestamp)?;
        if at < start || at > finish {
            continue;
        }
        system_memory.observe(point.system_memory.as_ref());
        if let Some(groups) = point
            .service
            .get("groups")
            .and_then(Value::as_object)
            .filter(|groups| !groups.is_empty())
        {
            let memory = checked_sum(
                groups
                    .values()
                    .map(|group| group.get("memory_current_bytes").and_then(Value::as_u64)),
            );
            peak_memory = peak_memory.max(memory);
        }
        if first.is_none() {
            first = Some(point.clone());
        }
        last = Some(point);
    }
    let (Some(first), Some(last)) = (first, last) else {
        return Err("no telemetry samples inside full-load hold".into());
    };
    let duration = (chrono::DateTime::parse_from_rfc3339(&last.timestamp)?
        - chrono::DateTime::parse_from_rfc3339(&first.timestamp)?)
    .to_std()
    .map_err(|_| "not enough telemetry samples inside hold")?;
    let seconds = duration.as_secs_f64();
    let count = |point: &Point| -> Option<u64> {
        let rows = point.activity.get("requests")?.as_array()?;
        checked_sum(rows.iter().map(|row| row.get(0).and_then(Value::as_u64)))
    };
    let requests = count(&last)
        .zip(count(&first))
        .and_then(|(end, begin)| end.checked_sub(begin));
    let cpu = first
        .service
        .get("groups")
        .and_then(Value::as_object)
        .zip(last.service.get("groups").and_then(Value::as_object))
        .filter(|(begin, end)| !begin.is_empty() && begin.keys().eq(end.keys()))
        .and_then(|(begin, end)| {
            checked_sum(begin.iter().map(|(path, value)| {
                end.get(path)?
                    .get("cpu_usage_us")?
                    .as_u64()?
                    .checked_sub(value.get("cpu_usage_us")?.as_u64()?)
            }))
        });
    Ok(json!({
        "sample_started_at": first.timestamp,
        "sample_finished_at": last.timestamp,
        "sample_duration_s": seconds,
        "requests_per_second": rate(requests, seconds, 1.0),
        "service_cpu_percent": cpu.and_then(|n| n.to_f64()).map(|n| n / (seconds * 10_000.0)),
        "sampled_peak_memory_bytes": peak_memory,
        "memory_scope": "sampled cgroup memory.current for selected application and proxy groups, including descendants, socket buffers and page cache",
        "system_memory": system_memory.report()
    }))
}

pub(super) fn rate(delta: Option<u64>, seconds: f64, scale: f64) -> Option<f64> {
    if seconds > 0.0 {
        delta.and_then(|n| n.to_f64()).map(|n| n * scale / seconds)
    } else {
        None
    }
}

pub(super) fn interval_seconds(begin: &Point, end: &Point) -> f64 {
    end.elapsed_s - begin.elapsed_s
}

pub(super) fn network_report(begin: &Point, end: &Point) -> Value {
    let seconds = interval_seconds(begin, end);
    let (Some(first), Some(last)) = (&begin.interfaces, &end.interfaces) else {
        return json!({"interfaces": null, "unavailable_reason": "network interface counters unavailable"});
    };
    let mut result = BTreeMap::new();
    for name in first.keys().chain(last.keys()).collect::<BTreeSet<_>>() {
        let (Some(start), Some(finish)) = (first.get(name), last.get(name)) else {
            result.insert(
                name,
                json!({"unavailable_reason": "interface was absent at an interval boundary"}),
            );
            continue;
        };
        let delta = [
            finish.rx_bytes.checked_sub(start.rx_bytes),
            finish.tx_bytes.checked_sub(start.tx_bytes),
            finish.rx_packets.checked_sub(start.rx_packets),
            finish.tx_packets.checked_sub(start.tx_packets),
        ];
        result.insert(name, json!({"receive_bytes": delta[0], "transmit_bytes": delta[1], "received_packets": delta[2], "transmitted_packets": delta[3],
            "receive_bits_per_second": rate(delta[0], seconds, 8.0), "transmit_bits_per_second": rate(delta[1], seconds, 8.0),
            "receive_packets_per_second": rate(delta[2], seconds, 1.0), "transmit_packets_per_second": rate(delta[3], seconds, 1.0),
            "unavailable_reason": if delta.iter().any(Option::is_none) {Some("interface counters reset")} else if seconds <= 0.0 { Some("no elapsed observation interval") } else {None}}));
    }
    json!(result)
}

pub(super) fn tcp_report(begin: &Point, end: &Point) -> Value {
    let (Some(first), Some(last)) = (&begin.tcp, &end.tcp) else {
        return json!({"unavailable_reason": "TCP namespace counters unavailable"});
    };
    let seconds = interval_seconds(begin, end);
    let mut report = BTreeMap::new();
    for (name, value) in last {
        if name == "CurrEstab" {
            report.insert(name, json!({"value": value}));
        } else {
            let delta = first.get(name).and_then(|start| value.checked_sub(*start));
            report.insert(name, json!({"count": delta, "per_second": rate(delta, seconds, 1.0), "unavailable_reason": if delta.is_none() {Some("counter unavailable or reset")} else {None}}));
        }
    }
    json!(report)
}

fn counter_delta(begin: &Value, end: &Value) -> Option<u64> {
    end.as_u64()?.checked_sub(begin.as_u64()?)
}

pub(super) fn activity_report(begin: &Point, end: &Point) -> Value {
    let seconds = interval_seconds(begin, end);
    let (Some(first), Some(last)) = (
        begin.activity.get("requests").and_then(Value::as_array),
        end.activity.get("requests").and_then(Value::as_array),
    ) else {
        return Value::Null;
    };
    let mut requests = BTreeMap::new();
    for (index, name) in crate::ENDPOINTS.iter().enumerate() {
        let delta =
            |field| counter_delta(first.get(index)?.get(field)?, last.get(index)?.get(field)?);
        let count = delta(0);
        let failed = delta(1);
        let successful = count
            .zip(failed)
            .and_then(|(all, failed)| all.checked_sub(failed));
        requests.insert(name, json!({"requests": count, "successful": successful, "failed": failed,
            "requests_per_second": rate(count, seconds, 1.0), "successful_requests_per_second": rate(successful, seconds, 1.0), "failed_requests_per_second": rate(failed, seconds, 1.0)}));
    }
    let connection_delta = |index| {
        counter_delta(
            begin.activity.get("connections")?.get(index)?,
            end.activity.get("connections")?.get(index)?,
        )
    };
    let established = connection_delta(0);
    let failed = connection_delta(1);
    json!({"endpoints": requests, "connections": {"established": established, "failed": failed, "cancelled": connection_delta(2),
        "established_per_second": rate(established, seconds, 1.0), "failed_per_second": rate(failed, seconds, 1.0)}})
}

pub(super) fn interval_report(begin: &Point, end: &Point) -> Value {
    let seconds = interval_seconds(begin, end);
    let mut cpu = BTreeMap::new();
    for (role, processes) in &end.processes {
        let reports: Vec<_> = processes.iter().map(|process| {
            let delta = begin.processes.get(role).into_iter().flatten().find(|p| p.pid == process.pid && p.start_ticks == process.start_ticks)
                .map(|p| process.cpu_seconds - p.cpu_seconds).filter(|n| *n >= 0.0);
            json!({"pid": process.pid, "cpu_seconds": delta, "cpu_percent_of_one_core": if seconds > 0.0 {delta.map(|n| n / seconds * 100.0)} else {None}})
        }).collect();
        cpu.insert(role, reports);
    }
    json!({"duration_s": seconds, "network": network_report(begin,end), "tcp": tcp_report(begin,end), "activity": activity_report(begin,end), "cpu": cpu})
}

struct ProcessTotal {
    first_at: f64,
    last_at: f64,
    first_cpu: f64,
    last: Process,
    peak_resident: u64,
    peak_descriptors: Option<usize>,
    peak_sockets: Option<usize>,
    errors: BTreeSet<String>,
}

pub(super) fn add_errors(
    destination: &mut BTreeSet<String>,
    errors: impl IntoIterator<Item = String>,
) {
    for error in errors {
        if destination.len() < 128 {
            destination.insert(error.chars().take(512).collect());
        }
    }
}

pub(crate) fn summarize(path: &Path) -> Result<Value> {
    // Keep only endpoint counters and peaks; a long trace must not become a second in-memory copy.
    let mut first: Option<Point> = None;
    let mut last: Option<Point> = None;
    let mut samples = 0;
    let mut roles = BTreeSet::new();
    let mut processes: BTreeMap<(String, u32, u64), ProcessTotal> = BTreeMap::new();
    let mut errors = BTreeSet::new();
    let mut peak_established = None;
    let mut peak_conntrack = None;
    let mut system_memory = SystemMemoryPeaks::default();
    for line in BufReader::new(File::open(path)?).lines() {
        let point: Point = serde_json::from_str(&line?)?;
        if first.is_none() {
            first = Some(point.clone());
        }
        samples += 1;
        system_memory.observe(point.system_memory.as_ref());
        peak_conntrack = peak_conntrack.max(
            point.limits["conntrack_count"]["value"]
                .as_str()
                .and_then(|n| n.parse::<u64>().ok()),
        );
        add_errors(&mut errors, point.errors.clone());
        peak_established = peak_established.max(
            point
                .tcp
                .as_ref()
                .and_then(|tcp| tcp.get("CurrEstab"))
                .copied(),
        );
        for (role, data) in &point.processes {
            roles.insert(role.clone());
            for process in data {
                let aggregate = processes
                    .entry((role.clone(), process.pid, process.start_ticks))
                    .or_insert_with(|| ProcessTotal {
                        first_at: point.elapsed_s,
                        last_at: point.elapsed_s,
                        first_cpu: process.cpu_seconds,
                        last: process.clone(),
                        peak_resident: process.resident_bytes,
                        peak_descriptors: process.open_descriptors,
                        peak_sockets: process.established_sockets,
                        errors: BTreeSet::new(),
                    });
                aggregate.last_at = point.elapsed_s;
                aggregate.peak_resident = aggregate.peak_resident.max(process.resident_bytes);
                aggregate.peak_descriptors =
                    aggregate.peak_descriptors.max(process.open_descriptors);
                aggregate.peak_sockets = aggregate.peak_sockets.max(process.established_sockets);
                add_errors(&mut aggregate.errors, process.errors.clone());
                aggregate.last = process.clone();
            }
        }
        last = Some(point);
    }
    let first = first.ok_or("no telemetry samples")?;
    let last = last.unwrap();
    let mut process_report = BTreeMap::new();
    for role in roles {
        let collected: Vec<_> = processes.iter().filter(|((process_role, _, _), _)| process_role == &role).map(|((_, pid, start_ticks), aggregate)| {
            let elapsed = aggregate.last_at - aggregate.first_at;
            let cpu = aggregate.last.cpu_seconds - aggregate.first_cpu;
            json!({"pid": pid, "name": aggregate.last.name, "start_ticks": start_ticks, "observed_duration_s": elapsed,
                "cpu_seconds": (cpu >= 0.0).then_some(cpu), "cpu_percent_of_one_core": (elapsed > 0.0 && cpu >= 0.0).then(|| cpu / elapsed * 100.0),
                "peak_resident_bytes": aggregate.peak_resident, "peak_open_descriptors": aggregate.peak_descriptors,
                "peak_established_sockets": aggregate.peak_sockets, "descriptor_limits": aggregate.last.descriptor_limits, "errors": aggregate.errors})
        }).collect();
        process_report.insert(role, json!({"processes": collected, "unavailable_reason": if collected.is_empty() { Some("no selected accessible process; supply the role's process IDs on the target host") } else {None}}));
    }
    Ok(
        json!({"host_boot_id": first.host_boot_id, "network_namespace": first.network_namespace, "sample_interval_s": first.sample_interval_s,
        "started_at": first.timestamp, "finished_at": last.timestamp, "observed_duration_s": last.elapsed_s-first.elapsed_s, "samples": samples,
        "network_scope": "per-interface host/network-namespace counters include unrelated traffic; do not add virtual links or both ends of a connection; packet counters can reflect offloads, not physical wire packets",
        "network": network_report(&first,&last), "tcp": tcp_report(&first,&last), "peak_namespace_established_sockets": peak_established, "activity": activity_report(&first,&last),
        "processes": process_report, "limits": last.limits, "peak_conntrack_count": peak_conntrack, "errors": errors,
        "process_scope": "CPU includes every process thread and is expressed relative to one CPU core; process memory, descriptors and established sockets are sampled peaks",
        "system_memory": system_memory.report()}),
    )
}
