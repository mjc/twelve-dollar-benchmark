use serde_json::{Value, json};
use std::{collections::BTreeSet, fmt::Write as _};

pub(super) fn render_score_table(report: &Value) -> String {
    let number = |value: Option<f64>, suffix: &str| {
        value.map_or_else(|| "unavailable".into(), |n| format!("{n:.2}{suffix}"))
    };
    let label = report["implementation"]
        .as_str()
        .unwrap_or("target")
        .replace('|', "\\|")
        .replace(['\r', '\n'], " ");
    format!(
        "| Rank | PR | Implementation | Max users | Req/s | p50 | p95 | p99 | Errors | CPU | Peak system used | Limited by |\n\
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
            report["hold"]["system_memory"]["peak_total_minus_available_bytes"]
                .as_f64()
                .map(|n| n / 1_048_576.0),
            " MiB"
        ),
        report["limit_reached"]
            .as_str()
            .unwrap_or("not established")
    )
}

fn number(value: &Value) -> String {
    value
        .as_f64()
        .map_or_else(|| "unavailable".into(), |n| format!("{n:.2}"))
}

pub(super) fn render_report(report: &Value, compact: bool) -> String {
    let title = report["score"].as_u64().map_or("results".into(), |score| {
        let bound = if report["score_is_lower_bound"] == true {
            "≥ "
        } else {
            ""
        };
        format!("score: {bound}{score} VUs")
    });
    let error_rate = report["aggregate"]["failure_rate"].as_f64().map_or_else(
        || "unavailable".into(),
        |rate| format!("{:.2}", rate * 100.0),
    );
    let table = if report["score"].is_u64() {
        format!(
            "{}\nReq/s and CPU are means from samples inside the confirmed hold. Peak system used is MemTotal − MemAvailable sampled for the whole observed system during the hold; it includes the OS and observer. It is unavailable when those whole-system samples are missing. The legacy cgroup metric remains in hold JSON. Latency and errors cover the complete confirmation run. Rank and PR are left blank; Limited by names a failing threshold, not an inferred resource bottleneck.",
            render_score_table(report)
        )
    } else {
        format!(
            "| Requested VUs | Reached VUs | p95 ms | p99 ms | HTTP errors % | Result |\n|---:|---:|---:|---:|---:|---|\n| {} | {} | {} | {} | {} | {} |",
            report["requested_users"],
            report["maximum_users"],
            number(&report["aggregate"]["p95_ms"]),
            number(&report["aggregate"]["p99_ms"]),
            error_rate,
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
        number(&report["duration_s"])
    );
    let _ = writeln!(
        text,
        "| All endpoints | {} | {} | {} | {} | {} |",
        number(&report["request_rates"]["requests_per_second"]),
        number(&report["request_rates"]["successful_requests_per_second"]),
        number(&report["request_rates"]["failed_requests_per_second"]),
        number(&report["aggregate"]["p95_ms"]),
        number(&report["aggregate"]["p99_ms"])
    );
    if let Some(endpoints) = report["endpoints"].as_object() {
        for (name, endpoint) in endpoints {
            let _ = writeln!(
                text,
                "| {name} | {} | {} | {} | {} | {} |",
                number(&endpoint["requests_per_second"]),
                number(&endpoint["successful_requests_per_second"]),
                number(&endpoint["failed_requests_per_second"]),
                number(&endpoint["p95_ms"]),
                number(&endpoint["p99_ms"])
            );
        }
    }
    let _ = writeln!(
        text,
        "\nFull-load hold: {} s / {} s required.",
        number(&report["common_hold_s"]),
        number(&report["required_hold_s"])
    );
    let _ = writeln!(
        text,
        "Source ports: {}–{}; theoretical tuple capacity {} before occupied ports and TIME_WAIT.",
        report["source_port_allocation"]["first"],
        report["source_port_allocation"]["last"],
        report["source_port_allocation"]["tuple_capacity"]
    );
    if !compact {
        render_request_errors(&mut text, report);
    }
    render_connections(&mut text, report);
    render_system_memory(&mut text, report);
    render_network(&mut text, report, compact);
    render_processes(&mut text, report, compact);
    text.push_str("\nExact latency/connection samples, resource time series, kernel limits and availability details are retained alongside aggregate.json.\n");
    text
}

fn render_request_errors(text: &mut String, report: &Value) {
    if let Some(endpoints) = report["endpoints"].as_object() {
        for (endpoint, details) in endpoints {
            if details["failed"].as_u64().unwrap_or(0) > 0
                && details["errors"]["counts"]
                    .as_object()
                    .is_none_or(serde_json::Map::is_empty)
            {
                let _ = writeln!(
                    text,
                    "\nRequest errors — {}: {} failed requests; error details are unavailable in this legacy report.",
                    markdown_text(endpoint),
                    details["failed"]
                );
                continue;
            }
            render_error_details(
                text,
                &format!("Request errors — {}", markdown_text(endpoint)),
                &details["errors"],
            );
        }
    }
    render_error_details(text, "Runtime errors", &report["runtime_error_details"]);
}

fn render_connections(text: &mut String, report: &Value) {
    let connections = &report["connections"];
    let _ = writeln!(
        text,
        "\n| Connection attempts/s | Established/s | Failed/s | Cancelled |\n|---:|---:|---:|---:|\n| {} | {} | {} | {} |",
        number(&connections["attempts_per_second"]),
        number(&connections["connections_per_second"]),
        number(&connections["failed_per_second"]),
        connections["cancelled"]
    );
    let _ = writeln!(
        text,
        "Connection attempts: {} ({} per second); {} established, {} failed, {} cancelled.\n\n| Setup outcome | Samples | Average ms | Median ms | p95 ms | p99 ms | Maximum ms |\n|---|---:|---:|---:|---:|---:|---:|",
        connections["attempts"],
        number(&connections["attempts_per_second"]),
        connections["established"],
        connections["failed"],
        connections["cancelled"]
    );
    for (name, key) in [
        ("All", "setup_duration_ms"),
        ("Successful", "successful_setup_duration_ms"),
        ("Failed (including cancelled)", "failed_setup_duration_ms"),
    ] {
        let setup = &connections[key];
        let _ = writeln!(
            text,
            "| {name} | {} | {} | {} | {} | {} | {} |",
            setup["samples"],
            number(&setup["avg_ms"]),
            number(&setup["med_ms"]),
            number(&setup["p95_ms"]),
            number(&setup["p99_ms"]),
            number(&setup["max_ms"])
        );
    }
    if let Some(reason) = connections["unavailable_reason"].as_str() {
        let _ = writeln!(text, "Connection metrics unavailable: {reason}");
    }
}

fn render_network(text: &mut String, report: &Value, compact: bool) {
    text.push_str("\nHost interface counters include background traffic. Packet counters may reflect offloads. Do not sum virtual links or both connection ends. Host aliases are deduplicated.\n\n| Observer(s) | Interface | RX MB/s | TX MB/s | RX packets/s (pps) | TX packets/s (pps) |\n|---|---|---:|---:|---:|---:|\n");
    if let Some(hosts) = report["network_hosts"]["hosts"].as_object() {
        for host in hosts.values() {
            let observers = host["observed_by"]
                .as_array()
                .into_iter()
                .flatten()
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
                    let megabytes_per_second = |key| {
                        network[key]
                            .as_f64()
                            .map_or(Value::Null, |bits_per_second| {
                                json!(bits_per_second / 8_000_000.0)
                            })
                    };
                    let _ = writeln!(
                        text,
                        "| {observers} | {name} | {} | {} | {} | {} |",
                        number(&megabytes_per_second("receive_bits_per_second")),
                        number(&megabytes_per_second("transmit_bits_per_second")),
                        number(&network["receive_packets_per_second"]),
                        number(&network["transmit_packets_per_second"])
                    );
                }
            }
        }
    }
    text.push_str("\nCPU includes all process threads; 100% is one core. Memory, descriptors and open TCP connections are sampled peaks.\n\n| Observer | Role / PID | CPU % | Peak RSS MiB | Peak FDs | FD soft / hard limit | Peak open TCP connections |\n|---|---|---:|---:|---:|---|---:|\n");
}

fn observations(report: &Value) -> impl Iterator<Item = (&str, &Value)> {
    report["generator_telemetry"]
        .as_object()
        .into_iter()
        .flat_map(|map| map.iter())
        .map(|(name, value)| (name.as_str(), value))
        .chain(std::iter::once(("target", &report["target_telemetry"])))
}

fn render_system_memory(text: &mut String, report: &Value) {
    text.push_str("\nWhole-system memory is read from each observer's `/proc/meminfo`; values cover its full observation window and include background system activity. Used (total − available) uses the kernel's MemAvailable estimate. Cache-inclusive used (total − free) counts reclaimable cache as used.\n\n| Observer | Total MiB | Minimum available MiB | Peak used (total − available) MiB | Peak used including reclaimable cache (total − free) MiB |\n|---|---:|---:|---:|---:|\n");
    for (observer, value) in observations(report) {
        let memory = &value["system_memory"];
        let mib = |key| {
            memory[key]
                .as_f64()
                .map_or(Value::Null, |bytes| json!(bytes / 1_048_576.0))
        };
        let _ = writeln!(
            text,
            "| {observer} | {} | {} | {} | {} |",
            number(&mib("total_bytes")),
            number(&mib("min_available_bytes")),
            number(&mib("peak_total_minus_available_bytes")),
            number(&mib("peak_total_minus_free_bytes_including_reclaimable"))
        );
    }
}

fn render_processes(text: &mut String, report: &Value, compact: bool) {
    for (observer, value) in observations(report) {
        if let Some(reason) = value["unavailable_reason"].as_str() {
            let _ = writeln!(
                text,
                "| {observer} | {reason} | unavailable | unavailable | unavailable | unavailable | unavailable |"
            );
        }
        if let Some(roles) = value["processes"].as_object() {
            for (role, values) in roles {
                if compact && role == "observer" {
                    continue;
                }
                if let Some(reason) = values["unavailable_reason"].as_str() {
                    let _ = writeln!(
                        text,
                        "| {observer} | {role}: {reason} | unavailable | unavailable | unavailable | unavailable | unavailable |"
                    );
                }
                for process in values["processes"].as_array().into_iter().flatten() {
                    let rss = process["peak_resident_bytes"]
                        .as_f64()
                        .map_or(Value::Null, |n| json!(n / 1_048_576.0));
                    let _ = writeln!(
                        text,
                        "| {observer} | {role} / {} | {} | {} | {} | {} / {} | {} |",
                        process["pid"],
                        number(&process["cpu_percent_of_one_core"]),
                        number(&rss),
                        process["peak_open_descriptors"],
                        process["descriptor_limits"]["soft"]
                            .as_str()
                            .unwrap_or("unavailable"),
                        process["descriptor_limits"]["hard"]
                            .as_str()
                            .unwrap_or("unavailable"),
                        process["peak_established_sockets"]
                    );
                }
            }
        }
    }
    if !compact {
        text.push_str("\nKernel counters are observations; conntrack count/max alone do not establish that these connections are tracked or that conntrack caused a limit.\n\n| Observer | Kernel setting | Value |\n|---|---|---|\n");
        for (observer, value) in observations(report) {
            if let Some(limits) = value["limits"].as_object() {
                for (name, limit) in limits {
                    let display = limit["value"]
                        .as_str()
                        .or_else(|| limit["unavailable_reason"].as_str())
                        .unwrap_or("unavailable");
                    let _ = writeln!(text, "| {observer} | {name} | {} |", markdown_text(display));
                }
            }
            let _ = writeln!(
                text,
                "| {observer} | peak namespace established sockets | {} |\n| {observer} | peak conntrack count | {} |",
                value["peak_namespace_established_sockets"], value["peak_conntrack_count"]
            );
        }
    }
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

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct ErrorContext<'a> {
    kind: &'a str,
    status_code: Option<u64>,
    reason: &'a str,
    body_excerpt: Option<&'a str>,
    path: &'a str,
    worker: Option<&'a str>,
}

fn string_field<'a>(value: &'a Value, name: &str) -> Option<&'a str> {
    value.get(name).and_then(Value::as_str)
}

fn error_context(example: &Value) -> ErrorContext<'_> {
    ErrorContext {
        kind: string_field(example, "kind").unwrap_or_default(),
        status_code: example.get("status_code").and_then(Value::as_u64),
        reason: string_field(example, "reason").unwrap_or_default(),
        body_excerpt: string_field(example, "body_excerpt"),
        path: string_field(example, "path").unwrap_or_default(),
        worker: string_field(example, "worker"),
    }
}

fn render_error_details(text: &mut String, title: &str, value: &Value) {
    let Some(counts) = value
        .get("counts")
        .and_then(Value::as_object)
        .filter(|counts| !counts.is_empty())
    else {
        return;
    };
    let _ = writeln!(text, "\n{title}:");
    render_error_counts(text, counts);
    if let Some(examples) = value.get("examples").and_then(Value::as_array) {
        render_error_examples(text, examples);
    }
    render_omitted_errors(text, value);
}

fn render_error_counts(text: &mut String, counts: &serde_json::Map<String, Value>) {
    for (label, prefix) in [("Error type", "kind:"), ("HTTP status", "status:")] {
        let rows = counts
            .iter()
            .filter_map(|(key, count)| key.strip_prefix(prefix).map(|name| (name, count)))
            .collect::<Vec<_>>();
        if !rows.is_empty() {
            let _ = writeln!(text, "\n| {label} | Count |\n|---|---:|");
            for (name, count) in rows {
                let _ = writeln!(text, "| {} | {count} |", markdown_text(name));
            }
        }
    }
}

fn render_error_examples(text: &mut String, examples: &[Value]) {
    let mut contexts = BTreeSet::new();
    let unique = examples
        .iter()
        .filter(|example| contexts.insert(error_context(example)))
        .collect::<Vec<_>>();
    let duplicates = examples.len() - unique.len();
    let mut shown = BTreeSet::new();
    let mut workers = BTreeSet::new();
    for (index, example) in unique.iter().enumerate() {
        if let Some(worker) = string_field(example, "worker")
            && workers.insert(worker)
        {
            shown.insert(index);
            if shown.len() == 5 {
                break;
            }
        }
    }
    for index in 0..unique.len() {
        if shown.len() == 5 {
            break;
        }
        shown.insert(index);
    }
    let shown_count = shown.len();
    if shown_count > 0 {
        let _ = writeln!(text, "\nExamples:\n");
    }
    for (_, example) in unique
        .iter()
        .enumerate()
        .filter(|(index, _)| shown.contains(index))
    {
        let worker = string_field(example, "worker").map(markdown_text);
        let endpoint = string_field(example, "endpoint")
            .map(markdown_text)
            .unwrap_or_default();
        let path = string_field(example, "path")
            .map(markdown_text)
            .unwrap_or_default();
        let kind = string_field(example, "kind")
            .map(markdown_text)
            .unwrap_or_default();
        let reason = string_field(example, "reason")
            .map(markdown_text)
            .unwrap_or_default();
        let status = example
            .get("status_code")
            .and_then(Value::as_u64)
            .map_or_else(|| "no HTTP status".to_owned(), |n| n.to_string());
        let worker = worker
            .as_deref()
            .map_or_else(String::new, |name| format!("{name}: "));
        let _ = writeln!(
            text,
            "- {worker}{endpoint} {path} {status}: {kind} ({reason})"
        );
        if let Some(body) = string_field(example, "body_excerpt") {
            let encoded = serde_json::to_string(body).unwrap_or_else(|_| "\"unavailable\"".into());
            let truncated = if example
                .get("body_truncated")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                " [truncated]"
            } else {
                ""
            };
            let _ = writeln!(text, "    {}{truncated}", markdown_text(&encoded));
        }
    }
    let hidden = unique.len().saturating_sub(shown_count);
    if hidden > 0 {
        let _ = writeln!(text, "- {hidden} distinct stored examples not shown.");
    }
    if duplicates > 0 {
        let _ = writeln!(
            text,
            "- {duplicates} repeated stored examples omitted from display."
        );
    }
}

fn render_omitted_errors(text: &mut String, value: &Value) {
    if let Some(omitted) = value
        .get("omitted")
        .and_then(Value::as_u64)
        .filter(|n| *n > 0)
    {
        let _ = writeln!(
            text,
            "- {omitted} additional examples omitted before storage."
        );
    }
}
