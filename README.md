# Goose load test

The `twelve-dollar-benchmark` crate uses Rust edition 2024.

Start your server separately. The runner builds the benchmark tool in release
mode and runs it against the supplied IP and ports using HTTP/1.1. HTTPS and TLS
are outside the benchmark contract and are not compiled into the generator.
From the repository root:

```bash
./run.sh 127.0.0.1 '3100-3104' --source-ips '127.0.0.[1-5]' --users 75000 --tokens /path/to/tokens.json
./run.sh 10.0.0.5 3000 --source-ips '10.0.0.[11-12]' --user bench --users 8000 --tokens /path/to/tokens.json
```

The runner uses Rust 1.99.0 and locked dependencies, installing rustup and that
Rust version when needed. The token file must already exist. The default is
`seed/tokens.json` in the directory where you invoke the runner; pass `--tokens`
or set `TOKENS` to use another file. Invoke the script by path from any
directory. Relative token, summary, and output paths resolve from the directory
you invoke it in.

| Option | Default | Meaning |
|---|---|---|
| `--source-ips IPS` | unset | Local source addresses, or SSH generator hosts with `--user` |
| `--destination-ports PORTS` | positional port(s) | Destination ports; overrides the port positional |
| `--user USER` | unset | Copy the release and tokens to each host and run over SSH |
| `--users N` | `100` | Total concurrent users across all generators |
| `--ramp-up TIME` | `60s` | Ramp to total users |
| `--duration TIME` | `5m` | Hold at total users |
| `--tokens PATH` | `seed/tokens.json` | User tokens |
| `--output-dir DIR` | `results/run-<timestamp>-<pid>/` | New results directory; must not already exist |
| `--application-pids PIDS` | detect listener | Comma-separated application PIDs on the target host |
| `--proxy-pids PIDS` | unset | Comma-separated proxy PIDs on the target host |
| `--sample-interval SECONDS` | `1` | Resource/network sampling period; minimum `0.1` |

Times accept seconds or `h`, `m`, `s`, such as `1m30s`. Ramp-down takes 30 seconds.
`--help` lists the options. Legacy `BASE_URL`, `VUS`, `RAMP_UP`, `DURATION`,
`TOKENS`, and `SOURCE_IPS` remain defaults; explicit arguments override them.
Omitting both target positionals uses `BASE_URL` (`http://127.0.0.1:3000` by
default). Legacy relative `TOKENS` and `SUMMARY` paths resolve from the directory
you invoke the runner in. `SUMMARY` requests an additional copy of the aggregate.

The port positional accepts a single port, a list such as `3100,3101,3102`, or an
inclusive range such as `3100-3104`. `--destination-ports` accepts the same forms.
All supplied ports must serve the same application, authentication, and workload.

Source lists accept IPv4/IPv6 and inclusive IPv4 final-octet ranges, including
`127.0.0.[1-5]` and `192.0.2.10,192.0.2.11`. Without `--user`, one local generator
assigns users across the source-IP × destination-port combinations. Each user
keeps its assigned pair for connections and reconnects.
Addresses must be locally bindable and reach the target; non-loopback addresses
must already be assigned. The runner checks binding and records the expanded
`source_ips` in JSON.

Separate destination ports can expand the available socket tuples. The runner
does not change the OS ephemeral-port range; connection tracking, memory, and
server capacity still limit the load.

With `--user`, the source IPs select SSH generator hosts. The runner copies the
release binary and tokens to every host and checks that the binary runs there.
All generators must be ready before a shared future start; host clocks must be
synchronized. Hosts must support the binary's OS, architecture, and runtime.
SSH/SCP use keys or your agent and normal known-host checks. `--users` is split
evenly with distinct token offsets and must be at least the host count. Remote
generators use their OS's source-address selection. Their global user offsets
spread users across destination ports while preserving the total user count.

Target monitoring uses the same SSH username when supplied. Without SSH access,
it can monitor a local loopback target. The application defaults to accessible
processes listening on all supplied destination ports. For a proxy, supply the
backend and proxy PIDs explicitly:

```bash
./run.sh 10.0.0.5 3000 --source-ips '10.0.0.[11-12]' --user bench \
  --users 8000 --application-pids 1234 --proxy-pids 5678 --sample-interval 0.5
```

Unavailable processes or counters carry an explanation. An unselected proxy
is reported as unavailable, never as zero CPU.

Ctrl-C or SIGTERM stops local and remote generators, allowing 10 seconds for
graceful cancellation before terminating them if needed, and collects available
outputs. Goose's duplicate native metric queues are disabled unless a native log, report,
running metric display, or coordinated-omission diagnostic is requested. Scoring
and telemetry are always collected. Goose diagnostic flags can follow `--`, such as
`-- --report-file native.html`; overrides for the target, users, test plans,
run time, hatch rate, startup time, iterations, and autostart are rejected.
Pass short diagnostic flags separately: `-v -v`, rather than `-vv`.

## Results

Each results directory contains `aggregate.json` and `report.md`. A `local/` or
per-host directory contains `summary.json`, exact HTTP `samples.json`,
`connection-samples.json`, `telemetry.jsonl`, and `output.log`. Target observations
are collected under `target/`. The coordinator renews an observer lease; if it
loses contact for 120 seconds, the observer stops and preserves partial results. Remote result directories, including native
reports, are copied back recursively. Relative native report/log paths resolve
inside each generator's result directory.

JSON records configured targets in `configured_hosts` and `destination_ports`,
and observed request destinations in `hosts`. Duplicate destination ports are
removed while preserving their first-occurrence order.

Error details are collected by default in endpoint summaries, aggregate
summaries, and raw samples. Counts report exact error-kind and HTTP-status
totals; each report retains at most 100 examples per endpoint, runtime errors,
and aggregate, with an omitted-example count when the cap is exceeded. Reasons
and response-body excerpts are each limited to 1 KiB. Examples are also written
as `BENCH_ERROR` lines to each worker's `output.log` as they occur. This logging
does not enable Goose's native metric queues; those remain disabled unless a
native diagnostic is requested.

The reports include total and per-endpoint successful/failed requests per second,
connection attempts and establishments per second, and setup durations including
failed and cancelled attempts. Request rates count requests; connection rates
count new transport connections. A keep-alive request does not create another
connection. Connection setup covers DNS and TCP; an
attempt can try several resolved addresses.

Linux telemetry records per-interface RX/TX throughput and packets per second,
application/proxy/generator CPU and memory, open descriptors and their limits,
and established sockets. It also records the observed ephemeral-port and kernel
limits. The launcher and each generator raise their soft descriptor limit to at
least 524,288, or as high as the inherited hard limit allows. A generator fails
before load if its limit cannot cover its users and process overhead. Source
addresses must already be assigned to that generator. CPU uses one core as
100%. FD, memory, and socket peaks are sampled, so shorter spikes may be missed.

Interface counters include other traffic on the host. Offloads can change packet
counts, and virtual interfaces can count the same traffic at several points.
Reports preserve host/interface measurements and deduplicate observations of the
same host/network namespace; they do not sum these into a claimed wire total.
Unavailable metrics retain their reason instead of a zero value.

## Workload and scoring

Users cycle through tokens, wait 0–5 seconds initially, then repeat: fetch the
feed, wait 3–7 seconds, fetch a random post, wait 3–8 seconds, like it with 15%
probability, create a post with 2% probability, and idle 5–15 seconds. Each user
owns an HTTP/1 client and retains its idle connection during think time. A server
that closes the connection requires that user to reconnect.
Feed response bodies are consumed and dropped before the user sleeps. HTTP
pools and sockets remain separate.
Connections go directly to the supplied target. The generator ignores outbound
proxy environment variables such as `HTTP_PROXY` and `ALL_PROXY`.
Goose builds each user's configured client once, at spawn. Pending users occupy
only scenario indices; that storage is released after the last user starts.

`maximum_users` describes the load stage. Measured established sockets report
the actual connection concurrency, which can differ during initial jitter,
reconnects, or source-port exhaustion.

Use `aggregate.json` and the final PASS/FAIL. Whole-run thresholds, including
shutdown: p95 **< 500 ms**, p99 **< 1,000 ms**, HTTP failures **< 1%**.
Failed requests include transport errors, incomplete bodies, and statuses outside
200–399. Endpoint checks are diagnostic, as in k6. Empty and interrupted runs
fail; runtime errors must be zero. Remote generators must overlap at full load
for `--duration`, allowing 250 ms scheduling tolerance; a missed start or
insufficient common hold fails. Exit status: 0 PASS, 1 FAIL, 2 setup/execution/output
error.

Scoring p95/p99 use k6's linear interpolation over combined unrounded samples
from all endpoints and hosts; per-host percentiles are never averaged. Latency
includes the complete body and connection setup. k6 excludes DNS/connect/TLS
setup, so new connections can measure slower. Goose's native header timings and
rounded percentiles are diagnostic.

During normal shutdown, Goose finishes an active browsing iteration; k6 allowed
10 seconds before interrupting it. Shutdown can take longer here, and `duration_s`
includes that time.

On Linux/Android, a local connector patch uses `IP_BIND_ADDRESS_NO_PORT` so
explicit source binding does not reserve a port before the destination is known.
This lets the same source port serve distinct destination tuples. Provenance and
the retained upstream license are in [the connector patch notes](vendor/hyper-util/LOCAL_PATCH.md).

The [Goose patch](vendor/goose/LOCAL_PATCH.md) allows its HTTP-only build by
gating the certificate option behind TLS features. The generator rejects HTTPS
targets and TLS-specific options before launching load.

The [Hyper patch](vendor/hyper/LOCAL-PATCHES.md) starts adaptive HTTP/1 reads at
1 KiB, allocates write headers on demand and reserves one slot for the first
queued body. Read buffers still grow for larger responses; HTTP/1 limits remain
unchanged.
