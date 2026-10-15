# Goose load test

Supported target: Ubuntu 24.04 on x86-64.

Start the server separately. From the repository root, run the benchmark with an
IP address and one or more ports:

```bash
./run.sh 127.0.0.1 '3100-3104' --source-ips '127.0.0.[1-5]' --users 75000 --tokens /path/to/tokens.json
./run.sh 10.0.0.5 3000 --source-ips '10.0.0.[11-12]' --user bench --users 8000 --tokens /path/to/tokens.json
```

The runner builds `twelve-dollar-benchmark`, an edition 2024 crate, with Rust
1.99.0 and locked dependencies. If
that toolchain is missing, it installs rustup and Rust 1.99.0. The generator
sends HTTP/1.1 traffic. It does not include HTTPS or TLS support.

Target metrics run in a separate binary, `twelve-dollar-observer`. The runner
builds both binaries and copies only the observer to the target when using
`--monitor-user`. The load generator stays on the generator host.

The token file must already exist. By default, the runner reads
`seed/tokens.json` from the directory where you invoke it. Use `--tokens` or
`TOKENS` to select another file. You can invoke `run.sh` by path from any
directory. Relative token, summary, and output paths resolve from the calling
directory.

Without `--users` or `VUS`, the runner finds a score. It warms up at 1,000 users
for two minutes without scoring, starts at `--start-users` (2,500 by default) and doubles until a run
fails, then narrows the passing/failing bracket to 250 users. Search runs use
one-minute holds. A separate five-minute run confirms the result;
if it fails, the runner steps down by 250 users and tries again. Each run also
includes the configured ramp-up, 30-second ramp-down, and drain, followed by
a one-minute pause at zero load before the next trial. Use `--skip-warmup` only
when a warm-up has already completed in the same session. A full search
can take hours. Progress and every trial's reports are saved as the search runs.

Use `--score --start-users 100000` to start the search at 100,000 users.
Use `--score --start-users 10000 --step-users 10000` to search at 10k, 20k,
30k, and so on instead of doubling. After a failure, the runner still narrows
to 250 users and confirms the result with a separate five-minute hold.
The warm-up stays at 1,000 users. The search stops at `--max-users` (300,000 by
default). If that load passes confirmation, the score is marked `≥`: the server
limit has not been found. Increase the ceiling when the generator setup supports
more users. Use `--users N` for a fixed-load run.
`--duration` controls fixed-load holds. A fixed-load result is not presented as
a confirmed score.

| Option | Default | Meaning |
|---|---|---|
| `--source-ips IPS` | unset | Local source addresses, or SSH generator hosts with `--user` |
| `--destination-ports PORTS` | positional port(s) | Destination ports; overrides the port positional |
| `--user USER` | unset | Copy the release and tokens to each host and run over SSH |
| `--monitor-user USER` | `--user` or unset | SSH to the target for monitoring while generators run locally |
| `--users N` | unset (scoring workflow) | Fixed total concurrent users across all generators |
| `--score` | enabled when neither `--users` nor `VUS` is set | Find and confirm a score |
| `--skip-warmup` | off | Reuse a warm-up completed earlier in the same session |
| `--start-users N` | `2500` | First search load; positive multiple of 250 |
| `--step-users N` | unset (double) | Fixed search increment; positive multiple of 250 |
| `--max-users N` | `300000` | Search ceiling; positive multiple of 250 |
| `--label NAME` | target URL | Implementation name in the final table |
| `--ramp-up TIME` | `60s` | Ramp to total users |
| `--duration TIME` | `5m` | Hold at total users |
| `--tokens PATH` | `seed/tokens.json` | User tokens |
| `--output-dir DIR` | `results/run-<timestamp>-<pid>/` | New results directory; must not already exist |
| `--application-pids PIDS` | detect listeners | Optional application PID override |
| `--proxy-pids PIDS` | detect recognized proxies | Optional proxy PID override |
| `--sample-interval SECONDS` | `1` | Resource/network sampling period; minimum `0.1` |

Times accept seconds or combinations of `h`, `m`, and `s`, such as `1m30s`.
Ramp-down lasts 30 seconds. `--help` lists all options.

Legacy `BASE_URL`, `DESTINATION_PORTS`, `VUS`, `RAMP_UP`, `DURATION`, `TOKENS`,
and `SOURCE_IPS` provide defaults; explicit arguments take precedence. If you
omit both target positionals, the default target is
`http://127.0.0.1:3000`. Relative `TOKENS`, `SUMMARY`, and output paths resolve
from the directory where you invoked the runner. `SUMMARY` requests an
additional copy of the aggregate report.

The port positional accepts a single port, a list such as `3100,3101,3102`, or an
inclusive range such as `3100-3104`. `--destination-ports` accepts the same forms.
All supplied ports must serve the same application, authentication, and workload.

Source lists accept IPv4/IPv6 and inclusive IPv4 final-octet ranges, including
`127.0.0.[1-5]` and `192.0.2.10,192.0.2.11`. Without `--user`, one local generator
assigns users across the source-IP × destination-port combinations. Each user
keeps its assigned pair for connections and reconnects.
For a local generator, source addresses must already be assigned to the host
and able to reach the target. The runner checks that it can bind each address
and records the expanded list as `source_ips` in JSON.

The connector selects source ports 1024–65535 explicitly for each
source-address/destination tuple. Clients share this range, with
`SO_REUSEADDR` allowing a source port to serve different destinations. For one
source address, five destination ports provide a theoretical capacity of
322,560 sockets before port collisions and `TIME_WAIT`; this is an upper bound,
not a load guarantee.
The runner does not change system-wide ephemeral-port settings. Connection
tracking, memory, and server capacity still limit the load.

With `--user`, the source IPs select SSH generator hosts. The runner copies the
release binary and tokens to every host and checks that the binary runs there.
All generators must be ready before a shared future start; host clocks must be
synchronized. Hosts must support the binary's OS, architecture, and runtime.
SSH/SCP use keys or your agent and normal known-host checks. `--users` is split
evenly with distinct token offsets and must be at least the host count. Remote
generators use their OS's source-address selection. Their global user offsets
spread users across destination ports while preserving the total user count.

Target monitoring uses `--monitor-user` when set, otherwise it reuses `--user`.
Set `--monitor-user` to monitor a remote target over SSH while running generators
locally. Without SSH, monitoring works for a loopback target. The runner detects
accessible application processes listening on all supplied destination ports.
It also looks for common proxy processes and their observed local TCP backends.
PID options are optional overrides. Use them for custom proxies, remote or Unix
socket backends, or idle proxy pools that automatic discovery cannot identify.
Direct application runs need no PID options. For example:

```bash
./run.sh 10.0.0.5 '3100-3104' --monitor-user bench \
  --users 150000 --tokens /path/to/tokens.json --sample-interval 1
```

Unavailable processes or counters include an explanation. If discovery finds
no recognized proxy, proxy metrics are reported as unavailable, not as zero CPU.

Ctrl-C or SIGTERM stops local and remote generators, allowing 10 seconds for
graceful cancellation before terminating them if needed, and collects available
outputs. Goose's duplicate native metric queues are disabled unless a native log, report,
running metric display, or coordinated-omission diagnostic is requested. Scoring
and telemetry are always collected. Goose diagnostic flags can follow `--`, such as
`-- --report-file native.html`; overrides for the target, users, test plans,
run time, hatch rate, startup time, iterations, and autostart are rejected.
Pass short diagnostic flags separately: `-v -v`, rather than `-vv`.

## Results

The script prints Markdown tables to the terminal when the run finishes.
Scoring runs start with a row matching the maximum-users comparison table:
implementation, confirmed users, hold req/s, p50/p95/p99, errors, service CPU,
sampled peak cgroup memory, and the failing threshold. Rank and PR are left blank.
Use `--label` to name the implementation. The same table is saved as
`leaderboard.md`; no JSON processing is needed to read or copy it.

Fixed-load runs start with requested and reached VUs, p95, p99, HTTP errors and
PASS/FAIL. Both modes then show request rates, connection rates and setup times,
bandwidth, packets per second, and process resources.
Scoring runs retain the search history in
`workflow.json`, with each trial under `trials/`. Setup failures and interrupted
runs stop the search instead of counting as an application limit.

Each results directory contains `aggregate.json` and a Markdown `report.md`.
The report includes overall and per-endpoint request rates, successful and
failed connection setup distributions, requested and reached user counts,
required and observed full-load hold times, source-port capacity, and sampled
kernel and conntrack counters and peaks. A `local/` or per-host directory
contains `summary.json`, exact HTTP `samples.json`, `connection-samples.json`,
`telemetry.jsonl`, and `output.log`. Target observations are stored in
`target/`.

The coordinator renews an observer lease. If contact is lost for 120 seconds,
the observer stops and keeps partial results. Remote result directories,
including native reports, are copied back recursively. Relative native
report/log paths resolve inside each generator's result directory.

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
