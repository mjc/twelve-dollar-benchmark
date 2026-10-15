# Twelve Dollar Benchmark

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

The build also produces `twelve-dollar-observer`. For remote target monitoring,
the runner copies only this observer binary to the target host; generators
continue running on their selected hosts.

The token file must already exist. By default, the runner reads
`seed/tokens.json` from the directory where you invoke it. Use `--tokens` or
`TOKENS` to select another file. You can invoke `run.sh` by path from any
directory. Relative token, summary, and output paths resolve from the calling
directory.

Without `--users` or `VUS`, the runner finds a score. It warms up at 1,000 users
for two minutes without scoring, starts at `--start-users` (2,500 by default)
and doubles until a run fails or reaches `--max-users` (300,000 by default),
then narrows the passing/failing bracket to 250 users. Search runs hold for one
minute. A separate five-minute run confirms the result; if it fails, the runner
steps down by 250 users and tries again. Each run also includes the configured
ramp-up, 30-second ramp-down, and drain, followed by a one-minute pause at
zero load before the next trial. Use `--skip-warmup` only when a warm-up has
already completed in the same session. A full search
can take hours. Progress and every trial's reports are saved as the search runs.

If the search ceiling passes confirmation, the result is a lower bound: the
server limit was not found. Increase `--max-users` when generator capacity
allows.

Use `--step-users N` to search with fixed increments instead of doubling. For
example, `--start-users 10000 --step-users 10000` tries 10,000, 20,000, 30,000,
and so on. After a failure, the runner still narrows to 250-user steps.

Use `--score` to select scoring explicitly, or `--users N` for a fixed-load run.
`--duration` controls fixed-load holds. A fixed-load result is not presented as
a confirmed score.

| Option | Default | Meaning |
|---|---|---|
| `--source-ips IPS` | unset | Local source addresses, or SSH generator hosts with `--user` |
| `--destination-ports PORTS` | positional port(s) | Destination ports; overrides the port positional |
| `--user USER` | unset | Copy the release and tokens to each host and run over SSH |
| `--monitor-user USER` | `--user` or unset | SSH to the target for monitoring while generators run locally |
| `--users N` | unset (scoring workflow) | Fixed total concurrent users across all generators |
| `--skip-warmup` | off | Reuse a warm-up completed earlier in this session |
| `--score` | enabled when neither `--users` nor `VUS` is set | Find and confirm a score |
| `--start-users N` | `2500` | First search load; positive multiple of 250 |
| `--step-users N` | doubling | Fixed search increment; positive multiple of 250 |
| `--max-users N` | `300000` | Search ceiling; positive multiple of 250 |
| `--label NAME` | target URL | Implementation name in the final score table |
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
`http://127.0.0.1:3000`. `SUMMARY` requests an additional copy of the aggregate
report.

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
The runner does not change system-wide ephemeral-port settings. On Linux, `run.sh`
checks for `/proc/sys/net/netfilter/nf_conntrack_max` and raises it to the
launcher process's effective soft descriptor limit only when conntrack is
available and the current value is lower. This runtime sysctl change requires
root, `doas`, or `sudo`; it is not persistent across reboot. If conntrack is
unavailable or already has a higher limit, the runner leaves it unchanged.
This applies on the launcher host only. SSH generator hosts run the binary
directly, and server hosts are not changed by the launcher; both need their own
conntrack capacity. Connection tracking, memory, and server capacity still
limit the load.

With `--user`, the source IPs select SSH generator hosts. The runner copies the
release binary and tokens to every host and checks that the binary runs there.
All generators must be ready before a shared future start; host clocks must be
synchronized. Hosts must support the binary's OS, architecture, and runtime.
SSH/SCP use keys or your agent and normal known-host checks. `--users` is split
evenly with distinct token offsets and must be at least the host count. Remote
generators use their OS's source-address selection. Their global user offsets
spread users across destination ports while preserving the total user count.

Without SSH, monitoring works for a loopback target. The runner detects
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

Ctrl-C or SIGTERM stops new users and asks current users to stop after their
active browsing iteration. The runner allows 10 seconds for graceful shutdown
before terminating a generator that has not exited. It collects available
outputs and marks the run incomplete. The benchmark collects its own request
samples, connection samples, and telemetry.

## Results

The runner prints Markdown tables when a run finishes. Scoring output starts
with the maximum-users comparison row: implementation label, confirmed users,
request rate during the confirmed hold, p50/p95/p99, HTTP errors, service CPU,
sampled peak cgroup memory, and the failing threshold. Rank and PR are blank.
The row is saved as `leaderboard.md` in the scoring results directory. A score
at the search ceiling is prefixed with `≥`. Fixed-load output starts with
requested and reached users, p95/p99, HTTP errors, and PASS/FAIL. Both modes
then show request rates, connection rates and setup times, bandwidth, packets
per second, and process resources. Scoring runs retain the search history in
`workflow.json`, with each trial under `trials/`. Setup failures and interrupted
runs stop the search instead of counting as an application limit.

Each results directory contains `aggregate.json` and a Markdown `report.md`.
The report includes overall and per-endpoint request rates, successful and
failed connection setup distributions, requested and reached user counts,
required and observed common full-load hold times, source-port capacity, and
sampled kernel and conntrack counters and peaks. Hold request rate, service CPU,
and sampled peak cgroup memory cover samples taken inside the common full-load
window. A `local/` or per-host directory
contains `summary.json`, exact HTTP `samples.json`, `connection-samples.json`,
`telemetry.jsonl`, and `output.log`. Target observations are stored in
`target/`.

The coordinator renews an observer lease. If contact is lost for 120 seconds,
the observer stops and keeps partial results. Remote result directories are
copied back recursively.

JSON records configured targets in `configured_hosts` and `destination_ports`,
and observed request destinations in `hosts`. Duplicate destination ports are
removed while preserving their first-occurrence order.

Error details are collected by default in endpoint summaries, aggregate
summaries, and raw samples. Counts report exact error-kind and HTTP-status
totals; each report retains at most 100 examples per endpoint, runtime errors,
and aggregate, with an omitted-example count when the cap is exceeded. Reasons
and response-body excerpts are each limited to 1 KiB. Rendered Markdown groups
counts by type and HTTP status and deduplicates representative examples by
context while keeping worker attribution. Examples are also written as
`BENCH_ERROR` lines to each worker's `output.log` as they occur.

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
100%. The rendered network table uses decimal MB/s and packets/s (pps); JSON
retains raw bit-per-second counters. Open TCP connections and file descriptors
are reported separately. FD, memory, and socket peaks are sampled, so shorter
spikes may be missed.

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
Feed response bodies are consumed and dropped before the user sleeps. Each
user's connection remains independent.
Connections go directly to the supplied target. The generator ignores outbound
proxy environment variables such as `HTTP_PROXY` and `ALL_PROXY`.
Each client and user task is created at its scheduled ramp-up time.

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
covers HTTP sending, waiting, and complete body reception, excluding DNS and TCP
connection setup. Connection setup durations are reported separately.

`duration_s` includes shutdown time.

On Linux/Android, a local connector patch uses `IP_BIND_ADDRESS_NO_PORT` so
explicit source binding does not reserve a port before the destination is known.
This lets the same source port serve distinct destination tuples. Provenance and
the retained upstream license are in [the connector patch notes](vendor/hyper-util/LOCAL_PATCH.md).

The [Hyper patch](vendor/hyper/LOCAL-PATCHES.md) starts adaptive HTTP/1 reads at
1 KiB, allocates write headers on demand and reserves one slot for the first
queued body. Read buffers still grow for larger responses; HTTP/1 limits remain
unchanged.

## Code layout

The binary entry point in `src/main.rs` dispatches load, orchestration, and
supervisor commands. `load.rs` owns user scheduling and shutdown; `workload.rs`
owns the browsing sequence, request checks, and token preflight. `transport.rs`
and `transport/body.rs` own HTTP connections and response buffers.

`runner.rs` coordinates local or SSH generators. Its `process` module owns SSH
quoting, transfers, and child cancellation; `monitor` owns the target observer
and heartbeat; `aggregate` validates and combines worker artifacts. `samples.rs`
shares sample merging and latency statistics, while `report.rs` renders reports.

`telemetry.rs` owns sampling and the observer lease. Its `host` module reads
Linux process, socket, network, and cgroup counters; `summary` computes interval,
whole-run, and hold metrics. The observer binary uses this same library code.
