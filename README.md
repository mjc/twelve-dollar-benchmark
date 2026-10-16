# Twelve Dollar Benchmark

HTTP/1.1 load generator for the Twelve Dollar Server Challenge. Runs on
Ubuntu 24.04, x86-64.

## Run

Start the server separately and supply its IP, ports, and an existing token file:

```bash
./run.sh 192.0.2.10 3100-3109 --users 180000 --duration 5m \
  --tokens /path/to/tokens.json --monitor-user ubuntu
```

`run.sh` builds the release binaries and reuses Cargo's build cache. On Ubuntu,
it installs missing Rust and C build tools; package installation requires root,
`sudo`, or `doas`.

`--monitor-user` uses SSH to collect target CPU, memory, connection, and network
stats. Set up SSH key access first. Omit it for a local loopback target. HTTPS is
not supported.

Ports can be a single number, comma-separated list, or range. All ports must
serve the same application. Tokens default to `seed/tokens.json`; relative paths
resolve from the directory where you invoke the script.

A fixed run defaults to a 60-second ramp, five-minute hold, 30-second ramp-down,
and 10-second grace period. Change the ramp with `--ramp-up` and hold with
`--duration`. Ctrl-C stops the run and preserves available partial results.

Run `./run.sh --help` for all options.

### Find a score

Omit `--users` to run the scoring workflow:

```bash
./run.sh 192.0.2.10 3100-3109 --tokens /path/to/tokens.json --monitor-user ubuntu
```

This warms up at 1,000 users for two minutes, searches with one-minute holds,
then confirms with a five-minute hold. It starts at 2,500 users, doubles until
failure, and narrows to 250-user steps. A failed confirmation steps down by 250.
Trials drain, then pause for one minute before the next run.

Set `--start-users`, `--step-users`, and `--max-users` to change the search range.
The default ceiling is 300,000; passing at the ceiling gives a lower bound.
A fixed `--users` run does not produce a confirmed score.

### Connections and remote generators

One source address can connect to multiple destination ports. For local source
addresses, use `--source-ips`; those addresses must already be assigned.

To split users across SSH generator hosts, add
`--source-ips '192.0.2.20,192.0.2.21' --user bench`. The hosts need compatible
binaries, SSH key access, and synchronized clocks.

The launcher raises its soft file-descriptor limit toward 524,288 and raises
`nf_conntrack_max` if available and lower. Raising conntrack requires root,
`sudo`, or `doas`. Remote generators and servers need their own limits.

## Workload

Each VU uses a token from the supplied file, owns a keep-alive connection, and
waits 0–5 seconds before its first request. It then repeats:

1. Fetch the feed; wait 3–7 seconds.
2. Fetch a random post; wait 3–8 seconds.
3. Like it with 15% probability and create a post with 2% probability.
4. Wait 5–15 seconds.

Connections stay open during think time. VUs and established connections can
differ during startup or reconnects.

## Results

Results go to `results/run-*/` unless `--output-dir` selects a new directory.
`report.md` is the readable report; `aggregate.json` contains combined results.
Raw request samples, connection samples, and telemetry are also retained.

Reports include requests/s, latency, errors, open connections, CPU, RSS,
whole-system memory, and network traffic. CPU uses one core as 100%; resource
peaks are sampled. Unavailable measurements include a reason.

PASS requires p95 **< 500 ms**, p99 **< 1,000 ms**, and HTTP failures **< 1%**
across the whole run, including shutdown. Failures include transport errors,
incomplete bodies, and statuses outside 200–399. Runtime errors, empty results,
and incomplete holds also fail. Exit codes are 0 for PASS, 1 for FAIL, and 2 for
setup, execution, or output errors.

HTTP latency covers sending through complete body reception; TCP setup is
reported separately. Hold request rates exclude ramp-up and shutdown.

## Observed generator throughput

Measured on 2026-10-16 at commit `a2db1cc`, with observer and resource sampling
enabled. The generator used one source address and ten destination ports.

| Generator VM | Specification |
|---|---|
| OS | Ubuntu 24.04, x86-64 |
| CPU | 1 vCPU |
| RAM | 2 GiB |
| Swap | Disabled |
| Disk | 50 GiB |

| Concurrent VUs | Hold duration | HTTP requests/s during hold | Peak established TCP connections |
|---|---|---|---|
| 180,000 | 300 seconds | 17,529.8 | 180,000 |

The full run completed 6,210,920 requests with zero HTTP errors. Connections are
the generator process's sampled established-socket peak.

| Generator resource | Measured value | Window |
|---|---|---|
| Process CPU | 53.23% of one core | Hold |
| VM CPU busy | 55.85% of one core | Hold |
| Peak process RSS | 966.41 MiB | Whole run |
| Peak VM used RAM | 1,801.05 MiB | Whole run |
| Minimum VM available RAM | 166.94 MiB | Whole run |
| Peak process file descriptors | 180,011 | Whole run |
| Average receive rate | 31.59 MB/s | Whole run |
| Average transmit rate | 3.66 MB/s | Whole run |

VM used RAM is `MemTotal - MemAvailable`, including the OS and kernel. Network
rates use decimal MB/s and include other interface traffic.

Patched dependencies: [revisions and patch provenance](patches/README.md).
