"""Check native reports and sleeping users against a local HTTP/1.1 server."""

from collections import Counter
from datetime import datetime
import http.server
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest


@unittest.skipUnless(os.environ.get("FEED_LOAD_BIN"), "set FEED_LOAD_BIN to test the built coordinator")
class MetricsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="goose metrics ")
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        self.binary = Path(os.environ["FEED_LOAD_BIN"]).resolve()
        self.tokens = self.root / "tokens.json"
        self.tokens.write_text(json.dumps([
            {"username": f"user-{index}", "token": f"token-{index}"}
            for index in range(8)
        ]))
        self.env = dict(os.environ)
        for name in ("BASE_URL", "TOKENS", "VUS", "RAMP_UP", "DURATION", "SOURCE_IPS", "DESTINATION_PORTS", "SUMMARY",
                     "BENCH_START_AT", "BENCH_USER_OFFSET", "BENCH_SAMPLES", "BENCH_TELEMETRY",
                     "BENCH_APPLICATION_PIDS", "BENCH_PROXY_PIDS", "BENCH_SAMPLE_INTERVAL",
                     "BENCH_OBSERVER_LEASE_SECONDS"):
            self.env.pop(name, None)
        self.lock = threading.Lock()
        self.sockets = set()
        self.source_ips = set()
        self.source_ports = set()
        self.connection_combinations = set()
        self.in_flight = 0
        self.requests = 0
        self.observations = []
        self.topology_observations = []
        fixture = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass

            def setup(self):
                super().setup()
                with fixture.lock:
                    fixture.sockets.add((self.server.server_port, *self.client_address))
                    fixture.source_ips.add(self.client_address[0])
                    fixture.source_ports.add(self.client_address[1])
                    fixture.connection_combinations.add((self.client_address[0], self.server.server_port))

            def finish(self):
                try:
                    super().finish()
                finally:
                    with fixture.lock:
                        fixture.sockets.discard((self.server.server_port, *self.client_address))

            def respond(self, status, body):
                with fixture.lock:
                    fixture.in_flight += 1
                    fixture.requests += 1
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    self.wfile.flush()
                finally:
                    with fixture.lock:
                        fixture.in_flight -= 1

            def do_GET(self):
                self.respond(200, json.dumps(
                    {"posts": [{"id": 42}]} if self.path == "/feed" else {"post": {"id": 42}}
                ).encode())

            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                self.respond(201, b"{}")

        self.handler = Handler
        self.listener_ports = [int(port) for port in os.environ.get("METRICS_SERVER_PORTS", "0,0").split(",")]
        self.assertEqual(len(self.listener_ports), 2)
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", self.listener_ports[0]), Handler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.servers = [(self.server, self.server_thread)]
        self.stop_observing = threading.Event()

        def observe():
            while not self.stop_observing.wait(0.05):
                with self.lock:
                    when = time.time()
                    self.observations.append((when, len(self.sockets), self.in_flight))
                    self.topology_observations.append((when, Counter(
                        (source_ip, destination_port)
                        for destination_port, source_ip, _source_port in self.sockets
                    )))

        self.observer_thread = threading.Thread(target=observe, daemon=True)
        self.observer_thread.start()
        self.addCleanup(self.stop_server)

    def stop_server(self):
        self.stop_observing.set()
        self.observer_thread.join()
        for server, thread in self.servers:
            server.shutdown()
            server.server_close()
            thread.join()

    def command(self, port, output, *, users=8, duration="8s"):
        return [str(self.binary), "--orchestrate", "127.0.0.1", str(port),
                "--users", str(users), "--ramp-up", "1s", "--duration", duration,
                "--source-ips", "127.0.0.[1-2]", "--tokens", str(self.tokens),
                "--application-pids", str(os.getpid()), "--sample-interval", "0.2",
                "--output-dir", str(output)]

    def run_benchmark(self, command, *, interrupt_after_failure=False, low_descriptor_limit=False,
                      descriptor_limits=None):
        log = self.root / "coordinator.log"
        if descriptor_limits:
            command = [sys.executable, "-c",
                       "import os,resource,sys; resource.setrlimit(resource.RLIMIT_NOFILE, tuple(map(int,sys.argv[1:3]))); os.execv(sys.argv[3],sys.argv[3:])",
                       *map(str, descriptor_limits), *command]
        elif low_descriptor_limit:
            # Use a fresh single-threaded interpreter; preexec_fn is unsafe in
            # this process because the HTTP fixture already owns threads.
            command = [sys.executable, "-c",
                       "import os,resource,sys; "
                       "hard=resource.getrlimit(resource.RLIMIT_NOFILE)[1]; "
                       "resource.setrlimit(resource.RLIMIT_NOFILE,(32,hard)); "
                       "os.execv(sys.argv[1],sys.argv[1:])", *command]
        with log.open("w") as output:
            child = subprocess.Popen(command, env=self.env, cwd=self.root,
                                     stdout=output, stderr=subprocess.STDOUT)
            try:
                if interrupt_after_failure:
                    trace = Path(command[-1]) / "local" / "telemetry.jsonl"
                    deadline = time.monotonic() + 20
                    while time.monotonic() < deadline and child.poll() is None:
                        if trace.exists():
                            points = []
                            for line in trace.read_text().splitlines():
                                try:
                                    points.append(json.loads(line))
                                except json.JSONDecodeError:
                                    pass  # A sampler write may still be in progress.
                            if any(point.get("activity", {}).get("connections", [0, 0])[1] > 0
                                   for point in points):
                                child.send_signal(signal.SIGINT)
                                break
                        time.sleep(0.2)
                    else:
                        self.fail("no actual refused connection appeared in the live trace")
                returncode = child.wait(timeout=90)
            finally:
                if child.poll() is None:
                    child.send_signal(signal.SIGTERM)
                    child.wait(timeout=25)
        return returncode, log.read_text()

    def assert_rates(self, summary, seconds):
        self.assertGreater(seconds, 0)
        self.assertAlmostEqual(summary["requests_per_second"], summary["requests"] / seconds)
        self.assertAlmostEqual(summary["successful_requests_per_second"],
                               (summary["requests"] - summary["failed"]) / seconds)
        self.assertAlmostEqual(summary["failed_requests_per_second"], summary["failed"] / seconds)

    def assert_setup_percentile(self, summary, values):
        ordered = sorted(values)
        rank = 0.99 * (len(ordered) - 1)
        lower = int(rank)
        upper = min(lower + 1, len(ordered) - 1)
        expected = ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)
        self.assertAlmostEqual(summary["p99_ms"], expected)

    def standalone_observer(self, directory, lease_seconds):
        directory.mkdir()
        control = directory / "metrics"
        env = dict(self.env, BENCH_APPLICATION_PIDS=str(os.getpid()), BENCH_PROXY_PIDS="",
                   BENCH_TARGET_PORT=str(self.server.server_port), BENCH_SAMPLE_INTERVAL="0.1",
                   BENCH_OBSERVER_LEASE_SECONDS=str(lease_seconds))
        log = (directory / "observer.log").open("w")
        child = subprocess.Popen([str(self.binary), "--observe", str(control)],
                                 cwd=directory, env=env, stdout=log, stderr=subprocess.STDOUT)

        def cleanup():
            try:
                if child.poll() is None:
                    control.write_text("stop\n")
                    child.wait(timeout=5)
            finally:
                log.close()

        self.addCleanup(cleanup)
        deadline = time.monotonic() + 10
        while not control.with_suffix(".ready").exists():
            self.assertIsNone(child.poll(), (directory / "observer.log").read_text())
            self.assertLess(time.monotonic(), deadline, "observer did not become ready")
            time.sleep(0.02)
        return child, control

    def test_observer_lease_renewal_keeps_sampling_until_coordinator_disappears(self):
        directory = self.root / "observer-renewed"
        child, control = self.standalone_observer(directory, 0.5)
        lease = control.with_suffix(".lease")
        lease.write_text("heartbeat\n")
        control.with_suffix(".start").write_text(str(int(time.time() * 1000) + 100))
        renew_until = time.monotonic() + 1.3
        while time.monotonic() < renew_until:
            lease.write_text("heartbeat\n")
            self.assertIsNone(child.poll(), "a renewed lease must permit a longer observation")
            time.sleep(0.1)
        self.assertEqual(child.wait(timeout=5), 0, (directory / "observer.log").read_text())
        self.assertFalse(control.exists(), "the observer must stop without a cancellation file")
        self.assertTrue(control.with_suffix(".done").exists())
        summary = json.loads((directory / "telemetry-summary.json").read_text())
        self.assertTrue(summary["stopped_by_lease_timeout"])
        self.assertIn("lease expired", summary["unavailable_reason"])
        self.assertGreater(summary["observed_duration_s"], 1.0)
        self.assertGreater(summary["samples"], 3)
        trace = [json.loads(line) for line in (directory / "telemetry.jsonl").read_text().splitlines()]
        self.assertEqual(len(trace), summary["samples"])

    def test_observer_without_lease_stops_and_preserves_partial_trace(self):
        directory = self.root / "observer-unleased"
        child, control = self.standalone_observer(directory, 0.3)
        control.with_suffix(".start").write_text(str(int(time.time() * 1000)))
        self.assertEqual(child.wait(timeout=5), 0, (directory / "observer.log").read_text())
        self.assertFalse(control.with_suffix(".lease").exists())
        summary = json.loads((directory / "telemetry-summary.json").read_text())
        self.assertTrue(summary["stopped_by_lease_timeout"])
        self.assertGreaterEqual(summary["samples"], 2)
        self.assertTrue(control.with_suffix(".done").exists())

    def test_report_counts_actual_connections_held_while_users_sleep(self):
        # Generator routing is always direct, even with inherited proxy settings.
        for name in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy",
                     "ALL_PROXY", "all_proxy"):
            self.env[name] = "http://127.0.0.1:1"
        self.env["NO_PROXY"] = self.env["no_proxy"] = ""
        output = self.root / "healthy"
        second_server = http.server.ThreadingHTTPServer(("127.0.0.1", self.listener_ports[1]), self.handler)
        second_thread = threading.Thread(target=second_server.serve_forever, daemon=True)
        second_thread.start()
        self.servers.append((second_server, second_thread))
        ports = [self.server.server_port, second_server.server_port]
        application_limits = resource.getrlimit(resource.RLIMIT_NOFILE)
        status, log = self.run_benchmark(self.command(",".join(map(str, ports)), output),
                                       low_descriptor_limit=True)
        self.assertEqual(status, 0, log)
        aggregate = json.loads((output / "aggregate.json").read_text())
        native = json.loads((output / "local" / "summary.json").read_text())
        self.assertTrue(aggregate["completed"])
        self.assertTrue(aggregate["passed"])
        self.assertFalse(native["native_metrics"])
        expected_hosts = [f"http://127.0.0.1:{port}" for port in ports]
        self.assertCountEqual([host.rstrip("/") for host in native["hosts"]], expected_hosts)
        self.assertCountEqual([host.rstrip("/") for host in aggregate["hosts"]], expected_hosts)
        self.assertEqual(aggregate["maximum_users"], 8)
        self.assertEqual(self.source_ips, {"127.0.0.1", "127.0.0.2"})
        combinations = {(source_ip, port): 2 for source_ip in self.source_ips for port in ports}
        self.assertEqual(self.connection_combinations, set(combinations))
        if os.environ.get("METRICS_PARENT_NETWORK_NAMESPACE"):
            self.assertEqual(self.source_ports, {44000, 44001})

        # Startup jitter is at most five seconds. Every user must own a socket
        # after that warmup, even when no request handler is doing any work.
        hold_start = datetime.fromisoformat(native["hold_started_at"].replace("Z", "+00:00")).timestamp()
        hold_end = datetime.fromisoformat(native["hold_finished_at"].replace("Z", "+00:00")).timestamp()
        late_hold = [row for row in self.observations if hold_start + 6 <= row[0] < hold_end - 0.25]
        self.assertGreater(len(late_hold), 10, "the fixture must observe the established hold interval")
        self.assertTrue(all(sockets == 8 for _, sockets, _ in late_hold), late_hold)
        self.assertTrue(any(sockets == 8 and busy == 0 for _, sockets, busy in late_hold))
        late_topology = [distribution for when, distribution in self.topology_observations
                         if hold_start + 6 <= when < hold_end - 0.25]
        self.assertTrue(all(distribution == combinations for distribution in late_topology), late_topology)

        seconds = aggregate["duration_s"]
        for endpoint in aggregate["endpoints"].values():
            self.assert_rates(endpoint, seconds)
        self.assertGreater(aggregate["endpoints"]["GET /feed"]["requests"], 0)
        self.assertGreater(aggregate["endpoints"]["GET /posts/:id"]["requests"], 0)
        self.assertAlmostEqual(aggregate["request_rates"]["requests_per_second"],
                               aggregate["aggregate"]["requests"] / seconds)

        connections = aggregate["connections"]
        raw = json.loads((output / "local" / "connection-samples.json").read_text())
        self.assertEqual(connections["established"], 8)
        self.assertEqual(connections["failed"], 0)
        self.assertEqual(len(raw["successful_ms"]), 8)
        self.assertTrue(all(duration >= 0 for duration in raw["successful_ms"]))
        self.assertEqual(connections["setup_duration_ms"]["samples"], 8)
        self.assert_setup_percentile(connections["setup_duration_ms"], raw["successful_ms"])
        self.assertAlmostEqual(connections["connections_per_second"], 8 / seconds)

        generator = aggregate["generator_telemetry"]["local"]
        target = aggregate["target_telemetry"]
        for report, role in ((generator, "generator"), (target, "application")):
            process = report["processes"][role]["processes"][0]
            self.assertGreaterEqual(process["cpu_seconds"], 0)
            self.assertGreaterEqual(process["cpu_percent_of_one_core"], 0)
            self.assertGreater(process["peak_resident_bytes"], 0)
            self.assertGreaterEqual(process["peak_open_descriptors"], 8)
            self.assertGreaterEqual(process["peak_established_sockets"], 8)
            self.assertIsNotNone(process["descriptor_limits"]["soft"])
            self.assertIsNotNone(report["limits"]["ephemeral_port_range"]["value"])
            if os.environ.get("METRICS_PARENT_NETWORK_NAMESPACE"):
                self.assertNotEqual(report["network_namespace"], os.environ["METRICS_PARENT_NETWORK_NAMESPACE"])
                self.assertEqual(report["limits"]["ephemeral_port_range"]["value"].split(), ["44000", "44001"])
            self.assertGreater(report["network"]["lo"]["receive_bits_per_second"], 0)
            self.assertGreater(report["network"]["lo"]["transmit_packets_per_second"], 0)
        self.assertEqual(target["processes"]["application"]["processes"][0]["pid"], os.getpid())
        generator_limits = generator["processes"]["generator"]["processes"][0]["descriptor_limits"]
        inherited_hard = application_limits[1]
        expected_generator_limit = (
            524288 if inherited_hard == resource.RLIM_INFINITY else min(524288, inherited_hard)
        )
        self.assertEqual(int(generator_limits["soft"]), expected_generator_limit)
        self.assertEqual(resource.getrlimit(resource.RLIMIT_NOFILE), application_limits)
        application_report_limit = target["processes"]["application"]["processes"][0]["descriptor_limits"]["soft"]
        self.assertEqual(application_report_limit,
                         "unlimited" if application_limits[0] == resource.RLIM_INFINITY else str(application_limits[0]))
        self.assertEqual(target["processes"]["proxy"]["processes"], [])
        self.assertIsInstance(target["processes"]["proxy"]["unavailable_reason"], str)

        hosts = aggregate["network_hosts"]["hosts"]
        self.assertEqual(len(hosts), 1, "target and generator share one network namespace")
        host = next(iter(hosts.values()))
        self.assertCountEqual(host["observed_by"], ["local", "target"])
        self.assertGreater(host["duration_s"], 0)
        self.assertGreater(host["network"]["lo"]["receive_bits_per_second"], 0)
        widest = max((generator, target), key=lambda report: report["observed_duration_s"])
        self.assertEqual(host["network"], widest["network"])

        trace = [json.loads(line) for line in (output / "local" / "telemetry.jsonl").read_text().splitlines()]
        self.assertGreater(len(trace), 2)
        intervals = [point["interval_rates"] for point in trace[1:]]
        self.assertTrue(any(point["activity"]["endpoints"]["GET /feed"]["requests_per_second"] > 0
                            for point in intervals))
        self.assertTrue((output / "target" / "telemetry.jsonl").is_file())
        report = (output / "report.md").read_text()
        self.assertIn("GET /feed", report)
        self.assertIn("p99", report.lower())

    def test_generator_continues_when_hard_limit_caps_descriptor_floor(self):
        output = self.root / "capped"
        status, log = self.run_benchmark(self.command(self.server.server_port, output),
                                         descriptor_limits=(32, 4096))
        self.assertEqual(status, 0, log)
        report = json.loads((output / "aggregate.json").read_text())
        soft = report["generator_telemetry"]["local"]["processes"]["generator"]["processes"][0]["descriptor_limits"]["soft"]
        self.assertEqual(soft, "4096")
        output_log = (output / "local" / "output.log").read_text()
        self.assertIn("capped at 4096", output_log)

    def test_generator_fails_before_load_when_hard_limit_cannot_cover_users(self):
        output = self.root / "too-low"
        status, log = self.run_benchmark(self.command(self.server.server_port, output),
                                         descriptor_limits=(32, 100))
        self.assertEqual(status, 2, log)
        self.assertEqual(self.requests, 0)
        worker_log = (output / "local" / "output.log").read_text()
        self.assertIn("below 8 users plus process overhead", worker_log)

    def test_refused_connections_preserve_failure_rates_and_setup_durations(self):
        # Bind without listening so Linux rejects connects; retain the bound
        # socket so another process cannot claim this port during the test.
        with socket.socket() as refused:
            refused.bind(("127.0.0.1", 0))
            output = self.root / "refused"
            status, log = self.run_benchmark(
                self.command(refused.getsockname()[1], output, users=2, duration="30s"),
                interrupt_after_failure=True,
            )
        self.assertNotEqual(status, 0, log)
        aggregate = json.loads((output / "aggregate.json").read_text())
        native = json.loads((output / "local" / "summary.json").read_text())
        self.assertFalse(native["native_metrics"])
        self.assertFalse(aggregate["passed"])
        connections = aggregate["connections"]
        raw = json.loads((output / "local" / "connection-samples.json").read_text())
        self.assertEqual(connections["established"], 0)
        self.assertGreater(connections["failed"], 0)
        self.assertEqual(connections["failed"], len(raw["failed_ms"]))
        self.assertEqual(connections["failed_setup_duration_ms"]["samples"], len(raw["failed_ms"]))
        self.assert_setup_percentile(connections["failed_setup_duration_ms"], raw["failed_ms"])
        self.assertTrue(connections["errors"])
        self.assertAlmostEqual(connections["failed_per_second"], connections["failed"] / aggregate["duration_s"])
        feed = aggregate["endpoints"]["GET /feed"]
        self.assertGreater(feed["failed"], 0)
        self.assertEqual(feed["requests"], feed["failed"])
        self.assert_rates(feed, aggregate["duration_s"])


@unittest.skipUnless(os.environ.get("FEED_LOAD_BIN"), "set FEED_LOAD_BIN to test the built coordinator")
class CapacityNamespaceTests(unittest.TestCase):
    def test_two_ephemeral_ports_hold_eight_connections_across_source_and_destination_pairs(self):
        unshare, ip = shutil.which("unshare"), shutil.which("ip")
        if sys.platform != "linux" or not unshare or not ip:
            self.skipTest("requires Linux, unshare and ip for an isolated port-range test")
        env = dict(os.environ, METRICS_NAMESPACE_IP=ip,
                   METRICS_PARENT_NETWORK_NAMESPACE=os.readlink("/proc/self/ns/net"))
        command = [unshare, "--user", "--map-root-user", "--net", sys.executable,
                   str(Path(__file__).resolve())]
        probe = subprocess.run([*command, "--namespace-capacity-probe"], env=env,
                               capture_output=True, text=True, timeout=15)
        if probe.returncode:
            reason = (probe.stderr or probe.stdout).strip()
            self.skipTest(f"isolated network namespace unavailable: {reason[-500:]}")
        child = subprocess.run([*command, "--namespace-capacity-child"], env=env,
                               capture_output=True, text=True, timeout=120)
        self.assertEqual(child.returncode, 0, child.stdout + child.stderr)


def prepare_capacity_namespace():
    if os.readlink("/proc/self/ns/net") == os.environ["METRICS_PARENT_NETWORK_NAMESPACE"]:
        raise RuntimeError("refusing to change the parent network namespace's ephemeral port range")
    subprocess.run([os.environ["METRICS_NAMESPACE_IP"], "link", "set", "lo", "up"], check=True)
    port_range = Path("/proc/sys/net/ipv4/ip_local_port_range")
    port_range.write_text("44000 44001\n")
    if port_range.read_text().split() != ["44000", "44001"]:
        raise RuntimeError("isolated namespace did not apply the two-port range")


if __name__ == "__main__":
    if sys.argv[1:] == ["--namespace-capacity-probe"]:
        prepare_capacity_namespace()
    elif sys.argv[1:] == ["--namespace-capacity-child"]:
        prepare_capacity_namespace()
        os.environ["METRICS_SERVER_PORTS"] = "31001,31002"
        unittest.main(argv=[sys.argv[0], "MetricsTests.test_report_counts_actual_connections_held_while_users_sleep"])
    else:
        unittest.main()
