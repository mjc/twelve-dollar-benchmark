"""Exercise the runner with local tool stand-ins; no downloads or load traffic."""
import json
import http.server
import threading
import time
import signal
import os
from pathlib import Path
import shlex
import shutil
import resource
import subprocess
import sys
import tempfile
import unittest

RUNNER = Path(__file__).resolve().parents[1] / "run.sh"
DRIVER = r'''
import json, os, pathlib, resource, shlex, sys
args = sys.argv[1:]
name = pathlib.Path(sys.argv[0]).name
root = pathlib.Path(os.environ["FIXTURE"])
with (root / "calls").open("a") as log:
    log.write(json.dumps([name, *args]) + "\n")
if args and args[0] == "--install-rustup":
    bindir = pathlib.Path(os.environ["CARGO_HOME"]) / "bin"
    bindir.mkdir(parents=True, exist_ok=True)
    (bindir / "rustup").symlink_to(root / "driver")
elif args and args[0] == "--run-load":
    if args[1:2] == ["--orchestrate"]:
        args.pop(1)
    (root / "result").write_text(json.dumps({
        "cwd": os.getcwd(), "host": os.environ.get("BASE_URL"),
        "tokens": os.environ.get("TOKENS"), "summary": os.environ.get("SUMMARY"),
        "source_ips": os.environ.get("SOURCE_IPS"), "destination_ports": os.environ.get("DESTINATION_PORTS"), "args": args[1:],
        "nofile": resource.getrlimit(resource.RLIMIT_NOFILE),
    }))
    sys.exit(7)
elif name == "git":
    print(root / "main" / ".git")
elif name == "curl":
    print("exec " + shlex.join([sys.executable, str(root / "driver"), "--install-rustup"]))
elif name == "rustc" or args == ["--version"]:
    version = "1.99.0" if os.environ["MODE"] == "native" else "1.98.0"
    print(name + " " + version + " (fixture)")
    if args == ["-vV"]:
        print("host: fixture-host")
else:
    if name == "rustup":
        if args[:2] == ["toolchain", "install"]:
            (root / "installed").touch()
            sys.exit(0)
        assert args[:2] == ["run", "1.99.0"], args
        if args[2] == "rustc":
            if args[3:] == ["-vV"]:
                print("rustc 1.99.0 (fixture)\nhost: fixture-host")
            sys.exit(0 if (root / "installed").exists() else 1)
        assert args[2] == "cargo", args
        args = args[3:]
    assert args[0] == "build", args
    assert "--locked" in args and "--release" in args
    assert pathlib.Path(args[args.index("--manifest-path") + 1]).is_file()
    assert args[args.index("--target") + 1] == "fixture-host", args
    target = pathlib.Path(args[args.index("--target-dir") + 1]) / "fixture-host" / "release"
    target.mkdir(parents=True, exist_ok=True)
    binary = target / "feed-load"
    binary.write_text("#!" + os.environ["BASH"] + "\nexec " +
        shlex.join([sys.executable, str(root / "driver"), "--run-load"]) + ' "$@"\n')
    binary.chmod(0o755)
'''


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="goose runner ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bench = self.root / "checkout"
        self.bench.mkdir(parents=True)
        shutil.copyfile(RUNNER, self.bench / "run.sh")
        (self.bench / "Cargo.toml").touch()
        (self.root / "other").mkdir()
        (self.root / "main" / "seed").mkdir(parents=True)
        (self.root / "main" / ".git").mkdir()
        (self.root / "main" / "seed" / "tokens.json").write_text("[]")
        driver = self.root / "driver"
        driver.write_text("#!" + sys.executable + "\n" + DRIVER)
        driver.chmod(0o755)
        bindir = self.root / "bin"
        bindir.mkdir()
        for tool in ["bash", "sh", "dirname"]:
            (bindir / tool).symlink_to(shutil.which(tool))
        for tool in ["cargo", "rustc", "git", "curl"]:
            (bindir / tool).symlink_to(driver)
        self.env = dict(os.environ, PATH=str(bindir), HOME=str(self.root),
                        CARGO_HOME=str(self.root / "cargo home"), MODE="native",
                        FIXTURE=str(self.root), BASH=shutil.which("bash"),
                        CARGO_BUILD_TARGET="other-target")
        for name in ["TOKENS", "SUMMARY", "BASE_URL", "SOURCE_IPS"]:
            self.env.pop(name, None)
        self.env["TOKENS"] = str(self.root / "main" / "seed" / "tokens.json")

    def run_runner(self, cwd, *args, limits=None):
        command = [self.env["BASH"], str(self.bench / "run.sh"), *args]
        if limits:
            command = [sys.executable, "-c",
                       "import os,resource,sys; resource.setrlimit(resource.RLIMIT_NOFILE, tuple(map(int,sys.argv[1:3]))); os.execv(sys.argv[3],sys.argv[3:])",
                       *map(str, limits), *command]
        return subprocess.run(command,
                              cwd=cwd, env=self.env, capture_output=True, text=True)

    def test_launcher_raises_soft_limit_and_preserves_hard_limit(self):
        hard = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
        if hard == resource.RLIM_INFINITY:
            hard = 1_000_000
        if hard < 524288:
            self.skipTest("host hard limit cannot accommodate the descriptor floor")
        run = self.run_runner(self.bench, limits=(32, hard))
        self.assertEqual(run.returncode, 7, run.stderr)
        soft, actual_hard = json.loads((self.root / "result").read_text())["nofile"]
        self.assertEqual(soft, 524288)
        self.assertEqual(actual_hard, hard)

    def test_launcher_preserves_already_higher_soft_limit(self):
        hard = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
        if hard == resource.RLIM_INFINITY:
            hard = 1_000_000
        if hard < 600000:
            self.skipTest("host hard limit cannot accommodate the higher soft limit")
        run = self.run_runner(self.bench, limits=(600000, hard))
        self.assertEqual(run.returncode, 7, run.stderr)
        soft, actual_hard = json.loads((self.root / "result").read_text())["nofile"]
        self.assertEqual(soft, 600000)
        self.assertEqual(actual_hard, hard)

    def test_launcher_caps_at_hard_limit_and_warns(self):
        run = self.run_runner(self.bench, limits=(32, 4096))
        self.assertEqual(run.returncode, 7, run.stderr)
        soft, hard = json.loads((self.root / "result").read_text())["nofile"]
        self.assertEqual((soft, hard), (4096, 4096))
        self.assertIn("capped at hard limit 4096", run.stderr)

    def calls(self):
        return [json.loads(line) for line in (self.root / "calls").read_text().splitlines()]

    def test_target_paths_flags_and_exit_status_from_any_directory(self):
        stale = self.bench / "target" / "release" / "feed-load"
        stale.parent.mkdir(parents=True)
        stale.write_text("#!" + self.env["BASH"] + "\nexit 99\n")
        stale.chmod(0o755)
        for cwd, target, host in [
            (self.bench.parent, ["127.0.0.1", "3000"], "http://127.0.0.1:3000"),
            (self.bench, ["::1", "8080"], "http://[::1]:8080"),
            (self.bench, ["[::1]", "8080"], "http://[::1]:8080"),
            (self.root / "other", [], "http://example.test:9000"),
        ]:
            with self.subTest(cwd=cwd):
                self.env.update(BASE_URL="http://example.test:9000", SOURCE_IPS="127.0.0.[1-5]",
                                SUMMARY="../results/custom.json")
                run = self.run_runner(cwd, *target, "--request-log", "file with spaces")
                self.assertEqual(run.returncode, 7, run.stderr)
                result = json.loads((self.root / "result").read_text())
                self.assertEqual(result["host"], host)
                self.assertEqual(result["cwd"], str(self.bench))
                self.assertEqual(Path(result["tokens"]).resolve(), self.root / "main/seed/tokens.json")
                self.assertEqual(result["summary"], str(cwd / "../results/custom.json"))
                self.assertEqual(result["source_ips"], "127.0.0.[1-5]")
                self.assertEqual(result["args"], ["--request-log", "file with spaces"])
        self.assertFalse(any(call[0] in ["curl", "rustup"] for call in self.calls()))

    def test_rustup_bootstrap_and_pinned_toolchain_reuse(self):
        self.env.update(MODE="old", TOKENS="../custom-tokens.json")
        run = self.run_runner(self.bench.parent, "127.0.0.1", "3000")
        self.assertEqual(run.returncode, 7, run.stderr)
        calls = self.calls()
        self.assertTrue(any(call[0] == "curl" for call in calls))
        self.assertIn(["rustup", "toolchain", "install", "1.99.0", "--profile", "minimal"], calls)
        self.assertTrue(any(call[:4] == ["rustup", "run", "1.99.0", "cargo"] for call in calls))
        self.assertEqual(json.loads((self.root / "result").read_text())["tokens"], str(self.bench.parent / "../custom-tokens.json"))
        (self.root / "calls").unlink()
        run = self.run_runner(self.bench, "127.0.0.1", "3000")
        self.assertEqual(run.returncode, 7, run.stderr)
        self.assertFalse(any(call[0] == "curl" or call[1:3] == ["toolchain", "install"]
                             for call in self.calls()))

    def test_destination_lists_and_ranges_are_forwarded_from_repo_root(self):
        for ports, first in [("3100,3102,3104", "3100"), ("3100-3104", "3100"),
                             ("[3100-3104]", "3100"), ("3100, 3102 - 3104", "3100")]:
            with self.subTest(ports=ports):
                run = self.run_runner(self.bench.parent, "127.0.0.1", ports)
                self.assertEqual(run.returncode, 7, run.stderr)
                result = json.loads((self.root / "result").read_text())
                self.assertEqual(result["host"], f"http://127.0.0.1:{first}")
                self.assertEqual(result["destination_ports"], ports)

    def test_invalid_target_stops_before_building(self):
        for args in [["127.0.0.1"], ["127.0.0.1", "0"], ["127.0.0.1", "65536"],
                     ["127.0.0.1", "not-a-port"], ["127.0.0.1/path", "3000"],
                     ["127.0.0.1@192.0.2.1", "3000"], ["127.0.0.1:80", "3000"],
                     ["127.0.0.999", "3000"], ["010.0.0.1", "3000"], ["::1/path", "3000"],
                     ["127.0.0.1", "3100,0"], ["127.0.0.1", "3100,"],
                     ["127.0.0.1", "3100-3099"], ["127.0.0.1", "[3100-3102"]]:
            with self.subTest(args=args):
                run = self.run_runner(self.bench.parent, *args)
                self.assertEqual(run.returncode, 2, run.stderr)
                self.assertIn("Usage:", run.stderr)
        self.assertFalse((self.root / "calls").exists())



REMOTE_DRIVER = r"""
import json, os, pathlib, shutil, subprocess, sys, time
root = pathlib.Path(os.environ["REMOTE_FIXTURE"])
args = sys.argv[1:]
name = pathlib.Path(sys.argv[0]).name
with (root / "calls").open("a") as log:
    log.write(json.dumps([name, *args]) + "\n")
def mapped(value):
    return value.replace("/tmp/feed-load-", str(root / "remote-"))
if name == "ssh":
    host, command = args[args.index("--") + 1:]
    if "--supervise " in command and host.endswith(".2"):
        time.sleep(float(os.environ.get("START_DELAY", "0")))
    command = mapped(command)
    child = subprocess.Popen([os.environ["BASH"], "-c", command], start_new_session=True)
    stat = pathlib.Path(f"/proc/{child.pid}/stat").read_text().split()[21]
    with (root / "processes").open("a") as log:
        log.write(json.dumps([child.pid, stat]) + "\n")
    sys.exit(child.wait())
else:
    paths = args[args.index("--") + 1:]
    if ":/" in paths[-1]:
        if os.environ.get("UPLOAD_FAIL") and "@127.0.0.2:" in paths[-1]:
            sys.exit(9)
        dest = pathlib.Path(mapped("/" + paths[-1].split(":/", 1)[1]))
        shutil.copy2(paths[0], dest)
    else:
        source = pathlib.Path(mapped("/" + paths[0].split(":/", 1)[1]))
        dest = pathlib.Path(paths[-1])
        if os.environ.get("COLLECT_FAIL") and "@127.0.0.2:" in paths[0]:
            sys.exit(9)
        if source.is_dir():
            shutil.copytree(source, dest, dirs_exist_ok=True)
        else:
            shutil.copy2(source, dest)
"""


@unittest.skipUnless(os.environ.get("FEED_LOAD_BIN"), "set FEED_LOAD_BIN to test the built coordinator")
class CoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="goose ssh ")
        self.root = Path(self.temp.name)
        self.addCleanup(self.cleanup)
        self.binary = Path(os.environ["FEED_LOAD_BIN"]).resolve()
        driver = self.root / "driver"
        driver.write_text("#!" + sys.executable + "\n" + REMOTE_DRIVER)
        driver.chmod(0o755)
        bindir = self.root / "bin"
        bindir.mkdir()
        for tool in ["ssh", "scp"]:
            (bindir / tool).symlink_to(driver)
        tokens = self.root / "custom tokens.json"
        tokens.write_text(json.dumps([{"username": "test", "token": "test-token"}]))
        fixture = self
        self.requests = 0
        self.feed_status = 200
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_GET(self):
                fixture.requests += 1
                body = json.dumps({"posts": [{"id": 42}]} if self.path == "/feed" else {"post": {"id": 42}}).encode()
                self.send_response(fixture.feed_status if self.path == "/feed" else 200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                self.send_response(201)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.env = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ["PATH"],
                        REMOTE_FIXTURE=str(self.root), BASH=shutil.which("bash"),
                        TOKENS=str(tokens), VUS="2", RAMP_UP="0s", DURATION="1s",
                        BASE_URL=f"http://127.0.0.1:{self.server.server_port}", SOURCE_IPS="127.0.0.[1-2]")
        for name in ["SUMMARY", "BENCH_START_AT", "BENCH_USER_OFFSET", "BENCH_SAMPLES"]:
            self.env.pop(name, None)

    def cleanup(self):
        if (self.root / "processes").exists():
            for line in (self.root / "processes").read_text().splitlines():
                pid, start = json.loads(line)
                proc = Path(f"/proc/{pid}")
                try:
                    command = (proc / "cmdline").read_bytes()
                    current_start = (proc / "stat").read_text().split()[21]
                    if current_start == start and str(self.root).encode() in command:
                        os.killpg(pid, signal.SIGKILL)
                except (FileNotFoundError, ProcessLookupError):
                    pass
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temp.cleanup()

    def command(self, output="results", remote=True, extra=()):
        return [str(self.binary), "--orchestrate", "--output-dir", str(self.root / output),
                *(["--user", "bench"] if remote else []), *extra]

    def verify_and_signal(self, process, kind, start_time, group=False):
        proc = Path(f"/proc/{process.pid}")
        self.assertEqual((proc / "stat").read_text().split()[21], start_time)
        self.assertEqual((proc / "cmdline").read_bytes().rstrip(b"\0").split(b"\0"),
                         [arg.encode() for arg in process.args])
        if group:
            self.assertEqual(os.getpgid(process.pid), process.pid)
            os.killpg(process.pid, kind)
        else:
            process.send_signal(kind)

    def run_coordinator(self, **kwargs):
        run = subprocess.run(self.command(**kwargs), env=self.env, cwd=self.root,
                             capture_output=True, text=True, timeout=95)
        report = json.loads((self.root / kwargs.get("output", "results") / "aggregate.json").read_text())
        return run, report

    def test_https_targets_and_certificate_options_fail_before_load(self):
        for args, host in [(self.command(), f"https://127.0.0.1:{self.server.server_port}"),
                           ([str(self.binary), "--host", f"https://127.0.0.1:{self.server.server_port}"], None),
                           (self.command(extra=["--", "--accept-invalid-certs"]), None)]:
            with self.subTest(args=args):
                env = dict(self.env)
                if host:
                    env["BASE_URL"] = host
                run = subprocess.run(args, env=env, cwd=self.root,
                                     capture_output=True, text=True, timeout=15)
                self.assertEqual(run.returncode, 2, run.stdout + run.stderr)
                self.assertIn("TLS", run.stderr)
                self.assertEqual(self.requests, 0)
                self.assertFalse((self.root / "results").exists())
                self.assertFalse((self.root / "calls").exists())

    def test_ssh_barrier_collects_all_reports_and_splits_total_users(self):
        self.env["START_DELAY"] = "3"
        second = http.server.ThreadingHTTPServer(("127.0.0.1", 0), self.server.RequestHandlerClass)
        second_thread = threading.Thread(target=second.serve_forever, daemon=True)
        second_thread.start()
        def stop_second():
            second.shutdown()
            second.server_close()
            second_thread.join()
        self.addCleanup(stop_second)
        ports = f"{self.server.server_port},{second.server_port}"
        run, report = self.run_coordinator(extra=["--request-log", "native.log", "--destination-ports", ports])
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr + str(report))
        self.assertTrue(report["passed"])
        self.assertEqual(report["maximum_users"], 2)
        self.assertGreaterEqual(report["common_hold_s"], 0.75)
        self.assertEqual([w["users"] for w in report["workers"]], [1, 1])
        self.assertEqual([w["user_offset"] for w in report["workers"]], [0, 1])
        self.assertCountEqual(report["hosts"], [f"http://127.0.0.1:{port}/" for port in [self.server.server_port, second.server_port]])
        self.assertEqual(report["destination_ports"], [self.server.server_port, second.server_port])
        samples = []
        for index, worker in enumerate(report["workers"]):
            directory = self.root / "results" / worker["name"]
            for filename in ["summary.json", "samples.json", "output.log", "native.log"]:
                self.assertTrue((directory / filename).exists(), filename)
            native = json.loads((directory / "summary.json").read_text())
            self.assertEqual(native["hosts"], [f"http://127.0.0.1:{[self.server.server_port, second.server_port][index]}/"])
            samples.extend(ms for endpoint in json.loads((directory / "samples.json").read_text())
                           for ms in endpoint["latency_ms"])
        self.assertEqual(report["aggregate"]["requests"], len(samples))
        rank = 0.99 * (len(samples) - 1)
        samples.sort()
        lower = int(rank)
        expected = samples[lower] + (samples[min(lower + 1, len(samples) - 1)] - samples[lower]) * (rank - lower)
        self.assertAlmostEqual(report["aggregate"]["p99_ms"], expected)

    def test_failed_upload_never_launches_any_generator(self):
        self.env["UPLOAD_FAIL"] = "1"
        run, report = self.run_coordinator()
        self.assertEqual(run.returncode, 2, run.stdout + run.stderr)
        self.assertFalse(report["passed"])
        calls = [json.loads(line) for line in (self.root / "calls").read_text().splitlines()]
        self.assertFalse(any(call[0] == "ssh" and "--supervise " in call[-1] for call in calls))

    def test_missing_remote_collection_cannot_pass(self):
        self.env["COLLECT_FAIL"] = "1"
        run, report = self.run_coordinator()
        self.assertEqual(run.returncode, 2, run.stdout + run.stderr)
        self.assertFalse(report["completed"])
        self.assertFalse(report["passed"])

    def test_local_run_writes_samples_and_native_output_in_directory(self):
        self.env.update(SOURCE_IPS="127.0.0.[1-2]", SUMMARY=str(self.root / "legacy.json"))
        run, report = self.run_coordinator(remote=False, extra=["--request-log", "native.log"])
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertEqual(report["source_ips"], ["127.0.0.1", "127.0.0.2"])
        self.assertEqual(report, json.loads((self.root / "legacy.json").read_text()))
        for filename in ["summary.json", "samples.json", "output.log", "native.log"]:
            self.assertTrue((self.root / "results" / "local" / filename).exists())

    def test_cancel_stops_remote_supervisors_and_never_passes(self):
        self.env["DURATION"] = "30s"
        log = (self.root / "coordinator.log").open("w")
        with log:
            process = subprocess.Popen(self.command(), env=self.env, cwd=self.root, stdout=log, stderr=log)
            start_time = Path(f"/proc/{process.pid}/stat").read_text().split()[21]
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if self.requests >= 2 and len(list(self.root.glob("remote-*/cancel.ready"))) == 2 and len(list(self.root.glob("remote-*/cancel.start"))) == 2:
                    break
                time.sleep(0.1)
            else:
                process.wait(timeout=95)
                self.fail("supervisors did not start")
            self.verify_and_signal(process, signal.SIGINT, start_time)
            code = process.wait(timeout=35)
        self.assertNotEqual(code, 0)
        report = json.loads((self.root / "results" / "aggregate.json").read_text())
        self.assertFalse(report["passed"])
        self.assertTrue(report["interrupted"])
        self.assertEqual(len(list(self.root.glob("remote-*/cancel.done"))), 2)

    def test_local_interrupt_preserves_collected_samples(self):
        # Failed feeds finish an iteration within seven seconds, inside the grace period.
        self.feed_status = 503
        self.env.update(VUS="1", DURATION="30s")
        log = (self.root / "coordinator.log").open("w")
        with log:
            process = subprocess.Popen(self.command(remote=False), env=self.env, cwd=self.root,
                                       stdout=log, stderr=log, start_new_session=True)
            start_time = Path(f"/proc/{process.pid}/stat").read_text().split()[21]
            deadline = time.monotonic() + 15
            while self.requests == 0 and time.monotonic() < deadline:
                time.sleep(0.1)
            if self.requests == 0:
                process.wait(timeout=60)
                self.fail("local generator did not make a request")
            self.verify_and_signal(process, signal.SIGINT, start_time, group=True)
            code = process.wait(timeout=15)
        self.assertEqual(code, 1, (self.root / "coordinator.log").read_text())
        directory = self.root / "results" / "local"
        raw = json.loads((directory / "samples.json").read_text())
        summary = json.loads((directory / "summary.json").read_text())
        report = json.loads((self.root / "results" / "aggregate.json").read_text())
        self.assertGreater(summary["aggregate"]["requests"], 0)
        self.assertEqual(report["aggregate"]["requests"], sum(len(s["latency_ms"]) for s in raw))
        self.assertFalse(report["passed"])
        self.assertTrue(report["interrupted"])

    def test_sigterm_stops_remote_supervisors_and_never_passes(self):
        self.env["DURATION"] = "30s"
        log = (self.root / "coordinator.log").open("w")
        with log:
            process = subprocess.Popen(self.command(), env=self.env, cwd=self.root,
                                       stdout=log, stderr=log)
            start_time = Path(f"/proc/{process.pid}/stat").read_text().split()[21]
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if self.requests >= 2 and len(list(self.root.glob("remote-*/cancel.start"))) == 2:
                    break
                time.sleep(0.1)
            else:
                process.wait(timeout=95)
                self.fail("remote generators did not make requests")
            self.verify_and_signal(process, signal.SIGTERM, start_time)
            code = process.wait(timeout=35)
        self.assertNotEqual(code, 0)
        report = json.loads((self.root / "results" / "aggregate.json").read_text())
        self.assertFalse(report["passed"])
        self.assertTrue(report["interrupted"])
        self.assertEqual(len(list(self.root.glob("remote-*/cancel.done"))), 2)


if __name__ == "__main__":
    unittest.main()
