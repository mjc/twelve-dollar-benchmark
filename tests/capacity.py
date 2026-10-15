"""Exercise a built generator against a disposable, constant-response HTTP server.

Run inside a fresh network namespace for 300k connections, so conntrack and
source ports remain isolated from other host traffic. This does not use the app.
"""

import argparse
import asyncio
import json
from pathlib import Path
import resource
import signal
import subprocess
import sys


async def serve(stats_path):
    active = {}
    peaks = {}
    requests = 0
    active_total = 0
    peak_total = 0

    class Handler(asyncio.Protocol):
        def connection_made(self, transport):
            nonlocal active_total, peak_total
            self.transport = transport
            self.buffer = bytearray()
            self.port = transport.get_extra_info("sockname")[1]
            active[self.port] = active.get(self.port, 0) + 1
            peaks[self.port] = max(peaks.get(self.port, 0), active[self.port])
            active_total += 1
            peak_total = max(peak_total, active_total)

        def connection_lost(self, _error):
            nonlocal active_total
            active[self.port] -= 1
            active_total -= 1

        def data_received(self, data):
            nonlocal requests
            self.buffer.extend(data)
            while b"\r\n\r\n" in self.buffer:
                header, body = self.buffer.split(b"\r\n\r\n", 1)
                length = next((int(row.split(b":", 1)[1]) for row in header.split(b"\r\n")
                               if row.lower().startswith(b"content-length:")), 0)
                if len(body) < length:
                    return
                self.buffer = bytearray(body[length:])
                requests += 1
                method, path, _version = header.split(b"\r\n", 1)[0].split(b" ")
                payload = b'{"posts":[{"id":1}]}' if path == b"/feed" else b'{"post":{"id":1}}'
                status = b"201 Created" if method == b"POST" else b"200 OK"
                self.transport.write(b"HTTP/1.1 " + status + b"\r\nContent-Type: application/json\r\nContent-Length: "
                                     + str(len(payload)).encode() + b"\r\n\r\n" + payload)

    loop = asyncio.get_running_loop()
    servers = [await loop.create_server(Handler, "127.0.0.1", 0, backlog=65535) for _ in range(5)]
    print(json.dumps([server.sockets[0].getsockname()[1] for server in servers]), flush=True)
    stop = asyncio.Event()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    while not stop.is_set():
        stats_path.write_text(json.dumps({"active": active_total, "peak_by_port": peaks,
                                         "peak_total": peak_total, "requests": requests}))
        try:
            await asyncio.wait_for(stop.wait(), 1)
        except TimeoutError:
            pass
    for server in servers:
        server.close()
        await server.wait_closed()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--users", type=int, default=300000)
    args = parser.parse_args()
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (max(soft, min(hard, 700000)), hard))
    if args.server:
        asyncio.run(serve(args.server))
        return
    args.output.mkdir(parents=True, exist_ok=False)
    stats = args.output / "server-stats.json"
    tokens = args.output / "tokens.json"
    tokens.write_text(json.dumps([{"username": "fixture", "token": "fixture"}]))
    server = subprocess.Popen([sys.executable, __file__, "--server", str(stats)], stdout=subprocess.PIPE, text=True)
    try:
        ports = json.loads(server.stdout.readline())
        command = [str(args.binary.resolve()), "--orchestrate", "127.0.0.1", ",".join(map(str, ports)),
                   "--users", str(args.users), "--ramp-up", "60s", "--duration", "10s",
                   "--source-ips", "127.0.0.2", "--tokens", str(tokens.resolve()),
                   "--output-dir", str((args.output / "run").resolve()), "--sample-interval", "5"]
        with (args.output / "coordinator.log").open("w") as log:
            status = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT).returncode
        report = json.loads((args.output / "run" / "aggregate.json").read_text())
        observed = json.loads(stats.read_text())
        result = {"benchmark_exit": status, "users": report["maximum_users"],
                  "connections": report["connections"], "server": observed}
        (args.output / "capacity.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
        assert report["maximum_users"] == args.users, "generator did not reach requested users"
        assert report["connections"]["established"] >= args.users, "missing established connections"
        assert observed["peak_total"] >= args.users, "server did not observe full connection capacity"
        assert report["connections"]["failed"] == 0, "connection attempts failed"
    finally:
        server.terminate()
        server.wait(timeout=10)


if __name__ == "__main__":
    main()
