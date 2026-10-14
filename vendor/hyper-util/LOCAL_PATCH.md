# Local connector patch

This is the published `hyper-util` 0.1.21 package from crates.io, based on
upstream commit `23a868965964c1d6bb1b94f30ba4c420a4bebe7c`:
<https://github.com/hyperium/hyper-util/tree/23a868965964c1d6bb1b94f30ba4c420a4bebe7c>.
The upstream MIT license and package sources are retained.

The source-binding changes are in `src/client/legacy/connect/http.rs`: on Linux and
Android, explicit source-IP binding sets `IP_BIND_ADDRESS_NO_PORT` before
`bind(source_ip, 0)`. The kernel chooses the ephemeral port at `connect`,
allowing source ports to be shared across distinct destination addresses and
ports. Other platforms and connections without a matching explicit source IP
keep upstream behavior. Failure to set the option follows the existing
connector error path rather than silently reducing capacity.

The additional IPv4 and IPv6 unit tests check that binding retains port zero,
connecting allocates a port, and the peer sees the requested source IP.
No global socket interposition or kernel configuration changes are involved.

The benchmark opts into `set_source_port_range(1024, 65535)` before making
requests. A process-wide counter for each source/destination tuple chooses
explicit ports. `SO_REUSEADDR` permits reuse across different destinations;
the kernel continues to reject duplicate established four-tuples. Bind/connect
collisions advance the counter, retries are bounded to the range, and long
collision runs yield to the runtime. Connections without an explicit source
address retain normal route selection. Other consumers keep the original
allocation behavior unless they enable this range. Tests cover the range,
wraparound, sharing and collisions.

`client/legacy/client.rs` also explicitly discards the HTTP/2 timer argument
when HTTP/2 is disabled, avoiding an unused-variable warning in HTTP-only builds.
