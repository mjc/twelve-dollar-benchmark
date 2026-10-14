# Local connector patch

This is the published `hyper-util` 0.1.21 package from crates.io, based on
upstream commit `23a868965964c1d6bb1b94f30ba4c420a4bebe7c`:
<https://github.com/hyperium/hyper-util/tree/23a868965964c1d6bb1b94f30ba4c420a4bebe7c>.
The upstream MIT license and package sources are retained.

The only code change is in `src/client/legacy/connect/http.rs`: on Linux and
Android, explicit source-IP binding sets `IP_BIND_ADDRESS_NO_PORT` before
`bind(source_ip, 0)`. The kernel chooses the ephemeral port at `connect`,
allowing source ports to be shared across distinct destination addresses and
ports. Other platforms and connections without a matching explicit source IP
keep upstream behavior. Failure to set the option follows the existing
connector error path rather than silently reducing capacity.

The additional IPv4 and IPv6 unit tests check that binding retains port zero,
connecting allocates a port, and the peer sees the requested source IP.
No global socket interposition or kernel configuration changes are involved.
