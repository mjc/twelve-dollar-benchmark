# Patched dependencies

Cargo pins the benchmark-specific Hyper sources to immutable commits:

- [mjc/hyper](https://github.com/mjc/hyper/tree/903ce8f3c54704c148c0967e4374c69707906dc8), based on upstream Hyper 1.12.0 commit `7f67699ce32d717081deb7018b4da7d679277ac0`.
- [mjc/hyper-util](https://github.com/mjc/hyper-util/tree/df185d6b535ca66198ebf8175e14b1ea4db75277), based on upstream hyper-util 0.1.21 commit `23a868965964c1d6bb1b94f30ba4c420a4bebe7c`.

The Hyper fork tunes HTTP/1 read buffering, write-header allocation, and the
first queued body slot. The hyper-util fork optimizes client connection setup,
including Linux/Android source binding with `IP_BIND_ADDRESS_NO_PORT`. Both
forks retain their upstream manifests, licenses, documentation, and tests; the
Cargo patches change only the source revisions, not dependency versions or
features.
