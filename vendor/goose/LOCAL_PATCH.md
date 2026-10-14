# Local patch

Source: the published Goose 0.18.1 package from crates.io, upstream commit
`7d2a470a3f9051d1f4fdb72c841001c09324858e`. The original license and package
provenance are retained. The following published paths are omitted:

- `.cargo-ok` and `.cargo-checksum.json`: registry cache markers.
- `.github/`: upstream GitHub automation.
- `.clinerules`, `context7.json`, `memory-bank/`, and `sample_goosebot_*.md`:
  upstream assistant configuration and review samples, unused by Cargo.
- `src/docs/goose-book/`: the standalone mdBook site, including its Markdown,
  configuration, and image assets. Rust sources do not include these files;
  Cargo library, example, and test targets do not reference this directory.

All Rust modules, explicit example/test targets, manifests, README, and license
are retained.

`create_reqwest_client` calls `danger_accept_invalid_certs` only when Goose's
`default` or `rustls-tls` feature is enabled. Reqwest does not expose that method
without a TLS backend, so the unconditional call prevented this HTTP benchmark
from compiling without TLS. TLS-enabled builds preserve the existing certificate
validation setting.

`GooseAttack::set_user_client_factory` is an opt-in hook for providing each user's
final independent client at spawn. It stores pending users as weighted scenario
indices, validates their hosts before starting, and releases the pending vector
after its last entry is consumed. This avoids preallocating full user state and
clients that the benchmark would immediately replace. Scenario scheduling,
spawn indices, and controller reallocation follow the existing paths. Without
the hook, Goose retains its original eager initialization. If client creation
fails after earlier users have spawned, those tasks are aborted and awaited
before returning the error so they cannot continue generating load.

Two local unit tests cover compact storage, deferred factory calls, weighted
spawn order, factory indices, storage release, early host validation, and
factory error propagation.

Remove these patches when upstream Goose supports the TLS-disabled build and
equivalent user/client initialization.
