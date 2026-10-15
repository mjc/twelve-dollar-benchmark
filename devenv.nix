{pkgs, ...}: {
  languages.rust = {
    enable = true;
    channel = "stable";
    version = "1.99.1";
    components = ["cargo" "rustc" "clippy" "rustfmt" "rust-src" "rust-analyzer"];
  };

  packages = with pkgs; [
    curl
    git
    openssh
    pkg-config
    python3
    iproute2
    util-linux
  ];

  tasks."benchmark:check" = {
    description = "Run the same Rust and Python checks as CI";
    exec = ''
      set -euo pipefail
      cargo fmt --check
      cargo clippy --locked --all-targets -- -D warnings
      cargo test --locked
      cargo build --locked
      TWELVE_DOLLAR_BENCHMARK_BIN=target/debug/twelve-dollar-benchmark python3 tests/test_runner.py
      TWELVE_DOLLAR_BENCHMARK_BIN=target/debug/twelve-dollar-benchmark python3 tests/test_metrics.py
    '';
  };
}
