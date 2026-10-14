{pkgs, ...}: {
  languages.rust = {
    enable = true;
    channel = "stable";
    version = "1.99.0";
    components = ["cargo" "rustc" "clippy" "rustfmt" "rust-src" "rust-analyzer"];
  };

  packages = with pkgs; [
    curl
    git
    openssh
    pkg-config
    python3
  ] ++ pkgs.lib.optionals pkgs.stdenv.isLinux [iproute2 util-linux];

  tasks."benchmark:check" = {
    description = "Run the same Rust and Python checks as CI";
    exec = ''
      set -euo pipefail
      cargo fmt --check
      cargo clippy --locked --all-targets -- -D warnings
      cargo test --locked
      cargo build --locked
      FEED_LOAD_BIN=target/debug/feed-load python3 tests/test_runner.py
      FEED_LOAD_BIN=target/debug/feed-load python3 tests/test_metrics.py
    '';
  };
}
