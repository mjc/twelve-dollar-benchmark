{pkgs, ...}: {
  languages.rust = {
    enable = true;
    channel = "stable";
    version = "1.99.1";
    components = ["cargo" "rustc" "clippy" "rustfmt" "rust-src" "rust-analyzer"];
  };

  packages = with pkgs; [
    curl
    openssh
  ];
}
