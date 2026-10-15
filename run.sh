#!/usr/bin/env bash
set -euo pipefail

export BENCH_CALLER_DIR="$PWD"
bench_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd -- "$bench_dir"

usage() { echo "Usage: $0 [IP PORTS] [OPTIONS]. Run '$0 --help' to list options." >&2; exit 2; }

validate_ports() {
  local value=$1 item start end
  local -a items
  if [[ $value == \[* || $value == *\] ]]; then
    [[ $value == \[*\] ]] || return 1
    value=${value:1:${#value}-2}
  fi
  [[ -n $value ]] || return 1
  [[ $value != ,* && $value != *, && $value != *,,* ]] || return 1
  IFS=, read -r -a items <<< "$value"
  for item in "${items[@]}"; do
    if [[ $item =~ ^[[:space:]]*([0-9]+)[[:space:]]*-[[:space:]]*([0-9]+)[[:space:]]*$ ]]; then
      start=${BASH_REMATCH[1]}
      end=${BASH_REMATCH[2]}
    elif [[ $item =~ ^[[:space:]]*([0-9]+)[[:space:]]*$ ]]; then
      start=${BASH_REMATCH[1]}
      end=$start
    else
      return 1
    fi
    [[ ${#start} -le 5 && ${#end} -le 5 ]] || return 1
    (( 10#$start >= 1 && 10#$start <= 65535 && 10#$end >= 1 && 10#$end <= 65535 && 10#$start <= 10#$end )) || return 1
  done
}

# Keep BASE_URL defaults available, or accept an explicit target IP/port.
if [[ $# -gt 0 && $1 != -* ]]; then
  if [[ $# -lt 2 ]] || ! validate_ports "$2"; then
    usage
  fi
  target_ports=$2
  first_port=${target_ports#[}
  first_port=${first_port%]}
  first_port=${first_port%%,*}
  first_port=${first_port%%-*}
  first_port=${first_port//[[:space:]]/}
  target_ip=$1
  if [[ $target_ip == \[*\] ]]; then
    target_ip=${target_ip:1:${#target_ip}-2}
  fi
  if [[ $target_ip == *:* ]]; then
    [[ $target_ip =~ ^[[:xdigit:].:]+$ && $target_ip == *:*:* ]] || usage
    target_ip="[$target_ip]"
  else
    [[ $target_ip =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] || usage
    IFS=. read -r -a octets <<< "$target_ip"
    for octet in "${octets[@]}"; do
      [[ $octet == 0 || $octet != 0* ]] || usage
      (( 10#$octet <= 255 )) || usage
    done
  fi
  export BASE_URL="http://$target_ip:$first_port"
  export DESTINATION_PORTS="$target_ports"
  shift 2
fi

# Resolve caller-relative files before switching to this repository's directory.
if [[ -n ${TOKENS:-} && $TOKENS != /* ]]; then
  export TOKENS="$BENCH_CALLER_DIR/$TOKENS"
elif [[ -z ${TOKENS:-} ]]; then
  export TOKENS="$BENCH_CALLER_DIR/seed/tokens.json"
fi
if [[ -n ${SUMMARY:-} && $SUMMARY != /* ]]; then
  export SUMMARY="$BENCH_CALLER_DIR/$SUMMARY"
elif [[ -z ${SUMMARY:-} ]]; then
  export SUMMARY="$BENCH_CALLER_DIR/results/summary.json"
fi

# Give the launcher and its local generator a useful descriptor floor. This
# affects only this process tree; the native generator checks its own limit.
descriptor_floor=524288
soft_nofile=$(ulimit -Sn)
hard_nofile=$(ulimit -Hn)
if [[ $soft_nofile != unlimited ]] && (( soft_nofile < descriptor_floor )); then
  if [[ $hard_nofile == unlimited ]] || (( hard_nofile >= descriptor_floor )); then
    ulimit -S -n "$descriptor_floor"
  else
    if (( hard_nofile > soft_nofile )); then
      ulimit -S -n "$hard_nofile"
    fi
    echo "Generator descriptor limit capped at hard limit $hard_nofile; increase it to reach $descriptor_floor" >&2
  fi
fi

# Host tuning is unnecessary for help/version requests and must never lower a
# larger conntrack table setting.
skip_conntrack_tuning=0
for arg in "$@"; do
  if [[ $arg == --help || $arg == --help=* || $arg == -h || $arg == --version || $arg == -V ]]; then
    skip_conntrack_tuning=1
    break
  fi
done

conntrack_max=/proc/sys/net/netfilter/nf_conntrack_max
if (( ! skip_conntrack_tuning )) && [[ -r $conntrack_max ]]; then
  effective_nofile=$(ulimit -Sn)
  if [[ $effective_nofile == unlimited ]]; then
    effective_nofile=$(< /proc/sys/fs/nr_open)
  fi
  current_conntrack_max=$(< "$conntrack_max")
  if (( current_conntrack_max < effective_nofile )); then
    if ! command -v sysctl >/dev/null; then
      echo "Conntrack is enabled but sysctl is unavailable; raise nf_conntrack_max to $effective_nofile as root" >&2
      exit 1
    fi
    privilege=()
    if (( EUID != 0 )); then
      if command -v doas >/dev/null; then
        privilege=(doas)
      elif command -v sudo >/dev/null; then
        privilege=(sudo)
      else
        echo "Conntrack is enabled but nf_conntrack_max is $current_conntrack_max; raise it to $effective_nofile as root" >&2
        exit 1
      fi
    fi
    if ! "${privilege[@]}" sysctl -w "net.netfilter.nf_conntrack_max=$effective_nofile" >/dev/null; then
      echo "Conntrack is enabled but nf_conntrack_max could not be raised from $current_conntrack_max to $effective_nofile" >&2
      exit 1
    fi
    echo "Raised nf_conntrack_max from $current_conntrack_max to $effective_nofile" >&2
  fi
fi

rust_version=1.99.0
cargo_command=(cargo)
rustc_command=(rustc)
if ! command -v cargo >/dev/null || ! command -v rustc >/dev/null ||
   [[ $(rustc --version) != "rustc $rust_version "* || $(cargo --version) != "cargo $rust_version "* ]]; then
  if ! command -v rustup >/dev/null; then
    export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
    if ! command -v rustup >/dev/null; then
      curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
        sh -s -- -y --no-modify-path --default-toolchain none
    fi
  fi
  if ! rustup run "$rust_version" rustc --version >/dev/null 2>&1; then
    rustup toolchain install "$rust_version" --profile minimal
  fi
  cargo_command=(rustup run "$rust_version" cargo)
  rustc_command=(rustup run "$rust_version" rustc)
fi

# Select the host explicitly so inherited Cargo targets cannot select a stale binary.
rust_details=$("${rustc_command[@]}" -vV)
rust_host=${rust_details#*$'\nhost: '}
rust_host=${rust_host%%$'\n'*}
"${cargo_command[@]}" build --locked --release --target "$rust_host" --manifest-path "$bench_dir/Cargo.toml" --target-dir "$bench_dir/target"
exec "$bench_dir/target/$rust_host/release/twelve-dollar-benchmark" --orchestrate "$@"
