use std::{env, error::Error, net::IpAddr, path::PathBuf, time::Duration};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub(super) struct Config {
    pub base_url: String,
    pub label: Option<String>,
    pub destination_ports: Vec<u16>,
    pub users: usize,
    pub start_users: usize,
    pub step_users: Option<usize>,
    pub max_users: usize,
    pub ramp_up: String,
    pub duration: String,
    pub tokens: PathBuf,
    pub source_ips: String,
    pub user: Option<String>,
    pub monitor_user: Option<String>,
    pub output: Option<PathBuf>,
    pub diagnostics: Vec<String>,
    pub application_pids: Vec<u32>,
    pub proxy_pids: Vec<u32>,
    pub sample_interval: Duration,
    pub help: bool,
    pub score: bool,
    pub skip_warmup: bool,
}

pub(super) const HELP: &str = "Usage: run.sh [IP PORTS] [options] [-- Goose diagnostics]

  --source-ips IPS       Local source IPs, or SSH generator hosts with --user
  --destination-ports PORTS  Destination ports, as a list or inclusive range
  --user USER            Copy the release/tokens and run generators over SSH
  --monitor-user USER    Monitor the target over SSH while generating locally
  --users N             Fixed-load run at N users; disables automatic scoring
  --score               Warm up, find the limit and confirm a five-minute score
  --skip-warmup         Reuse a warm-up already completed in this session
  --start-users N       First search load (2500); positive multiple of 250
  --step-users N        Increase search load by N instead of doubling; multiple of 250
  --max-users N         Search ceiling (300000); positive multiple of 250
  --label NAME          Implementation name in the final Markdown table
  --ramp-up TIME        Ramp to total users (60s)
  --duration TIME       Hold at total users (5m); ramp-down remains 30s
  --tokens PATH         Token file; explicit paths resolve from your directory
  --output-dir DIR      New results directory; explicit paths resolve from your directory
  --application-pids PIDS  Comma-separated application PIDs on the target host
  --proxy-pids PIDS        Comma-separated proxy PIDs on the target host
  --sample-interval SECONDS  Resource/network sampling period (1 second, minimum 0.1)
  --help                Show this runner's help

Target monitoring uses --monitor-user, falling back to --user. Without SSH, target
monitoring is local for loopback targets. Application and recognized proxy
processes are discovered from listeners and local TCP backends. PID flags are
optional overrides. An undetected proxy is reported as unavailable, never as zero CPU.

Legacy BASE_URL, DESTINATION_PORTS, VUS, RAMP_UP, DURATION, TOKENS and SOURCE_IPS remain defaults.
Explicit arguments win. Goose diagnostic flags may follow --; --host and native
workload overrides are rejected so they cannot change the target or total load.
Pass short diagnostic flags separately (for example, -v -v rather than -vv).

Without --users or VUS, run the scoring workflow: 1,000-user two-minute
unscored warm-up; one-minute searches from --start-users (2,500), doubling then narrowing to
250 users; a separate five-minute confirmation, stepping down 250 on failure.
--step-users selects fixed search increments; narrowing and confirmation remain at 250.
Trials drain completely, then pause for one minute before the next search or confirmation.
--score explicitly selects this workflow. --duration applies to fixed-load runs.
";

pub(super) fn pids(value: &str) -> Result<Vec<u32>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let mut pids = Vec::new();
    for item in value.split(',') {
        let pid: u32 = item.trim().parse()?;
        if pid == 0 {
            return Err("process IDs must be positive".into());
        }
        if !pids.contains(&pid) {
            pids.push(pid);
        }
    }
    Ok(pids)
}

pub(super) use twelve_dollar_benchmark::parse_destination_ports;

fn interval(value: &str) -> Result<Duration> {
    let seconds: f64 = value.trim_end_matches('s').parse()?;
    if !seconds.is_finite() || seconds < 0.1 {
        return Err("sample interval must be at least 0.1 seconds".into());
    }
    Ok(Duration::try_from_secs_f64(seconds)?)
}

pub(super) fn parse(args: &[String]) -> Result<Config> {
    let caller = env::var_os("BENCH_CALLER_DIR")
        .map(PathBuf::from)
        .map_or_else(env::current_dir, Ok)?;
    parse_with(args, caller, |key| env::var(key).ok())
}

fn parse_with(
    args: &[String],
    caller: PathBuf,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Config> {
    let default = |key, fallback: &str| {
        lookup(key)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| fallback.to_owned())
    };
    let mut users = default("VUS", "100");
    let mut fixed_users = lookup("VUS").is_some_and(|value| !value.is_empty());
    let mut explicit_score = false;
    let base_url = default("BASE_URL", "http://127.0.0.1:3000");
    let default_port = reqwest::Url::parse(&base_url)
        .ok()
        .and_then(|url| url.port_or_known_default())
        .unwrap_or(3000);
    let environment_ports = default("DESTINATION_PORTS", &default_port.to_string());
    let mut explicit_destination_ports = None;
    let mut config = Config {
        base_url,
        label: None,
        destination_ports: vec![default_port],
        users: 100,
        start_users: 2500,
        step_users: None,
        max_users: 300000,
        ramp_up: default("RAMP_UP", "60s"),
        duration: default("DURATION", "5m"),
        tokens: PathBuf::from(default("TOKENS", "seed/tokens.json")),
        source_ips: default("SOURCE_IPS", ""),
        user: None,
        monitor_user: None,
        output: None,
        diagnostics: Vec::new(),
        application_pids: Vec::new(),
        proxy_pids: Vec::new(),
        sample_interval: interval(&default("BENCH_SAMPLE_INTERVAL", "1"))?,
        help: false,
        score: false,
        skip_warmup: false,
    };
    let mut args = args.iter().peekable();
    let mut target = Vec::new();
    while let Some(arg) = args.next() {
        if arg == "--" {
            config.diagnostics.extend(args.cloned());
            break;
        }
        let (flag, inline) = arg
            .split_once('=')
            .map_or((arg.as_str(), None), |(a, b)| (a, Some(b)));
        if flag == "--help" || flag == "-h" {
            config.help = true;
            continue;
        }
        if flag == "--score" {
            if inline.is_some() {
                return Err("--score takes no value".into());
            }
            explicit_score = true;
            continue;
        }
        if flag == "--skip-warmup" {
            if inline.is_some() {
                return Err("--skip-warmup takes no value".into());
            }
            config.skip_warmup = true;
            continue;
        }
        if !flag.starts_with('-') {
            target.push(arg.clone());
            continue;
        }
        let takes_value = [
            "--label",
            "--user",
            "--monitor-user",
            "--output-dir",
            "--source-ips",
            "--destination-ports",
            "--max-users",
            "--start-users",
            "--step-users",
            "--users",
            "--ramp-up",
            "--duration",
            "--tokens",
            "--application-pids",
            "--proxy-pids",
            "--sample-interval",
        ]
        .contains(&flag);
        if !takes_value {
            config.diagnostics.push(arg.clone());
            if inline.is_none() && diagnostic_takes_value(flag) {
                config.diagnostics.push(
                    args.next()
                        .ok_or("missing diagnostic option value")?
                        .clone(),
                );
            }
            continue;
        }
        let value = inline
            .map(str::to_owned)
            .or_else(|| args.next().cloned())
            .ok_or_else(|| format!("{flag} requires a value"))?;
        if value.is_empty() {
            return Err(format!("{flag} requires a non-empty value").into());
        }
        match flag {
            "--label" => config.label = Some(value),
            "--user" => config.user = Some(value),
            "--monitor-user" => config.monitor_user = Some(value),
            "--output-dir" => config.output = Some(caller.join(value)),
            "--tokens" => config.tokens = caller.join(value),
            "--source-ips" => config.source_ips = value,
            "--destination-ports" => {
                explicit_destination_ports = Some(parse_destination_ports(&value)?);
            }
            "--start-users" | "--max-users" | "--step-users" => {
                let users: usize = value.parse()?;
                if users == 0 || !users.is_multiple_of(250) {
                    return Err(format!("{flag} must be a positive multiple of 250").into());
                }
                match flag {
                    "--start-users" => config.start_users = users,
                    "--step-users" => config.step_users = Some(users),
                    _ => config.max_users = users,
                }
            }
            "--users" => {
                users = value;
                fixed_users = true;
            }
            "--ramp-up" => config.ramp_up = value,
            "--duration" => config.duration = value,
            "--application-pids" => config.application_pids = pids(&value)?,
            "--proxy-pids" => config.proxy_pids = pids(&value)?,
            "--sample-interval" => config.sample_interval = interval(&value)?,
            _ => unreachable!(),
        }
    }
    let positional_destination_ports = if !target.is_empty() {
        if target.len() != 2 {
            return Err("supply target IP and port together".into());
        }
        let target_ip = target[0].as_str();
        let target_ip = target_ip
            .strip_prefix('[')
            .and_then(|value| value.strip_suffix(']'))
            .unwrap_or(target_ip);
        let ip: IpAddr = target_ip.parse()?;
        let destination_ports = parse_destination_ports(&target[1])?;
        let port = destination_ports[0];
        config.base_url = match ip {
            IpAddr::V4(ip) => format!("http://{ip}:{port}"),
            IpAddr::V6(ip) => format!("http://[{ip}]:{port}"),
        };
        Some(destination_ports)
    } else {
        None
    };
    config.destination_ports = if let Some(ports) = explicit_destination_ports {
        ports
    } else if let Some(ports) = positional_destination_ports {
        ports
    } else if config.help {
        parse_destination_ports(&environment_ports).unwrap_or_else(|_| vec![default_port])
    } else {
        parse_destination_ports(&environment_ports)?
    };
    validate_diagnostics(&config.diagnostics)?;
    config.score = explicit_score || !fixed_users;
    for user in config.user.iter().chain(config.monitor_user.iter()) {
        if user.is_empty()
            || user.starts_with('-')
            || !user
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "_.-".contains(c))
        {
            return Err("invalid SSH username".into());
        }
    }
    if config.help {
        config.users = users.parse().unwrap_or(100);
    } else {
        config.users = users.parse()?;
        if config.users == 0 {
            return Err("--users must be positive".into());
        }
        if config.score && config.start_users > config.max_users {
            return Err("--start-users must not exceed --max-users".into());
        }
        super::test_plan(config.users, &config.ramp_up, &config.duration)?;
        super::destinations::urls(&config.base_url, &config.destination_ports)?;
    }
    Ok(config)
}

fn diagnostic_takes_value(flag: &str) -> bool {
    [
        "--host",
        "-H",
        "--users",
        "-u",
        "--test-plan",
        "--run-time",
        "-t",
        "--startup-time",
        "-s",
        "--hatch-rate",
        "-r",
        "--iterations",
        "--request-log",
        "-R",
        "--request-format",
        "--transaction-log",
        "-T",
        "--transaction-format",
        "--scenario-log",
        "-S",
        "--scenario-format",
        "--error-log",
        "-E",
        "--error-format",
        "--debug-log",
        "-D",
        "--debug-format",
        "--report-file",
        "--running-metrics",
        "--timeout",
        "--co-mitigation",
        "--throttle-requests",
        "--scenarios",
        "--goose-log",
        "-G",
    ]
    .contains(&flag)
}

fn validate_diagnostics(args: &[String]) -> Result<()> {
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let (flag, inline) = arg
            .split_once('=')
            .map_or((arg.as_str(), false), |(a, _)| (a, true));
        if flag.starts_with('-') && !flag.starts_with("--") && flag.len() > 2 {
            return Err(format!("combined short flags are unsupported: {flag}; pass each flag separately, such as -v -v").into());
        }
        if flag == "--accept-invalid-certs" {
            return Err(
                "--accept-invalid-certs is unavailable: this benchmark uses HTTP without TLS"
                    .into(),
            );
        }
        if [
            "--host",
            "-H",
            "--users",
            "-u",
            "--test-plan",
            "--run-time",
            "-t",
            "--startup-time",
            "-s",
            "--hatch-rate",
            "-r",
            "--iterations",
            "--no-autostart",
            "--orchestrate",
            "--supervise",
            "--observe",
            "--stop-supervised",
            "--ready-supervised",
            "--start-supervised",
            "--refresh-observer",
        ]
        .contains(&flag)
        {
            return Err(format!("{flag} cannot override the runner; use target IP/port, --users, --ramp-up and --duration").into());
        }
        if diagnostic_takes_value(flag) && !inline {
            args.next().ok_or("missing diagnostic option value")?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_args(args: &[&str], defaults: &[(&str, &str)]) -> Result<Config> {
        parse_with(
            &args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>(),
            PathBuf::from("/caller"),
            |key| {
                defaults
                    .iter()
                    .find_map(|(name, value)| (*name == key).then(|| (*value).to_owned()))
            },
        )
    }

    #[test]
    fn explicit_options_override_legacy_defaults_without_environment_mutation() {
        let config = parse_args(
            &[
                "--users=7",
                "--ramp-up",
                "2s",
                "--source-ips=127.0.0.[1-5]",
                "::1",
                "4000",
                "--duration=3s",
                "--user=generator",
                "--tokens",
                "tokens.json",
                "--output-dir=results",
                "--application-pids=8,8,9",
                "--proxy-pids",
                "10",
                "--sample-interval=0.2s",
            ],
            &[
                ("VUS", "invalid"),
                ("RAMP_UP", "invalid"),
                ("DURATION", "invalid"),
                ("TOKENS", "legacy.json"),
                ("BASE_URL", "http://old:3000"),
                ("SOURCE_IPS", "old"),
            ],
        )
        .unwrap();
        assert_eq!(config.users, 7);
        assert_eq!(config.ramp_up, "2s");
        assert_eq!(config.duration, "3s");
        assert_eq!(config.source_ips, "127.0.0.[1-5]");
        assert_eq!(config.base_url, "http://[::1]:4000");
        assert_eq!(config.tokens, PathBuf::from("/caller/tokens.json"));
        assert_eq!(config.output, Some(PathBuf::from("/caller/results")));
        assert_eq!(config.user.as_deref(), Some("generator"));
        assert_eq!(config.application_pids, vec![8, 9]);
        assert_eq!(config.proxy_pids, vec![10]);
        assert_eq!(config.sample_interval, Duration::from_millis(200));
        assert_eq!(config.destination_ports, vec![4000]);
    }

    #[test]
    fn target_monitoring_does_not_select_remote_generators() {
        let cfg = parse_args(&["127.0.0.1", "3100-3104", "--monitor-user", "mjc"], &[]).unwrap();
        assert_eq!(cfg.monitor_user.as_deref(), Some("mjc"));
        assert!(cfg.user.is_none());
        assert!(parse_args(&["--monitor-user=bad user"], &[]).is_err());
    }

    #[test]
    fn destination_port_lists_and_ranges_are_ordered_and_deduplicated() {
        assert_eq!(
            parse_destination_ports("3100,3101,3102").unwrap(),
            vec![3100, 3101, 3102]
        );
        assert_eq!(
            parse_destination_ports("3100-3102").unwrap(),
            vec![3100, 3101, 3102]
        );
        assert_eq!(
            parse_destination_ports("[3100-3102,3101,3110]").unwrap(),
            vec![3100, 3101, 3102, 3110]
        );
        assert_eq!(
            parse_destination_ports("3100,3102-3104,3101").unwrap(),
            vec![3100, 3102, 3103, 3104, 3101]
        );
        let balanced = parse_destination_ports("3100-3102,3101,3100-3102").unwrap();
        assert_eq!(balanced, vec![3100, 3101, 3102]);
        assert_eq!(balanced.iter().filter(|port| **port == 3101).count(), 1);
        assert_eq!(parse_destination_ports("65535").unwrap(), vec![65535]);
    }

    #[test]
    fn destination_port_argument_overrides_positional_and_environment_ports() {
        let config = parse_args(
            &["127.0.0.1", "3000-3002", "--destination-ports=4000,4001"],
            &[("DESTINATION_PORTS", "5000")],
        )
        .unwrap();
        assert_eq!(config.destination_ports, vec![4000, 4001]);
        assert_eq!(config.base_url, "http://127.0.0.1:3000");

        let config = parse_args(&["127.0.0.1", "3000-3002"], &[]).unwrap();
        assert_eq!(config.destination_ports, vec![3000, 3001, 3002]);
        assert_eq!(config.base_url, "http://127.0.0.1:3000");

        let config = parse_args(
            &[],
            &[
                ("BASE_URL", "http://example.test:8080"),
                ("DESTINATION_PORTS", "9000-9001"),
            ],
        )
        .unwrap();
        assert_eq!(config.destination_ports, vec![9000, 9001]);
        assert_eq!(config.base_url, "http://example.test:8080");
    }

    #[test]
    fn invalid_destination_port_environment_is_ignored_when_cli_or_help_overrides_it() {
        let defaults = [("DESTINATION_PORTS", "not-a-port")];
        let config = parse_args(&["--destination-ports=4000"], &defaults).unwrap();
        assert_eq!(config.destination_ports, vec![4000]);

        let config = parse_args(&["127.0.0.1", "4100-4101"], &defaults).unwrap();
        assert_eq!(config.destination_ports, vec![4100, 4101]);

        let config = parse_args(&["--help"], &defaults).unwrap();
        assert!(config.help);
        assert_eq!(config.destination_ports, vec![3000]);
    }

    #[test]
    fn empty_legacy_environment_settings_fall_back_and_targets_use_http() {
        let config = parse_args(
            &[],
            &[
                ("BASE_URL", ""),
                ("DESTINATION_PORTS", ""),
                ("VUS", ""),
                ("RAMP_UP", ""),
                ("DURATION", ""),
                ("TOKENS", ""),
                ("SOURCE_IPS", ""),
            ],
        )
        .unwrap();
        assert_eq!(config.base_url, "http://127.0.0.1:3000");
        assert_eq!(config.destination_ports, vec![3000]);
        assert_eq!(config.users, 100);
        assert_eq!(config.ramp_up, "60s");
        assert_eq!(config.duration, "5m");
        assert_eq!(config.tokens, PathBuf::from("seed/tokens.json"));
        assert!(config.source_ips.is_empty());

        let config = parse_args(&[], &[("BASE_URL", "http://example.test")]).unwrap();
        assert_eq!(config.destination_ports, vec![80]);
        assert!(parse_args(&[], &[("BASE_URL", "https://example.test")]).is_err());
        // An explicit HTTP target wins over a legacy HTTPS default.
        assert!(
            parse_args(
                &["127.0.0.1", "3100"],
                &[("BASE_URL", "https://example.test")]
            )
            .is_ok()
        );
    }

    #[test]
    fn defaults_and_explicit_absolute_paths_keep_their_meaning() {
        let config = parse_args(&[], &[("TOKENS", "relative/legacy.json"), ("VUS", "12")]).unwrap();
        assert_eq!(config.users, 12);
        assert_eq!(config.tokens, PathBuf::from("relative/legacy.json"));
        assert_eq!(config.base_url, "http://127.0.0.1:3000");
        let config = parse_args(
            &["--tokens=/shared/tokens.json", "--output-dir", "/results"],
            &[],
        )
        .unwrap();
        assert_eq!(config.tokens, PathBuf::from("/shared/tokens.json"));
        assert_eq!(config.output, Some(PathBuf::from("/results")));
    }

    #[test]
    fn diagnostic_values_are_not_mistaken_for_workload_overrides_or_targets() {
        let config = parse_args(
            &[
                "--request-log",
                "-trace.log",
                "127.0.0.1",
                "3000",
                "--request-format=json",
            ],
            &[],
        )
        .unwrap();
        assert_eq!(
            config.diagnostics,
            vec!["--request-log", "-trace.log", "--request-format=json"]
        );
        let config = parse_args(&["--", "--request-log", "-trace.log"], &[]).unwrap();
        assert_eq!(config.diagnostics, vec!["--request-log", "-trace.log"]);
        let config = parse_args(&["--", "--request-log=-file", "-v", "-v"], &[]).unwrap();
        assert_eq!(config.diagnostics, vec!["--request-log=-file", "-v", "-v"]);
        for args in [
            vec!["--", "--host=http://other:3000"],
            vec!["--", "--users=9"],
            vec!["--", "-t5s"],
            vec!["--", "--observe"],
            vec!["--", "--start-supervised"],
            vec!["--", "--refresh-observer"],
            vec!["--", "--accept-invalid-certs"],
        ] {
            assert!(parse_args(&args, &[]).is_err(), "{args:?}");
        }
    }

    #[test]
    fn combined_short_flags_cannot_hide_runner_overrides() {
        for flag in ["-vH", "-qu", "-vt", "-vRfile", "-vv"] {
            for args in [vec![flag], vec!["--", flag]] {
                let error = parse_args(&args, &[]).err().unwrap().to_string();
                assert!(error.contains("combined short flags"), "{args:?}: {error}");
            }
        }
    }

    #[test]
    fn invalid_targets_counts_intervals_and_missing_values_fail_before_launch() {
        for args in [
            vec!["127.0.0.1"],
            vec!["127.0.0.1", "0"],
            vec!["[::1", "3000"],
            vec!["127.0.0.1", "65536"],
            vec!["127.0.0.1", "80,0"],
            vec!["127.0.0.1", "80-79"],
            vec!["--destination-ports="],
            vec!["--destination-ports=0"],
            vec!["--destination-ports=80,"],
            vec!["--destination-ports=80-65536"],
            vec!["--users=0"],
            vec!["--users=-1"],
            vec!["--output-dir"],
            vec!["--tokens="],
            vec!["--application-pids=0"],
            vec!["--proxy-pids=2,"],
            vec!["--sample-interval=0.01"],
            vec!["--sample-interval=NaN"],
            vec!["--sample-interval=inf"],
            vec!["--sample-interval=1e100"],
            vec!["--duration=1s;9,0s"],
            vec!["--request-log"],
        ] {
            assert!(parse_args(&args, &[]).is_err(), "{args:?}");
        }
        let config =
            parse_args(&["--help"], &[("VUS", "invalid"), ("DURATION", "invalid")]).unwrap();
        assert!(config.help);
    }
}
