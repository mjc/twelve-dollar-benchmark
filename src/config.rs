use std::{
    env,
    error::Error,
    net::IpAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

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
    pub application_pids: Vec<u32>,
    pub proxy_pids: Vec<u32>,
    pub sample_interval: Duration,
    pub help: bool,
    pub score: bool,
    pub skip_warmup: bool,
}

pub(super) const HELP: &str = "Usage: run.sh [IP PORTS] [options]

  --source-ips IPS          Local source IPs, or SSH generator hosts with --user
  --destination-ports PORTS Destination ports, as a list or inclusive range
  --user USER               Copy binary/tokens and run generators over SSH
  --monitor-user USER       Monitor the target over SSH while generating locally
  --users N                 Fixed-load run at N users; disables auto scoring
  --score                   Warm up, find the limit, confirm a five-minute score
  --skip-warmup             Reuse a warm-up completed earlier in this session
  --start-users N           First search load (2500); positive multiple of 250
  --step-users N            Search increment (default: double)
                            Positive multiple of 250
  --max-users N             Search ceiling (300000); positive multiple of 250
  --label NAME              Implementation name in the final Markdown table
  --ramp-up TIME            Ramp to total users (60s)
  --duration TIME           Hold at total users (5m); ramp-down remains 30s
  --tokens PATH             Token file
  --output-dir DIR          New results directory
  --application-pids PIDS   Comma-separated application PIDs on the target host
  --proxy-pids PIDS         Comma-separated proxy PIDs on the target host
  --sample-interval SECONDS Sampling interval in seconds (1; minimum 0.1)
  --help                    Show this help

Explicit token and output paths resolve from the directory where you invoke
run.sh.

Target monitoring uses --monitor-user, falling back to --user. Without SSH,
target monitoring is local for loopback targets. Application PIDs default to
accessible processes listening on the target port; identify the backend
explicitly behind a proxy. An unselected proxy is reported as unavailable, never
as zero CPU.

Legacy BASE_URL, DESTINATION_PORTS, VUS, RAMP_UP, DURATION, TOKENS, and
SOURCE_IPS remain defaults. Explicit arguments win. Unsupported options are
rejected.

Without --users or VUS, scoring runs by default: a 1,000-user, two-minute
unscored warm-up; one-minute searches from --start-users (2500), doubling or
using --step-users, then narrowing to 250-user steps; and a separate five-minute
confirmation, stepping down 250 users on failure. --score explicitly selects
this workflow. --duration applies to fixed-load runs.
Trials drain completely, then pause for one minute before the next trial.
";

pub(super) use twelve_dollar_benchmark::{
    parse_destination_ports, parse_pids as pids, parse_sample_interval as interval,
};

fn option_value(parser: &mut lexopt::Parser, flag: &str) -> Result<String> {
    let value = parser
        .value()
        .map_err(|_| format!("{flag} requires a value"))?
        .into_string()
        .map_err(|_| format!("{flag} value must be valid UTF-8"))?;
    if value.is_empty() {
        return Err(format!("{flag} requires a non-empty value").into());
    }
    Ok(value)
}

fn users_bound(parser: &mut lexopt::Parser, flag: &str) -> Result<usize> {
    let value = option_value(parser, flag)?;
    let users = value
        .parse::<usize>()
        .map_err(|_| format!("{flag} must be a positive multiple of 250"))?;
    if users == 0 || !users.is_multiple_of(250) {
        return Err(format!("{flag} must be a positive multiple of 250").into());
    }
    Ok(users)
}

pub(super) fn duration(value: &str) -> Result<Duration> {
    let value = value.trim();
    if value.is_empty() {
        return Err("duration must not be empty".into());
    }
    let mut total = Duration::ZERO;
    let mut rest = value;
    let mut previous_unit = 4;
    while !rest.is_empty() {
        let numeric_len = rest
            .char_indices()
            .take_while(|(_, c)| c.is_ascii_digit() || *c == '.')
            .map(|(index, c)| index + c.len_utf8())
            .last()
            .ok_or("invalid duration")?;
        let amount: f64 = rest[..numeric_len].parse()?;
        if !amount.is_finite() || amount < 0.0 {
            return Err("duration must be finite and nonnegative".into());
        }
        rest = &rest[numeric_len..];
        let (unit, multiplier, rank) = match rest.chars().next() {
            Some('h') => ("h", 3600.0, 3),
            Some('m') => ("m", 60.0, 2),
            Some('s') => ("s", 1.0, 1),
            None if previous_unit == 4 => ("", 1.0, 0),
            _ => return Err("duration units must be h, m, or s".into()),
        };
        if rank >= previous_unit && rank != 0 {
            return Err("duration units must be ordered h, m, s without repeats".into());
        }
        let seconds = amount * multiplier;
        if !seconds.is_finite() {
            return Err("duration is too large".into());
        }
        let component =
            Duration::try_from_secs_f64(seconds).map_err(|_| "duration is too large")?;
        total = total
            .checked_add(component)
            .ok_or("duration is too large")?;
        if rank == 0 {
            if !rest.is_empty() {
                return Err("invalid duration".into());
            }
            break;
        }
        previous_unit = rank;
        rest = &rest[unit.len()..];
        if rest.is_empty() {
            break;
        }
    }
    Instant::now()
        .checked_add(total)
        .ok_or("duration cannot be represented by the system clock")?;
    Ok(total)
}

pub(super) fn parse(args: &[String]) -> Result<Config> {
    let caller = env::var_os("BENCH_CALLER_DIR")
        .map(PathBuf::from)
        .map_or_else(env::current_dir, Ok)?;
    parse_with(args, &caller, |key| env::var(key).ok())
}

struct Overrides {
    users: String,
    fixed_users: bool,
    explicit_score: bool,
    environment_ports: String,
    default_port: u16,
    explicit_destination_ports: Option<Vec<u16>>,
    target: Vec<String>,
}

fn parse_with(
    args: &[String],
    caller: &Path,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Config> {
    let (mut config, mut overrides) = defaults(&lookup)?;
    parse_options(args, caller, &mut config, &mut overrides)?;
    finish(config, overrides)
}

fn defaults(lookup: &impl Fn(&str) -> Option<String>) -> Result<(Config, Overrides)> {
    let default = |key, fallback: &str| {
        lookup(key)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| fallback.to_owned())
    };
    let users = default("VUS", "100");
    let fixed_users = lookup("VUS").is_some_and(|value| !value.is_empty());
    let base_url = default("BASE_URL", "http://127.0.0.1:3000");
    let default_port = url::Url::parse(&base_url)
        .ok()
        .and_then(|url| url.port_or_known_default())
        .unwrap_or(3000);
    let environment_ports = default("DESTINATION_PORTS", &default_port.to_string());
    Ok((
        Config {
            base_url,
            label: None,
            destination_ports: vec![default_port],
            users: 100,
            start_users: 2500,
            step_users: None,
            max_users: 300_000,
            ramp_up: default("RAMP_UP", "60s"),
            duration: default("DURATION", "5m"),
            tokens: PathBuf::from(default("TOKENS", "seed/tokens.json")),
            source_ips: default("SOURCE_IPS", ""),
            user: None,
            monitor_user: None,
            output: None,
            application_pids: Vec::new(),
            proxy_pids: Vec::new(),
            sample_interval: interval(&default("BENCH_SAMPLE_INTERVAL", "1"))?,
            help: false,
            score: false,
            skip_warmup: false,
        },
        Overrides {
            users,
            fixed_users,
            explicit_score: false,
            environment_ports,
            default_port,
            explicit_destination_ports: None,
            target: Vec::new(),
        },
    ))
}

fn parse_options(
    args: &[String],
    caller: &Path,
    config: &mut Config,
    overrides: &mut Overrides,
) -> Result<()> {
    use lexopt::prelude::*;

    let mut parser = lexopt::Parser::from_args(args.iter().map(String::as_str));
    loop {
        if parser
            .try_raw_args()
            .is_some_and(|args| args.peek().is_some_and(|arg| arg.to_str() == Some("--")))
        {
            return Err("unsupported option: --".into());
        }
        let Some(arg) = parser.next()? else {
            break;
        };
        match arg {
            Short('h') | Long("help") => {
                let _ = parser.optional_value();
                config.help = true;
            }
            Long("score") => {
                if parser.optional_value().is_some() {
                    return Err("--score takes no value".into());
                }
                overrides.explicit_score = true;
            }
            Long("skip-warmup") => {
                if parser.optional_value().is_some() {
                    return Err("--skip-warmup takes no value".into());
                }
                config.skip_warmup = true;
            }
            Value(value) => {
                let value = value
                    .into_string()
                    .map_err(|_| "target arguments must be valid UTF-8")?;
                if value.starts_with('-') {
                    return Err(format!("unsupported option: {value}").into());
                }
                overrides.target.push(value);
            }
            Long("label") => config.label = Some(option_value(&mut parser, "--label")?),
            Long("user") => config.user = Some(option_value(&mut parser, "--user")?),
            Long("monitor-user") => {
                config.monitor_user = Some(option_value(&mut parser, "--monitor-user")?);
            }
            Long("output-dir") => {
                let value = option_value(&mut parser, "--output-dir")?;
                config.output = Some(caller.join(value));
            }
            Long("source-ips") => config.source_ips = option_value(&mut parser, "--source-ips")?,
            Long("destination-ports") => {
                let value = option_value(&mut parser, "--destination-ports")?;
                overrides.explicit_destination_ports = Some(parse_destination_ports(&value)?);
            }
            Long("start-users") => config.start_users = users_bound(&mut parser, "--start-users")?,
            Long("step-users") => {
                config.step_users = Some(users_bound(&mut parser, "--step-users")?);
            }
            Long("max-users") => config.max_users = users_bound(&mut parser, "--max-users")?,
            Long("users") => {
                overrides.users = option_value(&mut parser, "--users")?;
                overrides.fixed_users = true;
            }
            Long("ramp-up") => config.ramp_up = option_value(&mut parser, "--ramp-up")?,
            Long("duration") => config.duration = option_value(&mut parser, "--duration")?,
            Long("tokens") => {
                config.tokens = caller.join(option_value(&mut parser, "--tokens")?);
            }
            Long("application-pids") => {
                let value = option_value(&mut parser, "--application-pids")?;
                config.application_pids = pids(&value)?;
            }
            Long("proxy-pids") => {
                let value = option_value(&mut parser, "--proxy-pids")?;
                config.proxy_pids = pids(&value)?;
            }
            Long("sample-interval") => {
                let value = option_value(&mut parser, "--sample-interval")?;
                config.sample_interval = interval(&value)?;
            }
            Short(flag) => return Err(format!("unsupported option: -{flag}").into()),
            Long(flag) => return Err(format!("unsupported option: --{flag}").into()),
        }
    }
    Ok(())
}

fn finish(mut config: Config, overrides: Overrides) -> Result<Config> {
    let positional_destination_ports = if overrides.target.is_empty() {
        None
    } else {
        let [target_ip, target_ports] = overrides.target.as_slice() else {
            return Err("supply target IP and port together".into());
        };
        let target_ip = target_ip
            .strip_prefix('[')
            .and_then(|value| value.strip_suffix(']'))
            .unwrap_or(target_ip);
        let ip: IpAddr = target_ip.parse()?;
        let destination_ports = parse_destination_ports(target_ports)?;
        let port = destination_ports
            .first()
            .copied()
            .ok_or("destination ports must not be empty")?;
        config.base_url = match ip {
            IpAddr::V4(ip) => format!("http://{ip}:{port}"),
            IpAddr::V6(ip) => format!("http://[{ip}]:{port}"),
        };
        Some(destination_ports)
    };
    config.destination_ports = if let Some(ports) = overrides.explicit_destination_ports {
        ports
    } else if let Some(ports) = positional_destination_ports {
        ports
    } else if config.help {
        parse_destination_ports(&overrides.environment_ports)
            .unwrap_or_else(|_| vec![overrides.default_port])
    } else {
        parse_destination_ports(&overrides.environment_ports)?
    };
    config.score = overrides.explicit_score || !overrides.fixed_users;
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
        config.users = overrides.users.parse().unwrap_or(100);
    } else {
        config.users = overrides
            .users
            .parse()
            .map_err(|_| "--users must be a positive integer")?;
        if config.users == 0 {
            return Err("--users must be positive".into());
        }
        if config.score && config.start_users > config.max_users {
            return Err("--start-users must not exceed --max-users".into());
        }
        duration(&config.ramp_up)
            .map_err(|error| format!("invalid --ramp-up duration: {error}"))?;
        let hold =
            duration(&config.duration).map_err(|error| format!("invalid --duration: {error}"))?;
        let full_hold = hold
            .checked_add(Duration::from_secs(30))
            .ok_or("duration plus ramp-down exceeds the system clock range")?;
        Instant::now()
            .checked_add(full_hold)
            .ok_or("duration plus ramp-down exceeds the system clock range")?;
        super::destinations::urls(&config.base_url, &config.destination_ports)?;
    }
    Ok(config)
}
