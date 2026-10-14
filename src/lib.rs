use std::{collections::BTreeSet, env, error::Error};

pub mod control;
pub mod telemetry;

pub const ENDPOINTS: [&str; 4] = [
    "GET /feed",
    "GET /posts/:id",
    "POST /posts/:id/like",
    "POST /posts",
];

pub fn setting(name: &str, default: &str) -> String {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

pub fn parse_destination_ports(value: &str) -> Result<Vec<u16>, Box<dyn Error>> {
    let value = value.trim();
    let value = value
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(value);
    if value.is_empty() {
        return Err("destination ports must not be empty".into());
    }

    let mut ports = Vec::new();
    let mut seen = BTreeSet::new();
    for item in value.split(',') {
        let item = item.trim();
        if item.is_empty() {
            return Err("destination ports must not contain empty entries".into());
        }
        let (start, end) = match item.split_once('-') {
            Some((start, end)) => (start.trim().parse::<u16>()?, end.trim().parse::<u16>()?),
            None => {
                let port = item.parse::<u16>()?;
                (port, port)
            }
        };
        if start == 0 || end == 0 {
            return Err("destination ports must be positive".into());
        }
        if start > end {
            return Err("destination port ranges must be ascending".into());
        }
        for port in start..=end {
            if seen.insert(port) {
                ports.push(port);
            }
        }
    }
    Ok(ports)
}
