use std::{collections::BTreeSet, env, error::Error};

pub mod control;
pub mod telemetry;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub const ENDPOINTS: [&str; 4] = [
    "GET /feed",
    "GET /posts/:id",
    "POST /posts/:id/like",
    "POST /posts",
];

pub fn setting(name: &str, default: &str) -> String {
    setting_value(env::var(name).ok(), default)
}

fn setting_value(value: Option<String>, default: &str) -> String {
    value
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

pub fn parse_destination_ports(value: &str) -> Result<Vec<u16>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_environment_settings_use_k6_defaults() {
        assert_eq!(setting_value(None, "default"), "default");
        assert_eq!(setting_value(Some(String::new()), "default"), "default");
        assert_eq!(
            setting_value(Some("configured".into()), "default"),
            "configured"
        );
    }
}
