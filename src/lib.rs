use std::{collections::BTreeSet, env, error::Error};

pub mod control;
pub mod telemetry;

pub const ENDPOINTS: [&str; 4] = [
    "GET /feed",
    "GET /posts/:id",
    "POST /posts/:id/like",
    "POST /posts",
];

#[must_use]
pub fn setting(name: &str, default: &str) -> String {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

/// Parse a comma-separated list of positive process IDs, removing duplicates.
///
/// # Errors
///
/// Returns an error when an item is not a positive integer.
pub fn parse_pids(value: &str) -> Result<Vec<u32>, Box<dyn Error>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let mut pids = Vec::new();
    for item in value.split(',') {
        let pid = item
            .trim()
            .parse::<u32>()
            .map_err(|_| format!("invalid process ID: {item}"))?;
        if pid == 0 {
            return Err("process IDs must be positive".into());
        }
        if !pids.contains(&pid) {
            pids.push(pid);
        }
    }
    Ok(pids)
}

/// Parse a port list such as `3000,3001` or a range such as `3000-3002`.
///
/// # Errors
///
/// Returns an error for empty entries, invalid or zero ports, and descending ranges.
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
        let (start, end) = if let Some((start, end)) = item.split_once('-') {
            (start.trim().parse::<u16>()?, end.trim().parse::<u16>()?)
        } else {
            let port = item.parse::<u16>()?;
            (port, port)
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
    use super::parse_pids;

    #[test]
    fn process_id_parser_handles_empty_deduplicated_and_invalid_values() {
        assert_eq!(parse_pids("").unwrap(), Vec::<u32>::new());
        assert_eq!(parse_pids("12, 7,12").unwrap(), vec![12, 7]);
        assert!(parse_pids("0").is_err());
        assert!(parse_pids("12,nope").is_err());
    }
}
