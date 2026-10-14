use reqwest::Url;
use std::error::Error;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub(super) fn urls(base: &str, ports: &[u16]) -> Result<Vec<Url>> {
    let base = Url::parse(base)?;
    if base.scheme() != "http" || base.host_str().is_none() {
        return Err(
            "benchmark target must use http://; HTTPS/TLS is not supported by this benchmark"
                .into(),
        );
    }
    if ports.is_empty() {
        return Ok(vec![base]);
    }
    ports
        .iter()
        .map(|port| {
            let mut url = base.clone();
            url.set_port(Some(*port))
                .map_err(|_| "unable to set destination port")?;
            Ok(url)
        })
        .collect()
}

// Cycle sources and destinations together. Rotate the destination after each
// least-common-multiple period to cover the pairs a diagonal cycle would miss.
// This balances both lists even before enough users exist to fill every pair.
pub(super) fn index(user: usize, sources: usize, destinations: usize) -> usize {
    let sources = sources.max(1);
    let (mut gcd, mut remainder) = (sources, destinations);
    while remainder != 0 {
        (gcd, remainder) = (remainder, gcd % remainder);
    }
    // Divide in steps instead of multiplying the period, avoiding overflow.
    let phase = (user / sources / (destinations / gcd)) % gcd;
    (user % destinations + phase) % destinations
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn balances_every_source_destination_pair_without_moving_sleeping_users() {
        for (sources, ports) in [(1, 3), (3, 1), (3, 4)] {
            let mut counts = vec![vec![0; ports]; sources];
            for user in 0..sources * ports * 2 {
                counts[user % sources][index(user, sources, ports)] += 1;
            }
            assert!(counts.into_iter().flatten().all(|count| count == 2));
        }
        for sources in 1..=7 {
            for ports in 1..=7 {
                let mut source_counts = vec![0usize; sources];
                let mut port_counts = vec![0usize; ports];
                for user in 0..sources * ports * 2 {
                    source_counts[user % sources] += 1;
                    port_counts[index(user, sources, ports)] += 1;
                    assert!(
                        source_counts.iter().max().unwrap() - source_counts.iter().min().unwrap()
                            <= 1
                    );
                    assert!(
                        port_counts.iter().max().unwrap() - port_counts.iter().min().unwrap() <= 1
                    );
                }
            }
        }
        assert_eq!(
            (0..5).map(|user| index(user, 0, 3)).collect::<Vec<_>>(),
            [0, 1, 2, 0, 1]
        );
        // Remote workers retain disjoint global offsets, avoiding restarting the
        // destination cycle on every generator.
        assert_eq!(
            (2..5).map(|user| index(user, 0, 3)).collect::<Vec<_>>(),
            [2, 0, 1]
        );
    }
    #[test]
    fn destination_urls_preserve_scheme_ipv6_and_default_ports() {
        let targets = urls("http://[::1]/base?x=1", &[80, 8080]).unwrap();
        assert_eq!(targets[0].as_str(), "http://[::1]/base?x=1");
        assert_eq!(targets[1].as_str(), "http://[::1]:8080/base?x=1");
        assert_eq!(
            urls("http://127.0.0.1:3100", &[]).unwrap()[0].port(),
            Some(3100)
        );
        assert!(urls("ftp://localhost", &[3100]).is_err());
        assert!(urls("https://localhost", &[443])
            .unwrap_err()
            .to_string()
            .contains("HTTPS/TLS"));
    }
}
