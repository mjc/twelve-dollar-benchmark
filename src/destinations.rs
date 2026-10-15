use std::error::Error;
use url::Url;

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
                .map_err(|()| "unable to set destination port")?;
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
