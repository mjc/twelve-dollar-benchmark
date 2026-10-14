use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    error::Error,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};
pub type Result<T> = std::result::Result<T, Box<dyn Error>>;
pub type Roles = BTreeMap<String, Vec<u32>>;

#[derive(Clone, Serialize, Deserialize)]
struct Interface {
    rx_bytes: u64,
    tx_bytes: u64,
    rx_packets: u64,
    tx_packets: u64,
}
#[derive(Clone, Serialize, Deserialize)]
struct Process {
    pid: u32,
    name: String,
    start_ticks: u64,
    cpu_seconds: f64,
    resident_bytes: u64,
    open_descriptors: Option<usize>,
    established_sockets: Option<usize>,
    descriptor_limits: Value,
    errors: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Point {
    timestamp: String,
    elapsed_s: f64,
    host_boot_id: Option<String>,
    network_namespace: Option<String>,
    sample_interval_s: f64,
    interfaces: Option<BTreeMap<String, Interface>>,
    processes: BTreeMap<String, Vec<Process>>,
    limits: Value,
    activity: Value,
    #[serde(default)]
    tcp: Option<BTreeMap<String, u64>>,
    #[serde(default)]
    interval_rates: Value,
    #[serde(default)]
    service: Value,
    errors: Vec<String>,
}

fn interfaces(text: &str) -> Result<BTreeMap<String, Interface>> {
    let mut result = BTreeMap::new();
    for line in text.lines().skip(2) {
        let (name, values) = line
            .rsplit_once(':')
            .ok_or("invalid network interface row")?;
        let fields: Vec<u64> = values
            .split_whitespace()
            .map(str::parse)
            .collect::<std::result::Result<_, _>>()?;
        if fields.len() < 16 {
            return Err("incomplete interface counters".into());
        }
        result.insert(
            name.trim().into(),
            Interface {
                rx_bytes: fields[0],
                rx_packets: fields[1],
                tx_bytes: fields[8],
                tx_packets: fields[9],
            },
        );
    }
    Ok(result)
}

// Keep the netlink request and multipart decoder together so their bounds stay auditable.
#[allow(clippy::too_many_lines)]
fn socket_inodes() -> Result<HashSet<u64>> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    // SAFETY: no pointer arguments; OwnedFd takes ownership only on success.
    let raw = unsafe {
        libc::socket(
            libc::AF_NETLINK,
            libc::SOCK_RAW | libc::SOCK_CLOEXEC,
            libc::NETLINK_SOCK_DIAG,
        )
    };
    if raw < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let socket = unsafe { OwnedFd::from_raw_fd(raw) };
    let timeout = libc::timeval {
        tv_sec: 2,
        tv_usec: 0,
    };
    // SAFETY: the option pointer and length describe a live timeval.
    if unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            std::ptr::from_ref(&timeout).cast(),
            libc::socklen_t::try_from(size_of_val(&timeout))?,
        )
    } < 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: an all-zero sockaddr_nl is valid before assigning its family.
    let mut kernel: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
    kernel.nl_family = libc::sa_family_t::try_from(libc::AF_NETLINK)?;
    let mut inodes = HashSet::new();
    for family in [libc::AF_INET, libc::AF_INET6] {
        // nlmsghdr (16 bytes), inet_diag_req_v2 (8), inet_diag_sockid (48).
        let mut request = [0_u8; 72];
        request[..4].copy_from_slice(&72_u32.to_ne_bytes());
        request[4..6].copy_from_slice(&20_u16.to_ne_bytes()); // SOCK_DIAG_BY_FAMILY
        request[6..8].copy_from_slice(&0x301_u16.to_ne_bytes()); // REQUEST | DUMP
        request[8..12].copy_from_slice(&u32::try_from(family)?.to_ne_bytes());
        request[16] = u8::try_from(family)?;
        request[17] = u8::try_from(libc::IPPROTO_TCP)?;
        request[20..24].copy_from_slice(&2_u32.to_ne_bytes()); // 1 << TCP_ESTABLISHED
        // SAFETY: request and kernel are live buffers of their stated sizes.
        if unsafe {
            libc::sendto(
                socket.as_raw_fd(),
                request.as_ptr().cast(),
                request.len(),
                0,
                std::ptr::from_ref(&kernel).cast(),
                libc::socklen_t::try_from(size_of_val(&kernel))?,
            )
        } < 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        let mut buffer = vec![0_u8; 65_536].into_boxed_slice();
        'dump: loop {
            // SAFETY: buffer is writable for its entire stated length.
            let received = unsafe {
                libc::recv(
                    socket.as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                    libc::MSG_TRUNC,
                )
            };
            if received < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let received = usize::try_from(received)?;
            if received == 0 || received > buffer.len() {
                return Err("truncated socket diagnostic dump".into());
            }
            let mut offset = 0;
            while offset + 16 <= received {
                let header = &buffer[offset..offset + 16];
                let length = u32::from_ne_bytes(header[..4].try_into()?) as usize;
                if length < 16 || length > received - offset {
                    return Err("invalid socket diagnostic message".into());
                }
                let flags = u16::from_ne_bytes(header[6..8].try_into()?);
                if flags & 0x10 != 0 {
                    return Err("socket diagnostic dump interrupted".into());
                }
                let payload = &buffer[offset + 16..offset + length];
                match u16::from_ne_bytes(header[4..6].try_into()?) {
                    3 => break 'dump, // NLMSG_DONE
                    2 => {
                        // NLMSG_ERROR
                        let code = i32::from_ne_bytes(
                            payload
                                .get(..4)
                                .ok_or("truncated diagnostic error")?
                                .try_into()?,
                        );
                        if code != 0 {
                            return Err(std::io::Error::from_raw_os_error(
                                code.checked_neg().ok_or("invalid diagnostic error")?,
                            )
                            .into());
                        }
                    }
                    20 => {
                        // inet_diag_msg inode is a native-endian u32 at offset 68.
                        let inode = u32::from_ne_bytes(
                            payload
                                .get(68..72)
                                .ok_or("truncated socket diagnostic entry")?
                                .try_into()?,
                        );
                        inodes.insert(u64::from(inode));
                    }
                    _ => {}
                }
                offset += (length + 3) & !3;
            }
        }
    }
    Ok(inodes)
}

fn process_stat(text: &str) -> Result<(String, u64, u64, u64)> {
    let (prefix, tail) = text.rsplit_once(") ").ok_or("invalid process stat")?;
    let name = prefix
        .split_once('(')
        .ok_or("missing process name")?
        .1
        .to_owned();
    let fields: Vec<_> = tail.split_whitespace().collect();
    if fields.len() < 22 {
        return Err("incomplete process stat".into());
    }
    Ok((
        name,
        fields[19].parse()?,
        fields[11]
            .parse::<u64>()?
            .saturating_add(fields[12].parse()?),
        fields[21].parse()?,
    ))
}

#[allow(clippy::cast_precision_loss)] // Kernel CPU ticks become floating-point report seconds.
fn process(
    pid: u32,
    ticks: f64,
    page_size: u64,
    sockets: &Result<HashSet<u64>>,
) -> Result<Process> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let (name, start_ticks, cpu_ticks, resident_pages) = process_stat(&text)?;
    let mut errors = Vec::new();
    let sockets = sockets.as_ref().map_err(|error| {
        errors.push(format!("established sockets: {error}"));
    });
    let same_namespace = fs::read_link(format!("/proc/{pid}/ns/net"))
        .ok()
        .zip(fs::read_link("/proc/self/ns/net").ok())
        .is_some_and(|(process, observer)| process == observer);
    if !same_namespace {
        errors.push("established sockets unavailable: process network namespace differs from observer or cannot be read".into());
    }
    let (open_descriptors, established_sockets) = match fs::read_dir(format!("/proc/{pid}/fd")) {
        Ok(entries) => {
            let mut count = 0;
            let mut descriptors_complete = true;
            let mut established = BTreeSet::new();
            let mut socket_complete = sockets.is_ok() && same_namespace;
            for entry in entries {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(error) => {
                        errors.push(error.to_string());
                        socket_complete = false;
                        descriptors_complete = false;
                        continue;
                    }
                };
                count += 1;
                if let Ok(sockets) = &sockets {
                    match fs::read_link(entry.path()) {
                        Ok(link) => {
                            if let Some(inode) = link
                                .to_str()
                                .and_then(|s| s.strip_prefix("socket:["))
                                .and_then(|s| s.strip_suffix(']'))
                                .and_then(|s| s.parse::<u64>().ok())
                                .filter(|inode| sockets.contains(inode))
                            {
                                established.insert(inode);
                            }
                        }
                        // An FD can close between the directory listing and readlink.
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => {
                            errors.push(error.to_string());
                            socket_complete = false;
                        }
                    }
                }
            }
            (
                descriptors_complete.then_some(count),
                socket_complete.then_some(established.len()),
            )
        }
        Err(error) => {
            errors.push(format!("descriptors: {error}"));
            (None, None)
        }
    };
    let descriptor_limits = match fs::read_to_string(format!("/proc/{pid}/limits")) {
        Ok(text) => text
            .lines()
            .find_map(|line| line.strip_prefix("Max open files"))
            .map_or(Value::Null, |line| {
                let fields: Vec<_> = line.split_whitespace().collect();
                if fields.len() >= 2 {
                    json!({"soft": fields[0], "hard": fields[1]})
                } else {
                    Value::Null
                }
            }),
        Err(error) => {
            errors.push(format!("descriptor limits: {error}"));
            Value::Null
        }
    };
    Ok(Process {
        pid,
        name,
        start_ticks,
        cpu_seconds: cpu_ticks as f64 / ticks,
        resident_bytes: resident_pages.saturating_mul(page_size),
        open_descriptors,
        established_sockets,
        descriptor_limits,
        errors,
    })
}

fn service_resources(roles: &Roles) -> Value {
    let read = || -> Result<Value> {
        let mut paths = BTreeSet::new();
        for role in ["application", "proxy"] {
            for pid in roles.get(role).into_iter().flatten() {
                let text = fs::read_to_string(format!("/proc/{pid}/cgroup"))?;
                let group = text
                    .lines()
                    .find_map(|line| line.strip_prefix("0::"))
                    .ok_or("cgroup v2 membership unavailable")?;
                paths.insert(Path::new("/sys/fs/cgroup").join(group.trim_start_matches('/')));
            }
        }
        let mut groups = BTreeMap::new();
        for path in &paths {
            if paths
                .iter()
                .any(|parent| parent != path && path.starts_with(parent))
            {
                continue;
            }
            let memory: u64 = fs::read_to_string(path.join("memory.current"))?
                .trim()
                .parse()?;
            let cpu: u64 = fs::read_to_string(path.join("cpu.stat"))?
                .lines()
                .find_map(|line| line.strip_prefix("usage_usec ").map(str::to_owned))
                .ok_or("cgroup CPU usage unavailable")?
                .parse()?;
            groups.insert(
                path.to_string_lossy().into_owned(),
                json!({"memory_current_bytes": memory, "cpu_usage_us": cpu}),
            );
        }
        Ok(json!({"groups": groups}))
    };
    read().unwrap_or_else(|error| json!({"unavailable_reason": error.to_string()}))
}

fn checked_sum(values: impl IntoIterator<Item = Option<u64>>) -> Option<u64> {
    values
        .into_iter()
        .try_fold(0_u64, |total, value| total.checked_add(value?))
}

#[allow(clippy::cast_precision_loss)] // Kernel clock rates are small practical values.
fn ticks_per_second(ticks: libc::c_long) -> f64 {
    ticks as f64
}

/// Summarize counters between samples wholly inside the full-load hold.
///
/// # Errors
/// Returns an error if the file or samples are malformed, or no valid interval exists.
#[allow(clippy::cast_precision_loss)] // Benchmark rates and CPU share are floating-point estimates.
pub fn summarize_hold(
    path: &Path,
    start: chrono::DateTime<Utc>,
    finish: chrono::DateTime<Utc>,
) -> Result<Value> {
    let mut first: Option<Point> = None;
    let mut last: Option<Point> = None;
    let mut peak_memory = None;
    for line in BufReader::new(File::open(path)?).lines() {
        let point: Point = serde_json::from_str(&line?)?;
        let at = chrono::DateTime::parse_from_rfc3339(&point.timestamp)?;
        if at < start || at > finish {
            continue;
        }
        if let Some(groups) = point
            .service
            .get("groups")
            .and_then(Value::as_object)
            .filter(|groups| !groups.is_empty())
        {
            let memory = checked_sum(
                groups
                    .values()
                    .map(|group| group.get("memory_current_bytes").and_then(Value::as_u64)),
            );
            peak_memory = peak_memory.max(memory);
        }
        if first.is_none() {
            first = Some(point.clone());
        }
        last = Some(point);
    }
    let (Some(first), Some(last)) = (first, last) else {
        return Err("no telemetry samples inside full-load hold".into());
    };
    let duration = (chrono::DateTime::parse_from_rfc3339(&last.timestamp)?
        - chrono::DateTime::parse_from_rfc3339(&first.timestamp)?)
    .to_std()
    .map_err(|_| "not enough telemetry samples inside hold")?;
    let seconds = duration.as_secs_f64();
    let count = |point: &Point| -> Option<u64> {
        let rows = point.activity.get("requests")?.as_array()?;
        checked_sum(rows.iter().map(|row| row.get(0).and_then(Value::as_u64)))
    };
    let requests = count(&last)
        .zip(count(&first))
        .and_then(|(end, begin)| end.checked_sub(begin));
    let cpu = first
        .service
        .get("groups")
        .and_then(Value::as_object)
        .zip(last.service.get("groups").and_then(Value::as_object))
        .filter(|(begin, end)| !begin.is_empty() && begin.keys().eq(end.keys()))
        .and_then(|(begin, end)| {
            checked_sum(begin.iter().map(|(path, value)| {
                end.get(path)?
                    .get("cpu_usage_us")?
                    .as_u64()?
                    .checked_sub(value.get("cpu_usage_us")?.as_u64()?)
            }))
        });
    Ok(json!({
        "sample_started_at": first.timestamp,
        "sample_finished_at": last.timestamp,
        "sample_duration_s": seconds,
        "requests_per_second": rate(requests, seconds, 1.0),
        "service_cpu_percent": cpu.map(|n| n as f64 / (seconds * 10_000.0)),
        "sampled_peak_memory_bytes": peak_memory,
        "memory_scope": "sampled cgroup memory.current, including descendants, socket buffers and page cache"
    }))
}

fn limits() -> Value {
    let paths = [
        (
            "ephemeral_port_range",
            "/proc/sys/net/ipv4/ip_local_port_range",
        ),
        (
            "reserved_ports",
            "/proc/sys/net/ipv4/ip_local_reserved_ports",
        ),
        ("system_file_max", "/proc/sys/fs/file-max"),
        ("per_process_descriptor_max", "/proc/sys/fs/nr_open"),
        ("system_file_handles", "/proc/sys/fs/file-nr"),
        (
            "conntrack_count",
            "/proc/sys/net/netfilter/nf_conntrack_count",
        ),
        ("conntrack_max", "/proc/sys/net/netfilter/nf_conntrack_max"),
    ];
    let mut values = serde_json::Map::new();
    for (name, path) in paths {
        values.insert(
            name.into(),
            match fs::read_to_string(path) {
                Ok(value) => json!({"value": value.trim(), "unavailable_reason": null}),
                Err(error) => json!({"value": null, "unavailable_reason": error.to_string()}),
            },
        );
    }
    Value::Object(values)
}

#[cfg(test)]
pub(super) fn listener_pids(port: u16) -> Result<Vec<u32>> {
    listener_pids_for_ports(&[port])
}

fn listener_pids_for_ports(ports: &[u16]) -> Result<Vec<u32>> {
    let ports: BTreeSet<_> = ports.iter().copied().collect();
    let mut inodes = BTreeSet::new();
    for file in ["tcp", "tcp6"] {
        for line in fs::read_to_string(format!("/proc/net/{file}"))?
            .lines()
            .skip(1)
        {
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.len() >= 10
                && fields[3] == "0A"
                && fields[1].rsplit_once(':').is_some_and(|(_, p)| {
                    u16::from_str_radix(p, 16).is_ok_and(|port| ports.contains(&port))
                })
            {
                inodes.insert(format!("socket:[{}]", fields[9]));
            }
        }
    }
    let mut pids = Vec::new();
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(fds) = fs::read_dir(entry.path().join("fd")) else {
            continue;
        };
        if fds.filter_map(std::result::Result::ok).any(|fd| {
            fs::read_link(fd.path())
                .is_ok_and(|link| inodes.contains(link.to_string_lossy().as_ref()))
        }) {
            pids.push(pid);
        }
    }
    Ok(pids)
}

fn proc_address(value: &str) -> Result<std::net::SocketAddr> {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    let (address, port) = value.split_once(':').ok_or("invalid TCP address")?;
    let ip = match address.len() {
        8 => IpAddr::V4(Ipv4Addr::from(
            u32::from_str_radix(address, 16)?.to_ne_bytes(),
        )),
        32 => {
            let mut bytes = [0; 16];
            for (chunk, output) in address
                .as_bytes()
                .as_chunks::<8>()
                .0
                .iter()
                .zip(bytes.as_chunks_mut::<4>().0.iter_mut())
            {
                output.copy_from_slice(
                    &u32::from_str_radix(std::str::from_utf8(chunk)?, 16)?.to_ne_bytes(),
                );
            }
            IpAddr::V6(Ipv6Addr::from(bytes))
        }
        _ => return Err("invalid TCP address length".into()),
    };
    Ok(std::net::SocketAddr::new(
        ip,
        u16::from_str_radix(port, 16)?,
    ))
}

fn discover_roles(ports: &[u16]) -> Result<Roles> {
    let listeners = listener_pids_for_ports(ports)?;
    let (proxy, mut application): (Vec<_>, Vec<_>) = listeners.into_iter().partition(|pid| {
        fs::read_to_string(format!("/proc/{pid}/comm")).is_ok_and(|name| {
            matches!(
                name.trim(),
                "nginx"
                    | "haproxy"
                    | "envoy"
                    | "caddy"
                    | "traefik"
                    | "apache2"
                    | "httpd"
                    | "varnishd"
                    | "h2o"
            )
        })
    });
    let mut backend_ports = BTreeSet::new();
    for pid in &proxy {
        let inodes: BTreeSet<_> = fs::read_dir(format!("/proc/{pid}/fd"))?
            .filter_map(std::result::Result::ok)
            .filter_map(|entry| fs::read_link(entry.path()).ok())
            .collect();
        for file in ["tcp", "tcp6"] {
            for line in fs::read_to_string(format!("/proc/{pid}/net/{file}"))?
                .lines()
                .skip(1)
            {
                let fields: Vec<_> = line.split_whitespace().collect();
                if fields.len() < 10
                    || fields[3] != "01"
                    || !inodes.contains(&PathBuf::from(format!("socket:[{}]", fields[9])))
                {
                    continue;
                }
                let peer = proc_address(fields[2])?;
                let unspecified = if peer.is_ipv4() {
                    "0.0.0.0:0"
                } else {
                    "[::]:0"
                };
                let local_peer = peer.ip().is_loopback()
                    || std::net::UdpSocket::bind(unspecified)
                        .and_then(|socket| {
                            socket.connect(peer)?;
                            socket.local_addr()
                        })
                        .is_ok_and(|address| address.ip() == peer.ip());
                if local_peer && !ports.contains(&peer.port()) {
                    backend_ports.insert(peer.port());
                }
            }
        }
    }
    if !backend_ports.is_empty() {
        application.extend(listener_pids_for_ports(
            &backend_ports.into_iter().collect::<Vec<_>>(),
        )?);
        application.retain(|pid| !proxy.contains(pid));
        application.sort_unstable();
        application.dedup();
    }
    Ok(Roles::from([
        ("application".into(), application),
        ("proxy".into(), proxy),
    ]))
}

pub struct Sampler {
    stop: mpsc::Sender<()>,
    thread: Option<thread::JoinHandle<std::result::Result<(), String>>>,
    path: PathBuf,
}
impl Sampler {
    /// Start periodic host and process sampling and wait for the baseline sample.
    ///
    /// # Errors
    /// Returns an error if the output file cannot be created or the baseline fails.
    pub fn start(
        path: PathBuf,
        roles: Roles,
        interval: Duration,
        activity: Arc<dyn Fn() -> Value + Send + Sync>,
    ) -> Result<Self> {
        let file = File::create(&path)?;
        let (stop, receiver) = mpsc::channel();
        let (ready, received_ready) = mpsc::sync_channel(1);
        let thread = thread::spawn(move || {
            let mut ready = Some(ready);
            // sysconf accepts these constant selectors and has no pointer arguments.
            let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
            let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
            if ticks <= 0 || page <= 0 {
                return Err("unable to read kernel tick/page sizes".into());
            }
            let ticks = ticks_per_second(ticks);
            let page = u64::try_from(page).map_err(|_| "invalid kernel page size".to_owned())?;
            let mut writer = BufWriter::new(file);
            let started = Instant::now();
            let mut identities = BTreeMap::new();
            let mut final_sample = false;
            let mut previous: Option<Point> = None;
            loop {
                let mut errors = Vec::new();
                let interfaces = fs::read_to_string("/proc/net/dev")
                    .map_err(|e| e.to_string())
                    .and_then(|text| interfaces(&text).map_err(|e| e.to_string()))
                    .map_err(|e| errors.push(format!("network: {e}")))
                    .ok();
                let mut processes = BTreeMap::new();
                let sockets = socket_inodes();
                for (role, pids) in &roles {
                    let mut data = Vec::new();
                    for pid in pids {
                        match process(*pid, ticks, page, &sockets) {
                            Ok(data_point) => {
                                let original =
                                    identities.entry(*pid).or_insert(data_point.start_ticks);
                                if *original == data_point.start_ticks {
                                    data.push(data_point);
                                } else {
                                    errors.push(format!(
                                        "{role} PID {pid} was reused; later process excluded"
                                    ));
                                }
                            }
                            Err(error) => errors.push(format!("{role} PID {pid}: {error}")),
                        }
                    }
                    processes.insert(role.clone(), data);
                }
                let tcp = fs::read_to_string("/proc/net/snmp")
                    .map_err(|e| e.to_string())
                    .and_then(|text| tcp_counters(&text).map_err(|e| e.to_string()))
                    .map_err(|e| errors.push(format!("TCP counters: {e}")))
                    .ok();
                let mut point = Point {
                    timestamp: Utc::now().to_rfc3339(),
                    elapsed_s: started.elapsed().as_secs_f64(),
                    host_boot_id: fs::read_to_string("/proc/sys/kernel/random/boot_id")
                        .ok()
                        .map(|s| s.trim().to_owned()),
                    network_namespace: fs::read_link("/proc/self/ns/net")
                        .ok()
                        .map(|s| s.to_string_lossy().into_owned()),
                    sample_interval_s: interval.as_secs_f64(),
                    interfaces,
                    processes,
                    limits: limits(),
                    activity: activity(),
                    service: service_resources(&roles),
                    tcp,
                    interval_rates: Value::Null,
                    errors,
                };
                if let Some(begin) = &previous {
                    point.interval_rates = interval_report(begin, &point);
                }
                serde_json::to_writer(&mut writer, &point).map_err(|e| e.to_string())?;
                writer.write_all(b"\n").map_err(|e| e.to_string())?;
                writer.flush().map_err(|e| e.to_string())?;
                previous = Some(point);
                if let Some(sender) = ready.take() {
                    let _ = sender.send(());
                }
                if final_sample {
                    break;
                }
                match receiver.recv_timeout(interval) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => final_sample = true,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
            // A final point captures counters since the last regular sample.
            // Stop is requested after native requests finish; the summary uses the last available interval.
            Ok(())
        });
        received_ready
            .recv()
            .map_err(|_| "telemetry sampler failed before its baseline")?;
        Ok(Self {
            stop,
            thread: Some(thread),
            path,
        })
    }
    /// Stop sampling and summarize the collected measurements.
    ///
    /// # Errors
    /// Returns an error if the sampler thread or summary data cannot be read.
    ///
    pub fn finish(mut self) -> Result<Value> {
        let _ = self.stop.send(());
        let handle = self
            .thread
            .take()
            .ok_or("telemetry sampler thread was already joined")?;
        let result = handle.join().map_err(|_| "telemetry sampler panicked")?;
        result.map_err(|e| -> Box<dyn Error> { e.into() })?;
        summarize(&self.path)
    }
}
impl Drop for Sampler {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

fn tcp_counters(text: &str) -> Result<BTreeMap<String, u64>> {
    let mut rows = text.lines().filter_map(|row| row.strip_prefix("Tcp:"));
    let names: Vec<_> = rows
        .next()
        .ok_or("missing TCP counter names")?
        .split_whitespace()
        .collect();
    let values: Vec<_> = rows
        .next()
        .ok_or("missing TCP counter values")?
        .split_whitespace()
        .collect();
    if names.len() != values.len() {
        return Err("incomplete TCP counters".into());
    }
    names
        .into_iter()
        .zip(values)
        .filter(|(name, _)| {
            matches!(
                *name,
                "ActiveOpens"
                    | "PassiveOpens"
                    | "AttemptFails"
                    | "EstabResets"
                    | "CurrEstab"
                    | "RetransSegs"
            )
        })
        .map(|(name, value)| Ok((name.to_owned(), value.parse()?)))
        .collect()
}

#[allow(clippy::cast_precision_loss)] // Rates are reported as floating-point metrics.
fn rate(delta: Option<u64>, seconds: f64, scale: f64) -> Option<f64> {
    if seconds > 0.0 {
        delta.map(|n| n as f64 * scale / seconds)
    } else {
        None
    }
}

fn interval_seconds(begin: &Point, end: &Point) -> f64 {
    end.elapsed_s - begin.elapsed_s
}

fn network_report(begin: &Point, end: &Point) -> Value {
    let seconds = interval_seconds(begin, end);
    let (Some(first), Some(last)) = (&begin.interfaces, &end.interfaces) else {
        return json!({"interfaces": null, "unavailable_reason": "network interface counters unavailable"});
    };
    let mut result = BTreeMap::new();
    for name in first.keys().chain(last.keys()).collect::<BTreeSet<_>>() {
        let (Some(start), Some(finish)) = (first.get(name), last.get(name)) else {
            result.insert(
                name,
                json!({"unavailable_reason": "interface was absent at an interval boundary"}),
            );
            continue;
        };
        let delta = [
            finish.rx_bytes.checked_sub(start.rx_bytes),
            finish.tx_bytes.checked_sub(start.tx_bytes),
            finish.rx_packets.checked_sub(start.rx_packets),
            finish.tx_packets.checked_sub(start.tx_packets),
        ];
        result.insert(name, json!({"receive_bytes": delta[0], "transmit_bytes": delta[1], "received_packets": delta[2], "transmitted_packets": delta[3],
            "receive_bits_per_second": rate(delta[0], seconds, 8.0), "transmit_bits_per_second": rate(delta[1], seconds, 8.0),
            "receive_packets_per_second": rate(delta[2], seconds, 1.0), "transmit_packets_per_second": rate(delta[3], seconds, 1.0),
            "unavailable_reason": if delta.iter().any(Option::is_none) {Some("interface counters reset")} else if seconds <= 0.0 { Some("no elapsed observation interval") } else {None}}));
    }
    json!(result)
}

fn tcp_report(begin: &Point, end: &Point) -> Value {
    let (Some(first), Some(last)) = (&begin.tcp, &end.tcp) else {
        return json!({"unavailable_reason": "TCP namespace counters unavailable"});
    };
    let seconds = interval_seconds(begin, end);
    let mut report = BTreeMap::new();
    for (name, value) in last {
        if name == "CurrEstab" {
            report.insert(name, json!({"value": value}));
        } else {
            let delta = first.get(name).and_then(|start| value.checked_sub(*start));
            report.insert(name, json!({"count": delta, "per_second": rate(delta, seconds, 1.0), "unavailable_reason": if delta.is_none() {Some("counter unavailable or reset")} else {None}}));
        }
    }
    json!(report)
}

fn activity_report(begin: &Point, end: &Point) -> Value {
    let seconds = interval_seconds(begin, end);
    let (Some(first), Some(last)) = (
        begin.activity.get("requests").and_then(Value::as_array),
        end.activity.get("requests").and_then(Value::as_array),
    ) else {
        return Value::Null;
    };
    let mut requests = BTreeMap::new();
    for (index, name) in crate::ENDPOINTS.iter().enumerate() {
        let delta = |field| {
            first
                .get(index)
                .and_then(|v| v.get(field))
                .and_then(Value::as_u64)
                .zip(
                    last.get(index)
                        .and_then(|v| v.get(field))
                        .and_then(Value::as_u64),
                )
                .and_then(|(a, b)| b.checked_sub(a))
        };
        let count = delta(0);
        let failed = delta(1);
        let successful = count
            .zip(failed)
            .and_then(|(all, failed)| all.checked_sub(failed));
        requests.insert(name, json!({"requests": count, "successful": successful, "failed": failed,
            "requests_per_second": rate(count, seconds, 1.0), "successful_requests_per_second": rate(successful, seconds, 1.0), "failed_requests_per_second": rate(failed, seconds, 1.0)}));
    }
    let connection_delta = |index| {
        begin
            .activity
            .get("connections")
            .and_then(|v| v.get(index))
            .and_then(Value::as_u64)
            .zip(
                end.activity
                    .get("connections")
                    .and_then(|v| v.get(index))
                    .and_then(Value::as_u64),
            )
            .and_then(|(a, b)| b.checked_sub(a))
    };
    let established = connection_delta(0);
    let failed = connection_delta(1);
    json!({"endpoints": requests, "connections": {"established": established, "failed": failed, "cancelled": connection_delta(2),
        "established_per_second": rate(established, seconds, 1.0), "failed_per_second": rate(failed, seconds, 1.0)}})
}

fn interval_report(begin: &Point, end: &Point) -> Value {
    let seconds = interval_seconds(begin, end);
    let mut cpu = BTreeMap::new();
    for (role, processes) in &end.processes {
        let reports: Vec<_> = processes.iter().map(|process| {
            let delta = begin.processes.get(role).into_iter().flatten().find(|p| p.pid == process.pid && p.start_ticks == process.start_ticks)
                .map(|p| process.cpu_seconds - p.cpu_seconds).filter(|n| *n >= 0.0);
            json!({"pid": process.pid, "cpu_seconds": delta, "cpu_percent_of_one_core": if seconds > 0.0 {delta.map(|n| n / seconds * 100.0)} else {None}})
        }).collect();
        cpu.insert(role, reports);
    }
    json!({"duration_s": seconds, "network": network_report(begin,end), "tcp": tcp_report(begin,end), "activity": activity_report(begin,end), "cpu": cpu})
}

struct ProcessTotal {
    first_at: f64,
    last_at: f64,
    first_cpu: f64,
    last: Process,
    peak_resident: u64,
    peak_descriptors: Option<usize>,
    peak_sockets: Option<usize>,
    errors: BTreeSet<String>,
}

fn add_errors(destination: &mut BTreeSet<String>, errors: impl IntoIterator<Item = String>) {
    for error in errors {
        if destination.len() < 128 {
            destination.insert(error.chars().take(512).collect());
        }
    }
}

pub(crate) fn summarize(path: &Path) -> Result<Value> {
    // Keep only endpoint counters and peaks; a long trace must not become a second in-memory copy.
    let mut first: Option<Point> = None;
    let mut last: Option<Point> = None;
    let mut samples = 0;
    let mut roles = BTreeSet::new();
    let mut processes: BTreeMap<(String, u32, u64), ProcessTotal> = BTreeMap::new();
    let mut errors = BTreeSet::new();
    let mut peak_established = None;
    let mut peak_conntrack = None;
    for line in BufReader::new(File::open(path)?).lines() {
        let point: Point = serde_json::from_str(&line?)?;
        if first.is_none() {
            first = Some(point.clone());
        }
        samples += 1;
        peak_conntrack = peak_conntrack.max(
            point.limits["conntrack_count"]["value"]
                .as_str()
                .and_then(|n| n.parse::<u64>().ok()),
        );
        add_errors(&mut errors, point.errors.clone());
        peak_established = peak_established.max(
            point
                .tcp
                .as_ref()
                .and_then(|tcp| tcp.get("CurrEstab"))
                .copied(),
        );
        for (role, data) in &point.processes {
            roles.insert(role.clone());
            for process in data {
                let aggregate = processes
                    .entry((role.clone(), process.pid, process.start_ticks))
                    .or_insert_with(|| ProcessTotal {
                        first_at: point.elapsed_s,
                        last_at: point.elapsed_s,
                        first_cpu: process.cpu_seconds,
                        last: process.clone(),
                        peak_resident: process.resident_bytes,
                        peak_descriptors: process.open_descriptors,
                        peak_sockets: process.established_sockets,
                        errors: BTreeSet::new(),
                    });
                aggregate.last_at = point.elapsed_s;
                aggregate.peak_resident = aggregate.peak_resident.max(process.resident_bytes);
                aggregate.peak_descriptors =
                    aggregate.peak_descriptors.max(process.open_descriptors);
                aggregate.peak_sockets = aggregate.peak_sockets.max(process.established_sockets);
                add_errors(&mut aggregate.errors, process.errors.clone());
                aggregate.last = process.clone();
            }
        }
        last = Some(point);
    }
    let first = first.ok_or("no telemetry samples")?;
    let last = last.unwrap();
    let mut process_report = BTreeMap::new();
    for role in roles {
        let collected: Vec<_> = processes.iter().filter(|((process_role, _, _), _)| process_role == &role).map(|((_, pid, start_ticks), aggregate)| {
            let elapsed = aggregate.last_at - aggregate.first_at;
            let cpu = aggregate.last.cpu_seconds - aggregate.first_cpu;
            json!({"pid": pid, "name": aggregate.last.name, "start_ticks": start_ticks, "observed_duration_s": elapsed,
                "cpu_seconds": (cpu >= 0.0).then_some(cpu), "cpu_percent_of_one_core": (elapsed > 0.0 && cpu >= 0.0).then(|| cpu / elapsed * 100.0),
                "peak_resident_bytes": aggregate.peak_resident, "peak_open_descriptors": aggregate.peak_descriptors,
                "peak_established_sockets": aggregate.peak_sockets, "descriptor_limits": aggregate.last.descriptor_limits, "errors": aggregate.errors})
        }).collect();
        process_report.insert(role, json!({"processes": collected, "unavailable_reason": if collected.is_empty() { Some("no selected accessible process; supply the role's process IDs on the target host") } else {None}}));
    }
    Ok(
        json!({"host_boot_id": first.host_boot_id, "network_namespace": first.network_namespace, "sample_interval_s": first.sample_interval_s,
        "started_at": first.timestamp, "finished_at": last.timestamp, "observed_duration_s": last.elapsed_s-first.elapsed_s, "samples": samples,
        "network_scope": "per-interface host/network-namespace counters include unrelated traffic; do not add virtual links or both ends of a connection; packet counters can reflect offloads, not physical wire packets",
        "network": network_report(&first,&last), "tcp": tcp_report(&first,&last), "peak_namespace_established_sockets": peak_established, "activity": activity_report(&first,&last),
        "processes": process_report, "limits": last.limits, "peak_conntrack_count": peak_conntrack, "errors": errors,
        "process_scope": "CPU includes every process thread and is expressed relative to one CPU core; memory, descriptors and established sockets are sampled peaks"}),
    )
}

/// Run the target-side telemetry observer until its control lease ends.
///
/// # Errors
/// Returns an error if telemetry sampling or summary output fails.
pub fn observe(
    control: &Path,
    application_pids: Vec<u32>,
    proxy_pids: Vec<u32>,
    ports: &[u16],
    interval: Duration,
) -> Result<u8> {
    let discovery = discover_roles(ports);
    let discovery_error = discovery.as_ref().err().map(ToString::to_string);
    let mut roles = discovery.unwrap_or_default();
    if !application_pids.is_empty() {
        roles.insert("application".into(), application_pids);
    }
    if !proxy_pids.is_empty() {
        roles
            .entry("application".into())
            .or_default()
            .retain(|pid| !proxy_pids.contains(pid));
        roles.insert("proxy".into(), proxy_pids);
    }
    roles.entry("application".into()).or_default();
    roles.entry("proxy".into()).or_default();
    roles.insert("observer".into(), vec![std::process::id()]);
    let result = (|| {
        let lease_seconds: f64 = crate::setting("BENCH_OBSERVER_LEASE_SECONDS", "120").parse()?;
        if !lease_seconds.is_finite() || lease_seconds <= 0.0 {
            return Err("invalid observer lease duration".into());
        }
        let lease_duration = Duration::try_from_secs_f64(lease_seconds)?;
        let observer_started = Instant::now();
        let start_at = crate::control::wait_start(control)?;
        while Utc::now().timestamp_millis() < start_at {
            if control.exists() {
                return Err("target observation cancelled before start".into());
            }
            if observer_lease_expired(control, observer_started, lease_duration) {
                save_observer_summary(&json!({
                    "stopped_by_lease_timeout": true,
                    "observer_lease_seconds": lease_seconds,
                    "unavailable_reason": "coordinator lease expired before the coordinated start"
                }))?;
                return Ok(0);
            }
            thread::sleep(Duration::from_millis(10));
        }
        let sampler = Sampler::start(
            PathBuf::from("telemetry.jsonl"),
            roles,
            interval,
            Arc::new(|| Value::Null),
        )?;
        // Coordinator heartbeats renew this lease through startup, load and drain.
        // SSH disconnection alone cannot leave an observer running indefinitely.
        while !control.exists()
            && !observer_lease_expired(control, observer_started, lease_duration)
        {
            thread::sleep(Duration::from_millis(100));
        }
        let timed_out = !control.exists();
        let mut summary = sampler.finish()?;
        summary["process_discovery"] = json!({"method": "listening socket owners; recognized proxy processes and their observed local TCP backends",
            "limitation": "custom proxies, remote backends, Unix sockets and idle proxy pools may not be identifiable; optional PID overrides remain available",
            "error": discovery_error});
        if summary["processes"]["proxy"]["processes"]
            .as_array()
            .is_some_and(Vec::is_empty)
        {
            summary["processes"]["proxy"]["unavailable_reason"] =
                json!("no recognized proxy detected among target listener processes");
        }
        summary["stopped_by_lease_timeout"] = json!(timed_out);
        summary["observer_lease_seconds"] = json!(lease_seconds);
        if timed_out {
            summary["unavailable_reason"] =
                json!("coordinator lease expired before target observation stopped");
        }
        save_observer_summary(&summary)?;
        Ok(0)
    })();
    fs::write(
        control.with_extension("done"),
        if result.is_ok() {
            "observation complete\n"
        } else {
            "observation failed\n"
        },
    )?;
    result
}

fn observer_lease_expired(control: &Path, started: Instant, duration: Duration) -> bool {
    let elapsed = fs::metadata(control.with_extension("lease"))
        .and_then(|metadata| metadata.modified())
        .map_or_else(
            |_| started.elapsed(),
            |modified| modified.elapsed().unwrap_or(Duration::ZERO),
        );
    elapsed >= duration
}

fn save_observer_summary(summary: &Value) -> Result<()> {
    let mut output = BufWriter::new(File::create("telemetry-summary.json")?);
    serde_json::to_writer_pretty(&mut output, summary)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        net::{TcpListener, TcpStream},
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn checked_sum_marks_missing_and_overflowing_counters_unavailable() {
        assert_eq!(checked_sum([Some(2), Some(3)]), Some(5));
        assert_eq!(checked_sum([Some(u64::MAX), Some(1)]), None);
        assert_eq!(checked_sum([Some(2), None]), None);
    }

    #[test]
    fn decodes_proc_tcp_addresses_and_discovers_direct_application() {
        assert_eq!(
            proc_address("0100007F:0C1C").unwrap().to_string(),
            "127.0.0.1:3100"
        );
        assert_eq!(
            proc_address("00000000000000000000000001000000:0C1C")
                .unwrap()
                .to_string(),
            "[::1]:3100"
        );
        assert!(proc_address("bad").is_err());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let roles = discover_roles(&[listener.local_addr().unwrap().port()]).unwrap();
        assert!(roles["application"].contains(&std::process::id()));
        assert_eq!(roles["proxy"], Vec::<u32>::new());
    }

    fn fixture(elapsed: f64) -> Point {
        Point {
            timestamp: format!("sample-{elapsed}"),
            elapsed_s: elapsed,
            host_boot_id: Some("boot-a".into()),
            network_namespace: Some("net:[a]".into()),
            sample_interval_s: 1.0,
            interfaces: Some(BTreeMap::from([(
                "eth0".into(),
                Interface {
                    rx_bytes: 100,
                    tx_bytes: 200,
                    rx_packets: 10,
                    tx_packets: 20,
                },
            )])),
            processes: BTreeMap::from([
                (
                    "application".into(),
                    vec![Process {
                        pid: 42,
                        name: "app".into(),
                        start_ticks: 1000,
                        cpu_seconds: 1.0,
                        resident_bytes: 4096,
                        open_descriptors: Some(4),
                        established_sockets: Some(2),
                        descriptor_limits: json!({"soft":"1024","hard":"2048"}),
                        errors: vec![],
                    }],
                ),
                ("proxy".into(), vec![]),
            ]),
            limits: json!({}),
            activity: json!({"requests":[[0,0],[0,0],[0,0],[0,0]],"connections":[0,0,0]}),
            tcp: Some(BTreeMap::from([
                ("ActiveOpens".into(), 10),
                ("PassiveOpens".into(), 4),
                ("CurrEstab".into(), 2),
            ])),
            interval_rates: Value::Null,
            service: Value::Null,
            errors: vec![],
        }
    }

    fn temporary() -> PathBuf {
        static SERIAL: AtomicUsize = AtomicUsize::new(0);
        std::env::temp_dir().join(format!(
            "feed-telemetry-{}-{}-{}.jsonl",
            std::process::id(),
            Utc::now().timestamp_micros(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn observer_lease_renews_and_handles_missing_or_future_timestamps() {
        let control = temporary();
        let lease = control.with_extension("lease");
        let duration = Duration::from_secs(2);
        let old_start = Instant::now()
            .checked_sub(Duration::from_secs(3))
            .expect("monotonic clock supports the test interval");
        assert!(observer_lease_expired(&control, old_start, duration));
        assert!(!observer_lease_expired(&control, Instant::now(), duration));
        fs::write(&lease, "heartbeat\n").unwrap();
        assert!(!observer_lease_expired(&control, old_start, duration));
        let file = File::options().write(true).open(&lease).unwrap();
        file.set_times(fs::FileTimes::new().set_modified(std::time::UNIX_EPOCH))
            .unwrap();
        assert!(observer_lease_expired(&control, Instant::now(), duration));
        file.set_times(
            fs::FileTimes::new()
                .set_modified(std::time::SystemTime::now() + Duration::from_secs(10)),
        )
        .unwrap();
        assert!(!observer_lease_expired(&control, old_start, duration));
        fs::remove_file(lease).unwrap();
    }

    #[test]
    fn reads_counter_columns_and_process_names_containing_parentheses() {
        let network =
            interfaces("header\nheader\n lo: 100 3 0 0 0 0 0 0 200 4 0 0 0 0 0 0\n").unwrap();
        assert_eq!(network["lo"].tx_bytes, 200);
        assert_eq!(network["lo"].rx_packets, 3);
        let mut fields = vec!["0"; 22];
        fields[0] = "R";
        fields[11] = "15";
        fields[12] = "7";
        fields[19] = "234";
        fields[21] = "10";
        assert_eq!(
            process_stat(&format!("42 (name (with) spaces) {}", fields.join(" "))).unwrap(),
            ("name (with) spaces".into(), 234, 22, 10)
        );
        assert!(interfaces("header\nheader\n lo: 1 2").is_err());
    }

    #[test]
    fn computes_interval_activity_network_and_namespace_connection_rates() {
        let begin = fixture(1.0);
        let mut end = fixture(3.0);
        let iface = end.interfaces.as_mut().unwrap().get_mut("eth0").unwrap();
        iface.rx_bytes += 1000;
        iface.tx_bytes += 500;
        iface.rx_packets += 8;
        iface.tx_packets += 6;
        end.activity = json!({"requests":[[10,2],[2,1],[0,0],[0,0]],"connections":[4,2,1]});
        *end.tcp.as_mut().unwrap().get_mut("ActiveOpens").unwrap() = 16;
        let report = interval_report(&begin, &end);
        assert_eq!(report["network"]["eth0"]["receive_bits_per_second"], 4000.0);
        assert_eq!(
            report["network"]["eth0"]["transmit_packets_per_second"],
            3.0
        );
        assert_eq!(
            report["activity"]["endpoints"]["GET /feed"]["successful_requests_per_second"],
            4.0
        );
        assert_eq!(
            report["activity"]["endpoints"]["GET /feed"]["failed_requests_per_second"],
            1.0
        );
        assert_eq!(
            report["activity"]["connections"]["established_per_second"],
            2.0
        );
        assert_eq!(report["tcp"]["ActiveOpens"]["per_second"], 3.0);
        assert!(
            tcp_counters("Tcp: ActiveOpens PassiveOpens CurrEstab\nTcp: 10 4 2\n")
                .unwrap()
                .contains_key("CurrEstab")
        );
        assert!(tcp_counters("Tcp: ActiveOpens PassiveOpens\nTcp: 10\n").is_err());
    }

    #[test]
    fn resets_and_missing_metrics_are_unavailable_instead_of_zero() {
        let begin = fixture(0.0);
        let mut end = fixture(2.0);
        end.interfaces
            .as_mut()
            .unwrap()
            .get_mut("eth0")
            .unwrap()
            .rx_bytes = 1;
        let report = network_report(&begin, &end);
        assert!(report["eth0"]["receive_bits_per_second"].is_null());
        assert_eq!(
            report["eth0"]["unavailable_reason"],
            "interface counters reset"
        );
        end.interfaces = None;
        assert!(network_report(&begin, &end)["interfaces"].is_null());
        end.activity = Value::Null;
        assert!(activity_report(&begin, &end).is_null());
        assert!(rate(Some(0), 0.0, 1.0).is_none());
    }

    #[test]
    fn streaming_summary_keeps_interior_peaks_and_process_identity() {
        let path = temporary();
        let first = fixture(0.0);
        let mut middle = fixture(1.0);
        let mut last = fixture(2.0);
        let process = middle
            .processes
            .get_mut("application")
            .unwrap()
            .first_mut()
            .unwrap();
        process.resident_bytes = 16384;
        process.open_descriptors = Some(8);
        process.established_sockets = Some(6);
        process.cpu_seconds = 1.5;
        last.processes
            .get_mut("application")
            .unwrap()
            .first_mut()
            .unwrap()
            .cpu_seconds = 2.0;
        let mut output = File::create(&path).unwrap();
        for point in [first, middle, last] {
            serde_json::to_writer(&mut output, &point).unwrap();
            output.write_all(b"\n").unwrap();
        }
        drop(output);
        let report = summarize(&path).unwrap();
        fs::remove_file(path).unwrap();
        let process = &report["processes"]["application"]["processes"][0];
        assert_eq!(process["peak_resident_bytes"], 16384);
        assert_eq!(process["peak_open_descriptors"], 8);
        assert_eq!(process["peak_established_sockets"], 6);
        assert_eq!(process["cpu_percent_of_one_core"], 50.0);
        assert_eq!(report["host_boot_id"], "boot-a");
        assert!(
            report["processes"]["proxy"]["unavailable_reason"]
                .as_str()
                .is_some()
        );
    }

    #[test]
    fn sampler_flushes_baseline_and_final_activity_even_before_regular_interval() {
        let path = temporary();
        let count = Arc::new(AtomicUsize::new(0));
        let activity = count.clone();
        let sampler=Sampler::start(path.clone(),Roles::from([("generator".into(),vec![std::process::id()])]),Duration::from_secs(60),Arc::new(move||json!({"requests":[[activity.load(Ordering::Relaxed),0],[0,0],[0,0],[0,0]],"connections":[0,0,0]}))).unwrap();
        assert_eq!(
            BufReader::new(File::open(&path).unwrap()).lines().count(),
            1
        );
        count.store(4, Ordering::Relaxed);
        let report = sampler.finish().unwrap();
        assert_eq!(report["samples"], 2);
        assert_eq!(report["activity"]["endpoints"]["GET /feed"]["requests"], 4);
        assert!(
            report["processes"]["generator"]["processes"][0]["peak_open_descriptors"]
                .as_u64()
                .is_some()
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn discovers_local_listener_and_counts_open_established_socket_descriptors() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        assert!(
            listener_pids(listener.local_addr().unwrap().port())
                .unwrap()
                .contains(&std::process::id())
        );
        let observed = process(std::process::id(), 100.0, 4096, &socket_inodes()).unwrap();
        assert!(observed.established_sockets.unwrap() >= 2);
        assert!(observed.open_descriptors.unwrap() >= 3);
        assert!(observed.descriptor_limits["soft"].as_str().is_some());
        drop((client, server));
    }
}
