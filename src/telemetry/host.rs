use super::{Result, Roles};
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Interface {
    pub(super) rx_bytes: u64,
    pub(super) tx_bytes: u64,
    pub(super) rx_packets: u64,
    pub(super) tx_packets: u64,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Process {
    pub(super) pid: u32,
    pub(super) name: String,
    pub(super) start_ticks: u64,
    pub(super) cpu_seconds: f64,
    pub(super) resident_bytes: u64,
    pub(super) open_descriptors: Option<usize>,
    pub(super) established_sockets: Option<usize>,
    pub(super) descriptor_limits: Value,
    pub(super) errors: Vec<String>,
}
pub(super) fn interfaces(text: &str) -> Result<BTreeMap<String, Interface>> {
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

pub(super) fn socket_inodes() -> Result<HashSet<u64>> {
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
        read_socket_dump(socket.as_raw_fd(), &mut inodes)?;
    }
    Ok(inodes)
}

pub(super) fn read_socket_dump(fd: std::os::fd::RawFd, inodes: &mut HashSet<u64>) -> Result<()> {
    let mut buffer = vec![0_u8; 65_536].into_boxed_slice();
    'dump: loop {
        // SAFETY: buffer is writable for its entire stated length.
        let received = unsafe {
            libc::recv(
                fd,
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
            let length = usize::try_from(u32::from_ne_bytes(header[..4].try_into()?))?;
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
                2 => check_diagnostic_error(payload)?,
                20 => {
                    inodes.insert(diagnostic_inode(payload)?);
                }
                _ => {}
            }
            offset += (length + 3) & !3;
        }
    }
    Ok(())
}

pub(super) fn check_diagnostic_error(payload: &[u8]) -> Result<()> {
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
    Ok(())
}

pub(super) fn diagnostic_inode(payload: &[u8]) -> Result<u64> {
    let inode = u32::from_ne_bytes(
        payload
            .get(68..72)
            .ok_or("truncated socket diagnostic entry")?
            .try_into()?,
    );
    Ok(u64::from(inode))
}

pub(super) fn process_stat(text: &str) -> Result<(String, u64, u64, u64)> {
    let (prefix, tail) = text.rsplit_once(") ").ok_or("invalid process stat")?;
    let name = prefix
        .split_once('(')
        .ok_or("missing process name")?
        .1
        .to_owned();
    let mut fields = tail.split_whitespace();
    let mut field = || fields.next().ok_or("incomplete process stat");
    let _state = field()?;
    for _ in 0..10 {
        field()?;
    }
    let user_ticks: u64 = field()?.parse()?;
    let system_ticks: u64 = field()?.parse()?;
    for _ in 0..6 {
        field()?;
    }
    let start_ticks = field()?.parse()?;
    let _virtual_bytes = field()?;
    let resident_pages = field()?.parse()?;
    Ok((
        name,
        start_ticks,
        user_ticks.saturating_add(system_ticks),
        resident_pages,
    ))
}

#[cfg(target_os = "linux")]
pub(super) fn count_descriptors(
    pid: u32,
    sockets: Option<&HashSet<u64>>,
) -> Result<(usize, Option<usize>, Vec<std::io::Error>)> {
    use std::{
        ffi::{CStr, CString},
        os::fd::{AsRawFd, FromRawFd, OwnedFd},
    };

    struct Directory(*mut libc::DIR);
    impl Drop for Directory {
        fn drop(&mut self) {
            // SAFETY: this guard uniquely owns the DIR returned by fdopendir.
            unsafe {
                libc::closedir(self.0);
            }
        }
    }

    let path = CString::new(format!("/proc/{pid}/fd"))?;
    // SAFETY: path is NUL-terminated and flags require no additional arguments.
    let raw = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if raw < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: open returned a new owned descriptor.
    let owned = unsafe { OwnedFd::from_raw_fd(raw) };
    // fdopendir takes ownership of the descriptor, so transfer it from OwnedFd.
    let raw = owned.as_raw_fd();
    std::mem::forget(owned);
    // SAFETY: raw is a valid directory descriptor; fdopendir owns it on success.
    let directory = unsafe { libc::fdopendir(raw) };
    if directory.is_null() {
        let error = std::io::Error::last_os_error();
        // SAFETY: fdopendir failed and did not consume the descriptor.
        drop(unsafe { OwnedFd::from_raw_fd(raw) });
        return Err(error.into());
    }
    let directory = Directory(directory);
    let mut count = 0;
    let mut established = HashSet::with_capacity(sockets.map_or(0, HashSet::len));
    let mut socket_errors = Vec::new();
    let mut socket_complete = sockets.is_some();
    let mut link = [0_u8; 256];
    loop {
        // readdir returns a borrowed dirent valid until the next readdir call.
        // SAFETY: directory is live and exclusively used by this loop.
        unsafe {
            *libc::__errno_location() = 0;
        }
        // SAFETY: directory is live.
        let entry = unsafe { libc::readdir(directory.0) };
        if entry.is_null() {
            // SAFETY: errno is thread-local and readdir was just called.
            let errno = unsafe { *libc::__errno_location() };
            if errno != 0 {
                return Err(std::io::Error::from_raw_os_error(errno).into());
            }
            break;
        }
        // SAFETY: readdir supplies a NUL-terminated name in the live dirent.
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if name.to_bytes() == b"." || name.to_bytes() == b".." {
            continue;
        }
        count += 1;
        let Some(sockets) = sockets else { continue };
        // SAFETY: name is NUL-terminated; link is writable for its full length.
        let len = unsafe {
            libc::readlinkat(
                libc::dirfd(directory.0),
                name.as_ptr(),
                link.as_mut_ptr().cast(),
                link.len(),
            )
        };
        if len < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::NotFound {
                continue;
            }
            socket_errors.push(error);
            socket_complete = false;
            continue;
        }
        let len = usize::try_from(len)?;
        // A socket target is short; a full buffer means another target type, not
        // a failed sample. This also avoids treating truncated links as sockets.
        if len == link.len() {
            continue;
        }
        if let Some(inode) = link[..len]
            .strip_prefix(b"socket:[")
            .and_then(|target| target.strip_suffix(b"]"))
            .and_then(|inode| std::str::from_utf8(inode).ok())
            .and_then(|inode| inode.parse::<u64>().ok())
            .filter(|inode| sockets.contains(inode))
        {
            established.insert(inode);
        }
    }
    Ok((
        count,
        (socket_complete && sockets.is_some()).then_some(established.len()),
        socket_errors,
    ))
}

pub(super) fn process(
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
    let (open_descriptors, established_sockets) = match count_descriptors(
        pid,
        sockets.as_ref().ok().copied().filter(|_| same_namespace),
    ) {
        Ok((count, established, socket_errors)) => {
            errors.extend(socket_errors.into_iter().map(|error| error.to_string()));
            (Some(count), established)
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
        cpu_seconds: cpu_ticks
            .to_f64()
            .ok_or("kernel CPU tick counter exceeds floating-point range")?
            / ticks,
        resident_bytes: resident_pages.saturating_mul(page_size),
        open_descriptors,
        established_sockets,
        descriptor_limits,
        errors,
    })
}

pub(super) fn service_resources(roles: &Roles) -> Value {
    let read = || -> Result<Value> {
        let mut paths = BTreeSet::new();
        for role in ["application", "proxy", "generator"] {
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

pub(super) fn ticks_per_second(ticks: libc::c_long) -> f64 {
    ticks.to_f64().expect("kernel tick rate fits in f64")
}

pub(super) fn limits() -> Value {
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

pub(super) fn listener_pids_for_ports(ports: &[u16]) -> Result<Vec<u32>> {
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

pub(super) fn proc_address(value: &str) -> Result<std::net::SocketAddr> {
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

pub(super) fn discover_roles(ports: &[u16]) -> Result<Roles> {
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

pub(super) fn tcp_counters(text: &str) -> Result<BTreeMap<String, u64>> {
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
