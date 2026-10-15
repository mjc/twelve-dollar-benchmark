use super::*;
use super::{
    host::{count_descriptors, listener_pids, proc_address, process_stat},
    summary::{activity_report, checked_sum, network_report, rate},
};
use std::io::{BufRead, BufReader};
use std::{
    net::{TcpListener, TcpStream},
    os::fd::{AsRawFd, FromRawFd},
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
        fs::FileTimes::new().set_modified(std::time::SystemTime::now() + Duration::from_secs(10)),
    )
    .unwrap();
    assert!(!observer_lease_expired(&control, old_start, duration));
    fs::remove_file(lease).unwrap();
}

#[test]
fn reads_counter_columns_and_process_names_containing_parentheses() {
    let network = interfaces("header\nheader\n lo: 100 3 0 0 0 0 0 0 200 4 0 0 0 0 0 0\n").unwrap();
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
    let socket_dump = socket_inodes();
    let (before_count, before_sockets, _) =
        count_descriptors(std::process::id(), socket_dump.as_ref().ok()).unwrap();
    // SAFETY: dup creates an independent descriptor referring to the live client socket.
    let duplicate = unsafe { libc::dup(client.as_raw_fd()) };
    assert!(duplicate >= 0);
    // SAFETY: duplicate is a fresh descriptor owned by this test.
    let duplicate = unsafe { std::os::fd::OwnedFd::from_raw_fd(duplicate) };
    let (after_count, after_sockets, _) =
        count_descriptors(std::process::id(), socket_dump.as_ref().ok()).unwrap();
    assert_eq!(after_count, before_count + 1);
    assert_eq!(after_sockets, before_sockets);

    let long_dir = temporary().with_extension("long-fd");
    let target = long_dir
        .join("a".repeat(100))
        .join("b".repeat(100))
        .join("target");
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::write(&target, "long symlink target").unwrap();
    let long_target = File::open(&target).unwrap();
    let (count_with_long_target, _, _) =
        count_descriptors(std::process::id(), socket_dump.as_ref().ok()).unwrap();
    assert_eq!(count_with_long_target, after_count + 1);
    drop(long_target);
    fs::remove_dir_all(long_dir).unwrap();

    assert!(
        listener_pids(listener.local_addr().unwrap().port())
            .unwrap()
            .contains(&std::process::id())
    );
    let observed = process(std::process::id(), 100.0, 4096, &socket_inodes()).unwrap();
    assert!(observed.established_sockets.unwrap() >= 2);
    assert!(observed.open_descriptors.unwrap() >= 3);
    assert!(observed.descriptor_limits["soft"].as_str().is_some());
    drop((client, server, duplicate));
}
