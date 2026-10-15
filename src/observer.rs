use std::{env, path::Path, process::ExitCode};
use twelve_dollar_benchmark::{
    control, parse_destination_ports, parse_pids, parse_sample_interval, setting, telemetry,
};

fn observe_target(control: &Path) -> Result<u8, Box<dyn std::error::Error>> {
    let pids = |name| parse_pids(&setting(name, ""));
    let interval = parse_sample_interval(&setting("BENCH_SAMPLE_INTERVAL", "1"))
        .map_err(|_| "invalid sample interval")?;
    let ports = parse_destination_ports(&setting(
        "BENCH_TARGET_PORTS",
        &setting("BENCH_TARGET_PORT", "3000"),
    ))?;
    telemetry::observe(
        control,
        pids("BENCH_APPLICATION_PIDS")?,
        pids("BENCH_PROXY_PIDS")?,
        &ports,
        interval,
    )
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    if args
        .iter()
        .any(|arg| matches!(arg.as_str(), "--version" | "-V"))
    {
        println!("twelve-dollar-observer {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    if args
        .iter()
        .any(|arg| matches!(arg.as_str(), "--help" | "-h"))
    {
        println!(
            "Usage: twelve-dollar-observer --observe CONTROL\nSample target process, network, and service resources."
        );
        return ExitCode::SUCCESS;
    }
    let result = (|| {
        let action = args.first().ok_or("missing observer action")?;
        let path = Path::new(args.get(1).ok_or("missing observer control path")?);
        match action.as_str() {
            "--observe" => observe_target(path),
            "--refresh-observer" => control::refresh_observer(path),
            "--ready-supervised" => control::ready_supervised(path),
            "--stop-supervised" => control::stop_supervised(path),
            "--start-supervised" => {
                control::start_supervised(path, args.get(2).ok_or("missing start timestamp")?)
            }
            _ => Err(format!("unknown observer action: {action}").into()),
        }
    })();
    match result {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("metrics observer error: {error}");
            ExitCode::from(2)
        }
    }
}
