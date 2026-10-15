use std::{
    env,
    path::Path,
    process::ExitCode,
    sync::{Arc, atomic::Ordering},
};
use twelve_dollar_benchmark::control;

mod config;
mod connections;
mod destinations;
mod errors;
mod load;
mod report;
mod runner;
mod samples;
mod scoring;
mod transport;
mod workload;

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.as_slice() == ["--version"] || args.as_slice() == ["-V"] {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    if args.first().is_some_and(|a| {
        [
            "--supervise",
            "--stop-supervised",
            "--ready-supervised",
            "--start-supervised",
        ]
        .contains(&a.as_str())
    }) {
        let result = if let Some(p) = args.get(1) {
            match args.first().map(String::as_str) {
                Some("--supervise") => {
                    runner::supervise(Path::new(p), args.get(2..).unwrap_or_default())
                }
                Some("--stop-supervised") => control::stop_supervised(Path::new(p)),
                Some("--ready-supervised") => control::ready_supervised(Path::new(p)),
                Some("--start-supervised") => args.get(2).map_or_else(
                    || Err("missing coordinated start timestamp".into()),
                    |t| control::start_supervised(Path::new(p), t),
                ),
                _ => Err("unknown supervisor command".into()),
            }
        } else {
            Err("missing supervisor control path".into())
        };
        return match result {
            Ok(code) => ExitCode::from(code),
            Err(e) => {
                eprintln!("benchmark supervisor error: {e}");
                ExitCode::from(2)
            }
        };
    }
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("benchmark runtime error: {e}");
            return ExitCode::from(2);
        }
    };
    rt.block_on(async {
        if args.first().is_some_and(|a| a == "--orchestrate") {
            let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let flag = cancelled.clone();
            let signals = (
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()),
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()),
            );
            let (mut interrupt, mut terminate) = match signals {
                (Ok(interrupt), Ok(terminate)) => (interrupt, terminate),
                (Err(error), _) | (_, Err(error)) => {
                    eprintln!("benchmark signal handler error: {error}");
                    return ExitCode::from(2);
                }
            };
            tokio::spawn(async move {
                tokio::select! {
                    _ = interrupt.recv() => {},
                    _ = terminate.recv() => {},
                }
                flag.store(true, Ordering::Relaxed);
            });
            return match runner::run(args.get(1..).unwrap_or_default(), &cancelled) {
                Ok(c) => ExitCode::from(c),
                Err(e) => {
                    eprintln!("benchmark runner error: {e}");
                    ExitCode::from(2)
                }
            };
        }
        match load::run(&args).await {
            Ok(true) => ExitCode::SUCCESS,
            Ok(false) => ExitCode::from(1),
            Err(e) => {
                eprintln!("load test error: {e}");
                ExitCode::from(2)
            }
        }
    })
}
