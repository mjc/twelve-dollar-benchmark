use super::Result;
use std::{
    env, fs,
    io::Read,
    os::unix::process::CommandExt,
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use twelve_dollar_benchmark::control;

// Quote every argument for the remote POSIX shell, including user-supplied flags.
pub(super) fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub(super) fn ssh(user: &str, ip: &str) -> Command {
    let mut cmd = Command::new("ssh");
    cmd.args([
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
        "-l",
        user,
        "--",
        ip,
    ]);
    cmd
}

// Every remote executable and argument passes through the same POSIX quoting.
pub(super) fn remote_command(user: &str, host: &str, binary: &str, args: &[&str]) -> Command {
    let mut command = ssh(user, host);
    command.arg(
        std::iter::once(binary)
            .chain(args.iter().copied())
            .map(quote)
            .collect::<Vec<_>>()
            .join(" "),
    );
    command
}

pub(super) fn upload(
    user: &str,
    host: &str,
    local: &Path,
    remote: &str,
    cancelled: &AtomicBool,
) -> Result<()> {
    checked(
        scp()
            .arg("--")
            .arg(local)
            .arg(destination(user, host, remote)),
        cancelled,
    )
}

pub(super) fn log_output(command: &mut Command, log: fs::File) -> Result<()> {
    command
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log));
    Ok(())
}

pub(super) fn destination(user: &str, ip: &str, remote_dir: &str) -> String {
    let host = if ip.contains(':') {
        format!("[{ip}]")
    } else {
        ip.into()
    };
    format!("{user}@{host}:{remote_dir}")
}

pub(super) fn scp() -> Command {
    let mut cmd = Command::new("scp");
    cmd.args(["-q", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]);
    cmd
}

pub(super) fn checked(cmd: &mut Command, cancelled: &AtomicBool) -> Result<()> {
    checked_with_timeout(cmd, cancelled, Duration::from_secs(120))
}

pub(super) fn checked_with_timeout(
    cmd: &mut Command,
    cancelled: &AtomicBool,
    timeout: Duration,
) -> Result<()> {
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stderr = child.stderr.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut message = String::new();
        let _ = stderr.read_to_string(&mut message);
        let _ = sender.send(message);
    });
    let status = wait_child(
        &mut child,
        || cancelled.load(Ordering::Relaxed),
        Some(timeout),
    );
    // A proxy descendant may retain stderr after its parent was terminated.
    let message = receiver
        .recv_timeout(Duration::from_secs(1))
        .unwrap_or_default();
    let status = status?;
    if !status.success() {
        return Err(format!("command failed ({}): {}", status, message.trim()).into());
    }
    Ok(())
}

pub(super) fn execute(cmd: &mut Command, cancelled: &AtomicBool) -> Result<ExitStatus> {
    // The coordinator forwards one SIGINT; the terminal sends Ctrl-C to this process group.
    let mut child = cmd.stdin(Stdio::null()).process_group(0).spawn()?;
    wait_child(&mut child, || cancelled.load(Ordering::Relaxed), None)
}

fn interrupt_child(child: &Child) -> Result<()> {
    // SAFETY: this is our live, unreaped child; its PID cannot be reused.
    let result = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

pub(super) fn wait_child(
    child: &mut Child,
    mut cancelled: impl FnMut() -> bool,
    timeout: Option<Duration>,
) -> Result<ExitStatus> {
    let started = Instant::now();
    let mut stopping = None;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if cancelled() {
            if timeout.is_none() && stopping.is_none() {
                interrupt_child(child)?;
                stopping = Some(Instant::now());
            }
            if timeout.is_some()
                || stopping.is_some_and(|at: Instant| at.elapsed() >= Duration::from_secs(10))
            {
                child.kill()?;
                return Ok(child.wait()?);
            }
        }
        if timeout.is_some_and(|limit| started.elapsed() >= limit) {
            child.kill()?;
            child.wait()?;
            return Err("transfer/control command timed out".into());
        }
        thread::sleep(Duration::from_millis(100));
    }
}

pub(crate) fn supervise(control: &Path, args: &[String]) -> Result<u8> {
    let start_at = control::wait_start(control)?;
    let mut child = Command::new(env::current_exe()?)
        .args(args)
        .env("BENCH_START_AT", start_at.to_string())
        .stdin(Stdio::null())
        .spawn()?;
    let mut stopped = false;
    let status = wait_child(
        &mut child,
        || {
            stopped |= control.exists();
            stopped
        },
        None,
    )?;
    fs::write(control.with_extension("done"), format!("{status}\n"))?;
    Ok(u8::try_from(status.code().unwrap_or(2)).unwrap_or(2))
}
