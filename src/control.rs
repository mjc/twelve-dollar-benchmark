use chrono::Utc;
use std::{
    error::Error,
    fs,
    path::Path,
    thread,
    time::{Duration, Instant},
};
pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// Wait for the coordinator's start timestamp and report readiness.
///
/// # Errors
///
/// Returns an error if the start is cancelled, times out, is missed, or cannot be read.
pub fn wait_start(control: &Path) -> Result<i64> {
    fs::write(control.with_extension("ready"), "ready\n")?;
    let deadline = Instant::now() + Duration::from_secs(120);
    while !control.with_extension("start").exists() {
        if control.exists() || Instant::now() >= deadline {
            fs::write(control.with_extension("done"), "not started\n")?;
            return Err("coordinated start cancelled or timed out".into());
        }
        thread::sleep(Duration::from_millis(100));
    }
    let start_at: i64 = fs::read_to_string(control.with_extension("start"))?
        .trim()
        .parse()?;
    if Utc::now()
        .timestamp_millis()
        .checked_sub(start_at)
        .is_none_or(|elapsed| elapsed > 250)
    {
        fs::write(control.with_extension("done"), "missed start\n")?;
        return Err(
            "missed coordinated start; check SSH delay and host clock synchronization".into(),
        );
    }
    Ok(start_at)
}

/// Wait until a supervised generator has written its readiness marker.
///
/// # Errors
///
/// Returns an error if the marker is not written before the timeout.
pub fn ready_supervised(control: &Path) -> Result<u8> {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !control.with_extension("ready").exists() {
        if Instant::now() >= deadline {
            return Err("remote generator did not become ready".into());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Ok(0)
}

/// Publish a validated timestamp to start a supervised generator.
///
/// # Errors
///
/// Returns an error if the timestamp is invalid or the control files cannot be updated.
pub fn start_supervised(control: &Path, timestamp: &str) -> Result<u8> {
    let _: i64 = timestamp.parse()?;
    let temporary = control.with_extension("start.tmp");
    fs::write(&temporary, timestamp)?;
    refresh_observer(control)?;
    fs::rename(temporary, control.with_extension("start"))?;
    Ok(0)
}

/// Refresh the observer lease marker.
///
/// # Errors
///
/// Returns an error if the observer has stopped or the lease marker cannot be written.
pub fn refresh_observer(control: &Path) -> Result<u8> {
    if control.exists() || control.with_extension("done").exists() {
        return Err("cannot refresh an observer that has already stopped".into());
    }
    fs::write(control.with_extension("lease"), "alive\n")?;
    Ok(0)
}

/// Cancel a supervised generator and wait for its completion marker.
///
/// # Errors
///
/// Returns an error if cancellation cannot be written or shutdown times out.
pub fn stop_supervised(control: &Path) -> Result<u8> {
    fs::write(control, "cancel\n")?;
    let deadline = Instant::now() + Duration::from_secs(15);
    while !control.with_extension("done").exists() {
        if Instant::now() >= deadline {
            return Err("remote supervisor did not confirm shutdown".into());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture() -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "twelve-dollar-benchmark-control-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::create_dir(&dir).unwrap();
        (dir.clone(), dir.join("metrics"))
    }

    #[test]
    fn observer_lease_refresh_is_rejected_after_shutdown_or_cancel() {
        let (dir, control) = fixture();
        fs::write(control.with_extension("done"), "stopped\n").unwrap();
        assert!(refresh_observer(&control).is_err());
        assert!(!control.with_extension("lease").exists());
        fs::remove_file(control.with_extension("done")).unwrap();
        fs::write(&control, "cancel\n").unwrap();
        assert!(refresh_observer(&control).is_err());
        assert!(!control.with_extension("lease").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn observer_lease_refresh_writes_while_running() {
        let (dir, control) = fixture();
        assert_eq!(refresh_observer(&control).unwrap(), 0);
        assert_eq!(
            fs::read_to_string(control.with_extension("lease")).unwrap(),
            "alive\n"
        );
        fs::remove_dir_all(dir).unwrap();
    }
}
