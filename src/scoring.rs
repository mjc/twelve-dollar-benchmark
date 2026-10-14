use crate::{config::Config, runner};
use chrono::Utc;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Stage {
    Warmup,
    Search,
    Confirm,
}

impl Stage {
    fn name(self) -> &'static str {
        match self {
            Self::Warmup => "warmup",
            Self::Search => "search",
            Self::Confirm => "confirm",
        }
    }
    fn duration(self) -> &'static str {
        match self {
            Self::Warmup => "2m",
            Self::Search | Self::Confirm => "5m",
        }
    }
}

fn find_score(mut trial: impl FnMut(Stage, usize) -> Result<bool>) -> Result<usize> {
    // Warm-up thresholds do not contribute to the score.
    trial(Stage::Warmup, 1000)?;
    let mut low = 0;
    let mut high = 2500;
    while trial(Stage::Search, high)? {
        low = high;
        high = high
            .checked_mul(2)
            .ok_or("user count overflow during limit search")?;
    }
    while high - low > 250 {
        let middle = (low + (high - low) / 2) / 250 * 250;
        if trial(Stage::Search, middle)? {
            low = middle;
        } else {
            high = middle;
        }
    }
    while low > 0 {
        if trial(Stage::Confirm, low)? {
            return Ok(low);
        }
        low -= 250;
    }
    Ok(0)
}

fn trial_args(args: &[String], users: usize, stage: Stage, output: &Path) -> Vec<String> {
    let mut result: Vec<_> = args
        .iter()
        .filter(|arg| *arg != "--score")
        .cloned()
        .collect();
    result.extend([
        "--users".into(),
        users.to_string(),
        "--duration".into(),
        stage.duration().into(),
        "--output-dir".into(),
        output.to_string_lossy().into_owned(),
    ]);
    result
}

fn save_state(root: &Path, state: &Value) -> Result<()> {
    let temporary = root.join("workflow.tmp");
    fs::write(
        &temporary,
        format!("{}\n", serde_json::to_string_pretty(state)?),
    )?;
    fs::rename(temporary, root.join("workflow.json"))?;
    Ok(())
}

pub(super) fn run(cfg: &Config, args: &[String], cancelled: Arc<AtomicBool>) -> Result<u8> {
    let root = cfg.output.clone().unwrap_or_else(|| {
        PathBuf::from(format!(
            "results/score-{}-{}",
            Utc::now().timestamp_millis(),
            std::process::id()
        ))
    });
    fs::create_dir_all(
        root.parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?;
    fs::create_dir(&root)?;
    let root = fs::canonicalize(root)?;
    fs::create_dir(root.join("trials"))?;
    println!("Scoring results: {}", root.display());
    let mut state = json!({"completed": false, "score": null, "started_at": Utc::now(),
        "protocol": {"warmup_users": 1000, "warmup_hold_s": 120, "initial_search_users": 2500,
            "search_hold_s": 300, "confirmation_hold_s": 300, "resolution_users": 250,
            "ramp_up": cfg.ramp_up, "ramp_down": "30s", "warmup_scored": false}, "trials": []});
    save_state(&root, &state)?;
    let mut confirmed = None;
    let mut last = None;
    let score = find_score(|stage, users| {
        if cancelled.load(Ordering::Relaxed) {
            return Err("scoring interrupted".into());
        }
        let index = state["trials"].as_array().unwrap().len() + 1;
        let directory = root
            .join("trials")
            .join(format!("{index:02}-{}-{users}", stage.name()));
        println!(
            "\n{}: {users} VUs, {} hold{}",
            stage.name(),
            stage.duration(),
            if stage == Stage::Warmup {
                " (not scored)"
            } else {
                ""
            }
        );
        state["current"] = json!({"stage": stage, "users": users, "directory": directory});
        save_state(&root, &state)?;
        let status = runner::run(
            &trial_args(args, users, stage, &directory),
            cancelled.clone(),
        )?;
        let report: Value = serde_json::from_slice(&fs::read(directory.join("aggregate.json"))?)?;
        let passed = report["passed"] == true;
        state["trials"].as_array_mut().unwrap().push(json!({"stage": stage, "users": users,
            "scored": stage != Stage::Warmup, "passed": passed, "completed": report["completed"],
            "p95_ms": report["aggregate"]["p95_ms"], "p99_ms": report["aggregate"]["p99_ms"],
            "failure_rate": report["aggregate"]["failure_rate"], "report": directory.join("aggregate.json")}));
        save_state(&root, &state)?;
        if status == 2
            || cancelled.load(Ordering::Relaxed)
            || report["completed"] != true
            || report["runtime_errors"].as_u64() != Some(0)
        {
            return Err(format!(
                "{} at {users} VUs did not complete a valid load run; inspect {}",
                stage.name(),
                directory.display()
            )
            .into());
        }
        if (cfg.monitor_user.is_some() || cfg.user.is_some())
            && report["target_telemetry"]["unavailable_reason"].is_string()
        {
            return Err(format!(
                "target telemetry is unavailable; inspect {} before continuing the score search",
                directory.display()
            )
            .into());
        }
        if report["connections"]["errors"]
            .as_object()
            .is_some_and(|errors| {
                errors.keys().any(|error| {
                    error.contains("AddrNotAvailable")
                        || error.contains("tcp bind source port error")
                })
            })
        {
            return Err("generator source-port exhaustion invalidates the limit search; add source IPs or generator hosts".into());
        }
        if stage == Stage::Confirm && passed {
            confirmed = Some((report.clone(), directory.clone()));
        }
        last = Some(report);
        Ok(passed)
    });
    let score = match score {
        Ok(score) => score,
        Err(error) => {
            state["error"] = json!(error.to_string());
            state["finished_at"] = json!(Utc::now());
            save_state(&root, &state)?;
            return Err(error);
        }
    };
    state["completed"] = json!(true);
    state["current"] = Value::Null;
    state["score"] = json!(score);
    state["finished_at"] = json!(Utc::now());
    save_state(&root, &state)?;
    let (mut report, directory) =
        confirmed.unwrap_or_else(|| (last.unwrap_or_default(), root.clone()));
    report["score"] = json!(score);
    report["passed"] = json!(score > 0);
    report["scoring_protocol"] = state;
    report["confirmation_directory"] = json!(directory);
    let output = format!("{}\n", serde_json::to_string_pretty(&report)?);
    fs::write(root.join("aggregate.json"), &output)?;
    fs::write(
        root.join("report.md"),
        runner::render_report(&report, false),
    )?;
    if let Ok(summary) = std::env::var("SUMMARY")
        && !summary.is_empty()
    {
        let path = Path::new(&summary);
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, &output)?;
    }
    print!("{}", runner::render_report(&report, true));
    println!("Full score report: {}", root.join("report.md").display());
    Ok(if score > 0 { 0 } else { 1 })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_excludes_warmup_narrows_and_steps_down_on_confirmation_failure() {
        let mut calls = Vec::new();
        let score = find_score(|stage, users| {
            calls.push((stage, users));
            Ok(match stage {
                Stage::Warmup => false,
                Stage::Search => users <= 7625,
                Stage::Confirm => users <= 7250,
            })
        })
        .unwrap();
        assert!(calls[0] == (Stage::Warmup, 1000));
        assert!(
            calls[1..4]
                == [
                    (Stage::Search, 2500),
                    (Stage::Search, 5000),
                    (Stage::Search, 10000)
                ]
        );
        assert_eq!(score, 7250);
        assert!(calls.ends_with(&[(Stage::Confirm, 7500), (Stage::Confirm, 7250)]));
    }

    #[test]
    fn failures_at_the_smallest_candidate_produce_zero_without_false_confirmation() {
        let score = find_score(|stage, _| Ok(stage == Stage::Warmup)).unwrap();
        assert_eq!(score, 0);
        assert!(find_score(|_, _| Err("control failure".into())).is_err());
    }

    #[test]
    fn trial_overrides_workload_and_disable_recursive_scoring() {
        let args = ["--score", "--users", "9", "--duration", "3m"].map(String::from);
        let args = trial_args(&args, 2500, Stage::Search, Path::new("/trial"));
        let config = crate::config::parse(&args).unwrap();
        assert!(!config.score);
        assert_eq!(config.users, 2500);
        assert_eq!(config.duration, "5m");
        assert_eq!(config.output.as_deref(), Some(Path::new("/trial")));
    }
}
