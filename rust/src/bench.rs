use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

use crate::log;
use crate::loop_run::num_text;

const TRIALS: usize = 3;

struct Arm {
    name: &'static str,
    flags: &'static [&'static str],
}

struct Task {
    name: &'static str,
    task: &'static str,
    replies: &'static str,
    arms: &'static [Arm],
}

const PARALLEL_ARMS: &[Arm] = &[Arm { name: "parallel on", flags: &[] }, Arm { name: "parallel off", flags: &["--serial"] }];

const ALL_ARMS: &[Arm] = &[
    Arm { name: "parallel on", flags: &[] },
    Arm { name: "parallel off", flags: &["--serial"] },
    Arm { name: "background on", flags: &[] },
    Arm { name: "background off", flags: &["--no-background"] },
];

const TASKS: &[Task] = &[
    Task {
        name: "writes",
        task: "Write two files in one reply, read one back, and finish with a witness.",
        replies: r#"[[{"tool":"write_file","args":{"path":"a.txt","content":"alpha\n"}},{"tool":"write_file","args":{"path":"b.txt","content":"beta\n"}}],{"tool":"read_file","args":{"path":"a.txt"}},{"tool":"finish","args":{"witness":"a.txt and b.txt were written in one reply; read_file a.txt returned alpha."}}]"#,
        arms: ALL_ARMS,
    },
    Task {
        name: "background",
        task: "Start a 3 s background job, run a 2 s command while it runs, poll the job, and finish.",
        replies: concat!(
            r#"[{"tool":"run_shell","args":{"command":""#,
            r#"node -e \"setTimeout(() => {}, 3000)\""#,
            r#"","background":true,"job":"slow"}},{"tool":"run_shell","args":{"command":""#,
            r#"node -e \"setTimeout(() => {}, 2000)\""#,
            r#""}},{"tool":"job_status","args":{"job":"slow","waitMs":10000}},{"tool":"finish","args":{"witness":"job slow was polled after a 2 s command; job_status reported its state."}}]"#,
        ),
        arms: ALL_ARMS,
    },
    Task {
        name: "log",
        task: "Write one file, count the result events with query_log, and finish.",
        replies: r#"[{"tool":"write_file","args":{"path":"log.txt","content":"one\n"}},{"tool":"query_log","args":{"js":"events.filter((e) => e.kind === 'result').length"}},{"tool":"finish","args":{"witness":"write_file log.txt returned ok; query_log counted the result events."}}]"#,
        arms: ALL_ARMS,
    },
    Task {
        name: "slow-writes",
        task: "Write four independent files in one reply, then finish with a witness.",
        replies: r#"[[{"tool":"write_file","args":{"path":"a.txt","content":"alpha\n"}},{"tool":"write_file","args":{"path":"b.txt","content":"beta\n"}},{"tool":"write_file","args":{"path":"c.txt","content":"gamma\n"}},{"tool":"write_file","args":{"path":"d.txt","content":"delta\n"}}],{"tool":"finish","args":{"witness":"a.txt, b.txt, c.txt and d.txt were written in one reply."}}]"#,
        arms: PARALLEL_ARMS,
    },
];

struct Options {
    latency_ms: u64,
    only: Option<String>,
}

struct Trial {
    arm: &'static str,
    task: &'static str,
    id: String,
    status: String,
    wall_ms: f64,
    idle_ms: f64,
}

pub fn main(args: &[String]) -> i32 {
    match execute(args) {
        Ok(code) => code,
        Err(message) => {
            eprintln!("gmnew: {message}");
            1
        }
    }
}

fn execute(args: &[String]) -> Result<i32, String> {
    let options = parse_options(args)?;
    let tasks: Vec<&Task> = match &options.only {
        None => TASKS.iter().collect(),
        Some(name) => TASKS.iter().filter(|task| task.name == name).collect(),
    };
    if tasks.is_empty() {
        return Err(format!("no bench task named {}", options.only.as_deref().unwrap_or("")));
    }
    let root = std::env::temp_dir().join(format!("gmnew-bench-{}", log::random_hex(3)));
    fs::create_dir_all(&root).map_err(|e| e.to_string())?;
    let results = run_tasks(&root, &tasks, options.latency_ms);
    let _ = fs::remove_dir_all(&root);
    let results = results?;
    print_table(&results, &tasks, options.latency_ms);
    Ok(if results.iter().all(|r| r.status == "finished") { 0 } else { 1 })
}

fn parse_options(args: &[String]) -> Result<Options, String> {
    let mut options = Options { latency_ms: 0, only: None };
    let mut i = 0;
    while i < args.len() {
        let key = args[i].as_str();
        let Some(value) = args.get(i + 1) else {
            return Err(format!("{key} needs a value"));
        };
        match key {
            "--latency-ms" => {
                if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                    return Err("--latency-ms takes a whole number of milliseconds, 0 or more".to_string());
                }
                options.latency_ms = value.parse::<u64>().map_err(|e| e.to_string())?;
            }
            "--task" => options.only = Some(value.clone()),
            _ => return Err(format!("unknown flag: {key}")),
        }
        i += 2;
    }
    Ok(options)
}

fn run_tasks(root: &Path, tasks: &[&Task], latency_ms: u64) -> Result<Vec<Trial>, String> {
    let mut results = Vec::new();
    for task in tasks {
        let script = root.join(format!("{}.json", task.name));
        fs::write(&script, format!("{}\n", task.replies)).map_err(|e| e.to_string())?;
        for arm in task.arms {
            for trial in 1..=TRIALS {
                let workspace: PathBuf = root.join(format!("ws-{}-{}-{trial}", task.name, arm.name.replace(' ', "-")));
                fs::create_dir(&workspace).map_err(|e| e.to_string())?;
                let run = run_once(task, &script, &workspace, arm.flags, latency_ms)?;
                println!(
                    "{} | {} | trial {trial} | run {} | {} | wall {} ms | idleMs {}",
                    arm.name,
                    task.name,
                    run.id,
                    run.status,
                    num_text(run.wall_ms),
                    num_text(run.idle_ms)
                );
                results.push(Trial { arm: arm.name, task: task.name, ..run });
            }
        }
    }
    Ok(results)
}

fn run_once(task: &Task, script: &Path, workspace: &Path, flags: &[&str], latency_ms: u64) -> Result<Trial, String> {
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let child = Command::new(exe)
        .arg("run")
        .args(["--task", task.task, "--workspace"])
        .arg(workspace)
        .args(["--model", "scripted", "--script"])
        .arg(script)
        .args(["--latency-ms", &latency_ms.to_string()])
        .args(flags)
        .output()
        .map_err(|e| e.to_string())?;
    let stdout = String::from_utf8_lossy(&child.stdout).into_owned();
    let info = match parse_run_line(&stdout) {
        Some(found) => found,
        None => {
            return Err(format!("run did not start for {}: {}", task.name, String::from_utf8_lossy(&child.stderr)));
        }
    };
    let log_file = workspace.join("runs").join(&info.0).join(log::LOG_FILE);
    let text = fs::read_to_string(&log_file).map_err(|e| e.to_string())?;
    let events: Vec<Value> = text.lines().filter(|l| !l.trim().is_empty()).map(|l| serde_json::from_str(l).map_err(|e| e.to_string())).collect::<Result<_, _>>()?;
    let start = events.iter().find(|e| e.get("kind").and_then(Value::as_str) == Some("start")).and_then(|e| e.get("mono")).and_then(Value::as_f64).unwrap_or(0.0);
    let last = events.last().and_then(|e| e.get("mono")).and_then(Value::as_f64).unwrap_or(0.0);
    let summary = events.iter().find(|e| e.get("kind").and_then(Value::as_str) == Some("summary"));
    let Some(idle) = summary.and_then(|e| e.get("idleMs")).and_then(Value::as_f64) else {
        return Err(format!("run {} has no summary event", info.0));
    };
    Ok(Trial {
        arm: "",
        task: task.name,
        id: info.0,
        status: info.1,
        wall_ms: round1(last - start),
        idle_ms: idle,
    })
}

fn parse_run_line(stdout: &str) -> Option<(String, String)> {
    for line in stdout.lines() {
        let Some(rest) = line.strip_prefix("run ") else { continue };
        let mut parts = rest.splitn(2, ' ');
        let Some(id) = parts.next().and_then(|token| token.strip_suffix(':')) else { continue };
        let Some(after) = parts.next() else { continue };
        let status = after.split(' ').next().unwrap_or("");
        if !status.is_empty() && after[status.len()..].starts_with(" after") {
            return Some((id.to_string(), status.to_string()));
        }
    }
    None
}

fn round1(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

fn spread(values: &[f64]) -> String {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let mid = sorted.len() / 2;
    let median = if sorted.len() % 2 == 1 { sorted[mid] } else { (sorted[mid - 1] + sorted[mid]) / 2.0 };
    format!("{} ({} to {})", num_text(round1(median)), num_text(round1(sorted[0])), num_text(round1(sorted[sorted.len() - 1])))
}

fn print_table(results: &[Trial], tasks: &[&Task], latency_ms: u64) {
    let label = if latency_ms > 0 { format!("simulated latency {latency_ms} ms") } else { "no simulated latency (0 ms)".to_string() };
    println!();
    println!("{label}: median wall time per arm, with min to max");
    println!("arm              task        trials  status              wall ms: median (min to max)    idleMs: median (min to max)");
    for task in tasks {
        for arm in task.arms {
            let rows: Vec<&Trial> = results.iter().filter(|r| r.arm == arm.name && r.task == task.name).collect();
            let finished = rows.iter().filter(|r| r.status == "finished").count();
            let walls: Vec<f64> = rows.iter().map(|r| r.wall_ms).collect();
            let idles: Vec<f64> = rows.iter().map(|r| r.idle_ms).collect();
            println!(
                "{:<16} {:<11} {:<7} {:<19} {:<31} {}",
                arm.name,
                task.name,
                rows.len(),
                format!("{finished}/{} finished", rows.len()),
                spread(&walls),
                spread(&idles)
            );
        }
    }
}
