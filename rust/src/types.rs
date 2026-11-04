use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;

pub type Fut<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
pub type VerbOutcome = Result<Value, String>;
pub type VerbCall = fn(Arc<Ctx>, Value) -> Fut<VerbOutcome>;

pub struct VerbDef {
    pub name: &'static str,
    pub description: &'static str,
    pub schema: &'static str,
    pub call: VerbCall,
}

pub struct Ctx {
    pub workspace: PathBuf,
    pub run: Arc<crate::run::Run>,
    pub jobs: Arc<crate::exec::Jobs>,
    pub locks: Arc<crate::locks::Locks>,
    pub hub: Arc<crate::mcp_client::Hub>,
}

#[derive(Clone, Debug, Default)]
pub struct WorkerOpts {
    pub workspace: PathBuf,
    pub run_dir: Option<PathBuf>,
    pub task: Option<String>,
    pub max_steps: u64,
    pub max_inflight: usize,
    pub mcp_config: Option<PathBuf>,
}

impl WorkerOpts {
    pub fn parse(args: &[String]) -> WorkerOpts {
        let mut opts = WorkerOpts {
            workspace: PathBuf::from("."),
            max_steps: 100_000,
            max_inflight: 8,
            ..WorkerOpts::default()
        };
        let mut it = args.iter();
        while let Some(flag) = it.next() {
            let value = it.next().cloned();
            match (flag.as_str(), value) {
                ("--workspace", Some(v)) => opts.workspace = PathBuf::from(v),
                ("--run-dir", Some(v)) => opts.run_dir = Some(PathBuf::from(v)),
                ("--task", Some(v)) => opts.task = Some(v),
                ("--max-steps", Some(v)) => opts.max_steps = v.parse().unwrap_or(opts.max_steps),
                ("--max-inflight", Some(v)) => opts.max_inflight = v.parse().unwrap_or(opts.max_inflight),
                ("--mcp-config", Some(v)) => opts.mcp_config = Some(PathBuf::from(v)),
                _ => {}
            }
        }
        opts
    }
}
