mod bench;
mod ci;
mod cli;
mod code;
mod exec;
mod feed;
mod fetch;
mod fsverbs;
mod gate;
mod git;
mod hot;
mod locks;
mod log;
mod loop_run;
mod mcp;
mod mcp_client;
mod notices;
mod relay;
mod resources;
mod retry;
mod review;
mod rows;
mod run;
mod runcode;
mod search;
mod skills;
mod types;
mod update;
mod verbs;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match args.first().map(String::as_str) {
        Some("serve") => {
            let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("runtime");
            rt.block_on(relay::serve(args[1..].to_vec()))
        }
        Some("worker") => {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");
            match rt.block_on(mcp::serve_stdio(types::WorkerOpts::parse(&args[1..]))) {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("gmnew worker: {e}");
                    1
                }
            }
        }
        _ => cli::main(args),
    };
    std::process::exit(code);
}
