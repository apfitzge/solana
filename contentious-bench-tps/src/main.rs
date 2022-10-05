use {clap::Parser, cli::Config, log::info};

mod cli;

fn main() {
    solana_logger::setup_with_default("solana=info");
    solana_metrics::set_panic_hook("contentious-bench-tps", /*version:*/ None);

    let config = Config::parse();
    info!("config: {config:?}");
}
