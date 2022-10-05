use {
    clap::{crate_name, Parser},
    cli::Config,
    log::*,
};

mod accounts;
mod cli;

fn main() {
    solana_logger::setup_with_default("solana=info");
    solana_metrics::set_panic_hook("contentious-bench-tps", /*version:*/ None);

    let config = Config::parse();
    info!("running {} with configuration: {:?}", crate_name!(), config);

    let Config {
        entrypoint,
        num_contentious_transfer_accounts,
        num_regular_transfer_accounts,
    } = config;

    info!("generating accounts...");
    let accounts = accounts::Accounts::new(
        num_contentious_transfer_accounts,
        num_regular_transfer_accounts,
    );
}
