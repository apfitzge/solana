use {clap::Parser, std::net::SocketAddr};

/// Arguments for the contentious-bench-tps program.
#[derive(Debug, Parser)]
#[clap(author, version, about)]
pub struct Config {
    /// Entrypoint to the cluster
    #[clap(short, long, value_parser)]
    pub entrypoint: SocketAddr,
    /// Number of contentious accounts for transfers
    #[clap(short, long, value_parser, default_value_t = 10)]
    pub num_contentious_transfer_accounts: usize,
    /// Number of non-contentious accounts for transfers
    #[clap(short, long, value_parser, default_value_t = 100000)]
    pub num_regular_transfer_accounts: usize,
}
