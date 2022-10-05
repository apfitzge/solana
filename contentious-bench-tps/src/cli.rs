use {clap::Parser, std::net::SocketAddr};

/// Arguments for the contentious-bench-tps program.
#[derive(Debug, Parser)]
#[clap(author, version, about)]
pub struct Config {
    /// Entrypoint to the cluster
    #[clap(short, long, value_parser)]
    pub entrypoint: SocketAddr,
}
