use {
    clap::Parser,
    std::{net::SocketAddr, ops::RangeInclusive, time::Duration},
};

/// Arguments for the contentious-bench-tps program.
#[derive(Debug, Parser)]
#[clap(author, version, about)]
pub struct Config {
    /// Entrypoint to the cluster
    #[clap(short, long, value_parser)]
    pub entrypoint: SocketAddr,

    /// Duration of the benchmark in seconds
    #[clap(short, long, value_parser = parse_duration_from_secs, default_value = "10")]
    pub duration: Duration,

    /// Number of threads to use for contentious transfers
    #[clap(short, long, value_parser, default_value_t = 1)]
    pub num_contentious_transfer_threads: usize,

    /// Number of contentious accounts for transfers
    #[clap(short, long, value_parser, default_value_t = 10)]
    pub num_contentious_transfer_accounts: usize,

    /// Range of additional fees for contentious transfers in microlamports
    #[clap(short, long, value_parser=parse_range_inclusive)]
    pub contentious_transfer_fee_range: Option<RangeInclusive<u64>>,

    /// Number of threads to use for regular transfers
    #[clap(short, long, value_parser, default_value_t = 4)]
    pub num_regular_transfer_threads: usize,

    /// Number of non-contentious accounts for transfers
    #[clap(short, long, value_parser, default_value_t = 10000)]
    pub num_regular_transfer_accounts: usize,

    /// Range of additional fees for regular transfers in microlamports
    #[clap(short, long, value_parser=parse_range_inclusive)]
    pub regular_transfer_fee_range: Option<RangeInclusive<u64>>,
}

/// Parse a duration in seconds from a string
fn parse_duration_from_secs(src: &str) -> Result<Duration, String> {
    src.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|err| err.to_string())
}

/// Parse a range of u64s from a string
fn parse_range_inclusive(src: &str) -> Result<RangeInclusive<u64>, String> {
    let mut parts = src.splitn(2, "-");
    let start = parts
        .next()
        .unwrap()
        .parse::<u64>()
        .map_err(|err| err.to_string())?;
    let end = parts
        .next()
        .unwrap()
        .parse::<u64>()
        .map_err(|err| err.to_string())?;
    Ok(start..=end)
}
