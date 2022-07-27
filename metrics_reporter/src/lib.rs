pub use {solana_metrics::datapoint_info, solana_metrics_reporter_derive::ReportMetrics};

pub trait ReportMetrics {
    /// Reports metrics using datapoint_info!
    fn report(&self);
}
