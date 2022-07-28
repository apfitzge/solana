pub use {
    solana_metrics::datapoint_info,
    solana_metrics_reporter_derive::{ReportMetrics, ReportMetricsInterval},
};

pub trait ReportMetrics {
    /// Reports metrics using datapoint_info!
    fn report(&self);
}

pub trait ReportMetricsInterval {
    /// Reports metrics using datapoint_info! and resets fields
    fn report(&mut self);
}
