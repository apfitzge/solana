use {log::Level, std::time::Duration};
pub use {metrics_reporter_derive::ReportMetrics, solana_metrics::datapoint};

pub trait ReportMetrics {
    /// Report the metrics on an interval - and reset on reporting (use default for ALL fields except the Instant)
    fn report(&mut self);
}

#[derive(ReportMetrics)]
#[report_log_level(Level::Info)]
#[report_name("bank_new_from_fields")]
#[report_interval(Duration::from_millis(1000))]
struct Reporter {
    #[last_report_time]
    last_report_time: std::time::Instant,
    field1: u64,
    field2: f64,
    field4: std::sync::atomic::AtomicBool,
}

impl Reporter {
    fn report_test(&mut self) {
        self.report();
    }
}
