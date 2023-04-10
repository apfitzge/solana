use {
    solana_poh::leader_bank_notifier::LeaderBankNotifier,
    std::{
        sync::{
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
            Arc,
        },
        thread::JoinHandle,
        time::Duration,
    },
};

#[derive(Default)]
pub struct Stats {
    pub scheduler_slot_stats: Arc<SchedulerSlotStats>,
    pub worker_slot_stats: Arc<WorkerSlotStats>,

    pub scheduler_time_stats: Arc<SchedulerTimeStats>,
    pub worker_time_stats: Arc<WorkerTimeStats>,
}

#[derive(Default)]
pub struct SchedulerSlotStats {
    pub num_consume_scheduled: AtomicUsize,
}

impl SchedulerSlotStats {
    fn report(&self, slot: u64) {
        datapoint_info!(
            "banking_stage-scheduler_slot_stats",
            ("slot", slot, i64),
            (
                "num_consume_scheduled",
                self.num_consume_scheduled.swap(0, Ordering::Relaxed),
                i64
            )
        );
    }
}

#[derive(Default)]
pub struct WorkerSlotStats {
    pub num_transactions: AtomicUsize,
    pub num_executed_transactions: AtomicUsize,
    pub num_retryable_transactions: AtomicUsize,
}

impl WorkerSlotStats {
    fn report(&self, slot: u64) {
        datapoint_info!(
            "banking_stage-worker_slot_stats",
            ("slot", slot, i64),
            (
                "num_transactions",
                self.num_transactions.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "num_executed_transactions",
                self.num_executed_transactions.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "num_retryable_transactions",
                self.num_retryable_transactions.swap(0, Ordering::Relaxed),
                i64
            ),
        );
    }
}

#[derive(Default)]
pub struct SchedulerTimeStats {
    pub num_packets_received: AtomicUsize,
    pub num_packets_dropped: AtomicUsize,
    pub schedule_consume_time_us: AtomicU64,
}

impl SchedulerTimeStats {
    fn report(&self) {
        datapoint_info!(
            "banking_stage-scheduler_time_stats",
            (
                "num_packets_received",
                self.num_packets_received.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "num_packets_dropped",
                self.num_packets_dropped.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "schedule_consume_time_us",
                self.schedule_consume_time_us.swap(0, Ordering::Relaxed),
                i64
            )
        );
    }
}

#[derive(Default)]
pub struct WorkerTimeStats {
    pub wait_for_bank_time_us: AtomicU64,
}

impl WorkerTimeStats {
    fn report(&self) {
        datapoint_info!(
            "banking_stage-worker_time_stats",
            (
                "wait_for_bank_time_us",
                self.wait_for_bank_time_us.swap(0, Ordering::Relaxed),
                i64
            )
        );
    }
}

pub struct StatsReporter {
    pub slot_thread_hdl: JoinHandle<()>,
    pub time_thread_hdl: JoinHandle<()>,
}

impl StatsReporter {
    pub fn new(
        exit: Arc<AtomicBool>,
        leader_bank_notifier: Arc<LeaderBankNotifier>,
    ) -> (Self, Stats) {
        let stats = Stats::default();
        let Stats {
            scheduler_slot_stats,
            worker_slot_stats,
            scheduler_time_stats,
            worker_time_stats,
        } = &stats;

        let slot_thread_hdl = Self::start_slot_thread(
            scheduler_slot_stats.clone(),
            worker_slot_stats.clone(),
            exit.clone(),
            leader_bank_notifier,
        );
        let time_thread_hdl = Self::start_time_thread(
            scheduler_time_stats.clone(),
            worker_time_stats.clone(),
            exit,
        );
        (
            Self {
                slot_thread_hdl,
                time_thread_hdl,
            },
            stats,
        )
    }

    fn start_slot_thread(
        scheduler_stats: Arc<SchedulerSlotStats>,
        worker_stats: Arc<WorkerSlotStats>,
        exit: Arc<AtomicBool>,
        leader_bank_notifier: Arc<LeaderBankNotifier>,
    ) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name("solBanknSlotSts".to_string())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    if let Some(slot) =
                        leader_bank_notifier.wait_for_completed(Duration::from_millis(500))
                    {
                        scheduler_stats.report(slot);
                        worker_stats.report(slot);
                    }
                }
            })
            .unwrap()
    }

    fn start_time_thread(
        scheduler_stats: Arc<SchedulerTimeStats>,
        worker_stats: Arc<WorkerTimeStats>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name("solBanknTimeSts".to_string())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    scheduler_stats.report();
                    worker_stats.report();
                }
            })
            .unwrap()
    }
}
