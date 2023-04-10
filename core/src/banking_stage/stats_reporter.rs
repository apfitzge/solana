use {
    solana_poh::leader_bank_notifier::LeaderBankNotifier,
    std::{
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc,
        },
        thread::JoinHandle,
        time::Duration,
    },
};

#[derive(Clone, Default)]
pub struct Stats {
    pub slot_stats: Arc<SlotStats>,
    pub time_stats: Arc<TimeStats>,
}

#[derive(Default)]

pub struct SlotStats {
    pub scheduler_stats: SchedulerSlotStats,
    pub worker_stats: WorkerSlotStats,
}

#[derive(Default)]
pub struct SchedulerSlotStats {}

impl SchedulerSlotStats {
    fn report(&self, slot: u64) {
        datapoint_info!("banking_stage-scheduler_slot_stats", ("slot", slot, i64));
    }
}

#[derive(Default)]
pub struct WorkerSlotStats {
    pub num_transactions: AtomicU64,
    pub num_executed_transactions: AtomicU64,
    pub num_retryable_transactions: AtomicU64,
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
pub struct TimeStats {
    pub scheduler_stats: SchedulerTimeStats,
    pub worker_stats: WorkerTimeStats,
}

#[derive(Default)]
pub struct SchedulerTimeStats {}

impl SchedulerTimeStats {
    fn report(&self) {
        // datapoint_info!("banking_stage-scheduler_time_stats");
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
        let slot_thread_hdl =
            Self::start_slot_thread(stats.slot_stats.clone(), exit.clone(), leader_bank_notifier);
        let time_thread_hdl = Self::start_time_thread(stats.time_stats.clone(), exit);
        (
            Self {
                slot_thread_hdl,
                time_thread_hdl,
            },
            stats,
        )
    }

    fn start_slot_thread(
        stats: Arc<SlotStats>,
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
                        stats.scheduler_stats.report(slot);
                        stats.worker_stats.report(slot);
                    }
                }
            })
            .unwrap()
    }

    fn start_time_thread(stats: Arc<TimeStats>, exit: Arc<AtomicBool>) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name("solBanknTimeSts".to_string())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    stats.scheduler_stats.report();
                    stats.worker_stats.report();
                }
            })
            .unwrap()
    }
}
