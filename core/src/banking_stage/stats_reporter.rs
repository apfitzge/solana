use {
    solana_poh::leader_bank_notifier::LeaderBankNotifier,
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
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
pub struct WorkerSlotStats {}

impl WorkerSlotStats {
    fn report(&self, slot: u64) {
        datapoint_info!("banking_stage-worker_slot_stats", ("slot", slot, i64));
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
        datapoint_info!("banking_stage-scheduler_time_stats");
    }
}

#[derive(Default)]
pub struct WorkerTimeStats {}

impl WorkerTimeStats {
    fn report(&self) {
        datapoint_info!("banking_stage-worker_time_stats");
    }
}

pub struct StatsReporter {
    slot_thread_hdl: JoinHandle<()>,
    time_thread_hdl: JoinHandle<()>,
}

impl StatsReporter {
    pub fn new(exit: Arc<AtomicBool>, leader_bank_notifier: LeaderBankNotifier) -> (Self, Stats) {
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

    pub fn join(self) -> std::thread::Result<()> {
        self.slot_thread_hdl.join()?;
        self.time_thread_hdl.join()
    }

    fn start_slot_thread(
        stats: Arc<SlotStats>,
        exit: Arc<AtomicBool>,
        leader_bank_notifier: LeaderBankNotifier,
    ) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name("solBanknSlotSts".to_string())
            .spawn(move || {
                while exit.load(Ordering::Relaxed) {
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
                while exit.load(Ordering::Relaxed) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    stats.scheduler_stats.report();
                    stats.worker_stats.report();
                }
            })
            .unwrap()
    }
}
