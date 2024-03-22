//! this service asynchronously reports CostTracker stats

use {
    crossbeam_channel::Receiver,
    solana_ledger::blockstore::Blockstore,
    solana_runtime::bank::Bank,
    std::{
        sync::Arc,
        thread::{self, Builder, JoinHandle},
    },
};
pub enum CostUpdate {
    FrozenBank { bank: Arc<Bank> },
}

pub type CostUpdateReceiver = Receiver<CostUpdate>;

pub struct CostUpdateService {
    thread_hdl: JoinHandle<()>,
}

impl CostUpdateService {
    pub fn new(blockstore: Arc<Blockstore>, cost_update_receiver: CostUpdateReceiver) -> Self {
        let thread_hdl = Builder::new()
            .name("solCostUpdtSvc".to_string())
            .spawn(move || {
                Self::service_loop(blockstore, cost_update_receiver);
            })
            .unwrap();

        Self { thread_hdl }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }

    fn service_loop(_blockstore: Arc<Blockstore>, cost_update_receiver: CostUpdateReceiver) {
        for cost_update in cost_update_receiver.iter() {
            match cost_update {
                CostUpdate::FrozenBank { bank } => {
                    let mut loop_count = 0;
                    loop {
                        loop_count += 1;
                        let cost_tracker = bank.read_cost_tracker().unwrap();
                        if cost_tracker.in_flight_transaction_count() == 0 {
                            break;
                        }
                        if loop_count >= 100 {
                            warn!("in_flight_transaction_count ({}) did not reach 0 before reaching timeout for slot {}",
                                cost_tracker.in_flight_transaction_count(), bank.slot());
                            break;
                        }

                        std::thread::sleep(std::time::Duration::from_millis(5));
                        continue;
                    }

                    let cost_tracker = bank.read_cost_tracker().unwrap();
                    cost_tracker.report_stats(bank.slot());
                }
            }
        }
    }
}
