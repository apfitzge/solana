use {
    solana_poh::leader_bank_notifier::LeaderBankNotifier,
    solana_runtime::transaction_error_metrics::TransactionErrorMetrics,
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
    transaction_error_counts: TransactionErrorCounts,
}

#[derive(Debug, Default)]
struct TransactionErrorCounts {
    total: AtomicUsize,
    account_in_use: AtomicUsize,
    too_many_account_locks: AtomicUsize,
    account_loaded_twice: AtomicUsize,
    account_not_found: AtomicUsize,
    blockhash_not_found: AtomicUsize,
    blockhash_too_old: AtomicUsize,
    call_chain_too_deep: AtomicUsize,
    already_processed: AtomicUsize,
    instruction_error: AtomicUsize,
    insufficient_funds: AtomicUsize,
    invalid_account_for_fee: AtomicUsize,
    invalid_account_index: AtomicUsize,
    invalid_program_for_execution: AtomicUsize,
    not_allowed_during_cluster_maintenance: AtomicUsize,
    invalid_writable_account: AtomicUsize,
    invalid_rent_paying_account: AtomicUsize,
    would_exceed_max_block_cost_limit: AtomicUsize,
    would_exceed_max_account_cost_limit: AtomicUsize,
    would_exceed_max_vote_cost_limit: AtomicUsize,
    would_exceed_account_data_block_limit: AtomicUsize,
    max_loaded_accounts_data_size_exceeded: AtomicUsize,
}

impl TransactionErrorCounts {
    fn report(&self, slot: u64) {
        let total = self.total.swap(0, Ordering::Relaxed);
        if total == 0 {
            return;
        }

        datapoint_info!(
            "banking_stage-transaction_error_counts",
            ("slot", slot, i64),
            ("total", total, i64),
            (
                "account_in_use",
                self.account_in_use.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "too_many_account_locks",
                self.too_many_account_locks.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "account_loaded_twice",
                self.account_loaded_twice.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "account_not_found",
                self.account_not_found.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "blockhash_not_found",
                self.blockhash_not_found.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "blockhash_too_old",
                self.blockhash_too_old.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "call_chain_too_deep",
                self.call_chain_too_deep.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "already_processed",
                self.already_processed.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "instruction_error",
                self.instruction_error.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "insufficient_funds",
                self.insufficient_funds.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "invalid_account_for_fee",
                self.invalid_account_for_fee.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "invalid_account_index",
                self.invalid_account_index.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "invalid_program_for_execution",
                self.invalid_program_for_execution
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "not_allowed_during_cluster_maintenance",
                self.not_allowed_during_cluster_maintenance
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "invalid_writable_account",
                self.invalid_writable_account.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "invalid_rent_paying_account",
                self.invalid_rent_paying_account.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "would_exceed_max_block_cost_limit",
                self.would_exceed_max_block_cost_limit
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "would_exceed_max_account_cost_limit",
                self.would_exceed_max_account_cost_limit
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "would_exceed_max_vote_cost_limit",
                self.would_exceed_max_vote_cost_limit
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "would_exceed_account_data_block_limit",
                self.would_exceed_account_data_block_limit
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "max_loaded_accounts_data_size_exceeded",
                self.max_loaded_accounts_data_size_exceeded
                    .swap(0, Ordering::Relaxed),
                i64
            ),
        );
    }
}

impl WorkerSlotStats {
    pub fn accumulate_error_counts(&self, error_counters: TransactionErrorMetrics) {
        let TransactionErrorMetrics {
            total,
            account_in_use,
            too_many_account_locks,
            account_loaded_twice,
            account_not_found,
            blockhash_not_found,
            blockhash_too_old,
            call_chain_too_deep,
            already_processed,
            instruction_error,
            insufficient_funds,
            invalid_account_for_fee,
            invalid_account_index,
            invalid_program_for_execution,
            not_allowed_during_cluster_maintenance,
            invalid_writable_account,
            invalid_rent_paying_account,
            would_exceed_max_block_cost_limit,
            would_exceed_max_account_cost_limit,
            would_exceed_max_vote_cost_limit,
            would_exceed_account_data_block_limit,
            max_loaded_accounts_data_size_exceeded,
        } = error_counters;

        self.transaction_error_counts
            .total
            .fetch_add(total, Ordering::Relaxed);
        self.transaction_error_counts
            .account_in_use
            .fetch_add(account_in_use, Ordering::Relaxed);
        self.transaction_error_counts
            .too_many_account_locks
            .fetch_add(too_many_account_locks, Ordering::Relaxed);
        self.transaction_error_counts
            .account_loaded_twice
            .fetch_add(account_loaded_twice, Ordering::Relaxed);
        self.transaction_error_counts
            .account_not_found
            .fetch_add(account_not_found, Ordering::Relaxed);
        self.transaction_error_counts
            .blockhash_not_found
            .fetch_add(blockhash_not_found, Ordering::Relaxed);
        self.transaction_error_counts
            .blockhash_too_old
            .fetch_add(blockhash_too_old, Ordering::Relaxed);
        self.transaction_error_counts
            .call_chain_too_deep
            .fetch_add(call_chain_too_deep, Ordering::Relaxed);
        self.transaction_error_counts
            .already_processed
            .fetch_add(already_processed, Ordering::Relaxed);
        self.transaction_error_counts
            .instruction_error
            .fetch_add(instruction_error, Ordering::Relaxed);
        self.transaction_error_counts
            .insufficient_funds
            .fetch_add(insufficient_funds, Ordering::Relaxed);
        self.transaction_error_counts
            .invalid_account_for_fee
            .fetch_add(invalid_account_for_fee, Ordering::Relaxed);
        self.transaction_error_counts
            .invalid_account_index
            .fetch_add(invalid_account_index, Ordering::Relaxed);
        self.transaction_error_counts
            .invalid_program_for_execution
            .fetch_add(invalid_program_for_execution, Ordering::Relaxed);
        self.transaction_error_counts
            .not_allowed_during_cluster_maintenance
            .fetch_add(not_allowed_during_cluster_maintenance, Ordering::Relaxed);
        self.transaction_error_counts
            .invalid_writable_account
            .fetch_add(invalid_writable_account, Ordering::Relaxed);
        self.transaction_error_counts
            .invalid_rent_paying_account
            .fetch_add(invalid_rent_paying_account, Ordering::Relaxed);
        self.transaction_error_counts
            .would_exceed_max_block_cost_limit
            .fetch_add(would_exceed_max_block_cost_limit, Ordering::Relaxed);
        self.transaction_error_counts
            .would_exceed_max_account_cost_limit
            .fetch_add(would_exceed_max_account_cost_limit, Ordering::Relaxed);
        self.transaction_error_counts
            .would_exceed_max_vote_cost_limit
            .fetch_add(would_exceed_max_vote_cost_limit, Ordering::Relaxed);
        self.transaction_error_counts
            .would_exceed_account_data_block_limit
            .fetch_add(would_exceed_account_data_block_limit, Ordering::Relaxed);
        self.transaction_error_counts
            .max_loaded_accounts_data_size_exceeded
            .fetch_add(max_loaded_accounts_data_size_exceeded, Ordering::Relaxed);
    }

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

        self.transaction_error_counts.report(slot);
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
