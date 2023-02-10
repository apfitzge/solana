//! The `banking_stage` processes Transaction messages. It is intended to be used
//! to construct a software pipeline. The stage uses all available CPU cores and
//! can do its processing in parallel with signature verification on the GPU.

use {
    self::{
        commit_executor::CommitExecutor, consume_executor::ConsumeExecutor,
        decision_maker::DecisionMaker, execution_pipeline::ExecutionPipeline,
        forward_executor::ForwardExecutor, packet_receiver::PacketReceiver,
        record_executor::RecordExecutor, scheduler_handle::SchedulerHandle,
    },
    crate::{
        latest_unprocessed_votes::{LatestUnprocessedVotes, VoteSource},
        leader_slot_banking_stage_metrics::LeaderSlotMetricsTracker,
        leader_slot_banking_stage_timing_metrics::LeaderExecuteAndCommitTimings,
        packet_deserializer::PacketDeserializer,
        qos_service::QosService,
        scheduler_stage::{ProcessedTransactionsSender, ScheduledTransactionsReceiver},
        sigverify::SigverifyTracerPacketStats,
        tracer_packet_stats::TracerPacketStats,
        unprocessed_packet_batches::*,
        unprocessed_transaction_storage::{ThreadType, UnprocessedTransactionStorage},
    },
    crossbeam_channel::{Receiver as CrossbeamReceiver, Sender as CrossbeamSender},
    solana_client::connection_cache::ConnectionCache,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_perf::{data_budget::DataBudget, packet::PacketBatch},
    solana_poh::poh_recorder::{PohRecorder, PohRecorderError},
    solana_runtime::{
        self, bank_forks::BankForks, bank_status::BankStatus,
        transaction_error_metrics::TransactionErrorMetrics, vote_sender_types::ReplayVoteSender,
    },
    solana_sdk::{
        feature_set::allow_votes_to_directly_update_vote_state, pubkey::Pubkey,
        timing::AtomicInterval,
    },
    solana_transaction_status::TransactionTokenBalance,
    std::{
        cmp,
        collections::HashMap,
        env,
        net::UdpSocket,
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Arc, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

pub mod commit_executor;
pub mod consume_executor;
pub mod decision_maker;
pub mod execution_pipeline;
pub mod external_scheduler;
pub mod forward_executor;
pub mod multi_iterator_scheduler;
pub mod packet_receiver;
pub mod record_executor;
pub mod scheduler_error;
pub mod scheduler_handle;
pub mod thread_aware_account_locks;
pub mod thread_local_scheduler;

// Fixed thread size seems to be fastest on GCP setup
pub const NUM_THREADS: u32 = 6;

const TOTAL_BUFFERED_PACKETS: usize = 700_000;

const MAX_NUM_TRANSACTIONS_PER_BATCH: usize = 64;

const NUM_VOTE_PROCESSING_THREADS: u32 = 2;
const MIN_THREADS_BANKING: u32 = 1;
const MIN_TOTAL_THREADS: u32 = NUM_VOTE_PROCESSING_THREADS + MIN_THREADS_BANKING;

const SLOT_BOUNDARY_CHECK_PERIOD: Duration = Duration::from_millis(10);
pub type BankingPacketBatch = (Vec<PacketBatch>, Option<SigverifyTracerPacketStats>);
pub type BankingPacketSender = CrossbeamSender<BankingPacketBatch>;
pub type BankingPacketReceiver = CrossbeamReceiver<BankingPacketBatch>;

pub struct ProcessTransactionBatchOutput {
    // The number of transactions filtered out by the cost model
    cost_model_throttled_transactions_count: usize,
    // Amount of time spent running the cost model
    cost_model_us: u64,
    execute_and_commit_transactions_output: ExecuteAndCommitTransactionsOutput,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitTransactionDetails {
    Committed { compute_units: u64 },
    NotCommitted,
}

pub struct ExecuteAndCommitTransactionsOutput {
    // Total number of transactions that were passed as candidates for execution
    transactions_attempted_execution_count: usize,
    // The number of transactions of that were executed. See description of in `ProcessTransactionsSummary`
    // for possible outcomes of execution.
    executed_transactions_count: usize,
    // Total number of the executed transactions that returned success/not
    // an error.
    executed_with_successful_result_count: usize,
    // Transactions that either were not executed, or were executed and failed to be committed due
    // to the block ending.
    retryable_transaction_indexes: Vec<usize>,
    // A result that indicates whether transactions were successfully
    // committed into the Poh stream.
    commit_transactions_result: Result<Vec<CommitTransactionDetails>, PohRecorderError>,
    execute_and_commit_timings: LeaderExecuteAndCommitTimings,
    error_counters: TransactionErrorMetrics,
}

#[derive(Debug, Default)]
pub struct BankingStageStats {
    last_report: AtomicInterval,
    id: u32,
    rebuffered_packets_count: AtomicUsize,
    consumed_buffered_packets_count: AtomicUsize,
    forwarded_transaction_count: AtomicUsize,
    forwarded_vote_count: AtomicUsize,

    // Timing
    consume_buffered_packets_elapsed: AtomicU64,
    filter_pending_packets_elapsed: AtomicU64,
    pub(crate) packet_conversion_elapsed: AtomicU64,
    transaction_processing_elapsed: AtomicU64,
}

impl BankingStageStats {
    pub fn new(id: u32) -> Self {
        BankingStageStats {
            id,
            ..BankingStageStats::default()
        }
    }

    fn is_empty(&self) -> bool {
        0 == self.rebuffered_packets_count.load(Ordering::Relaxed) as u64
            + self.consumed_buffered_packets_count.load(Ordering::Relaxed) as u64
            + self
                .consume_buffered_packets_elapsed
                .load(Ordering::Relaxed)
            + self.filter_pending_packets_elapsed.load(Ordering::Relaxed)
            + self.packet_conversion_elapsed.load(Ordering::Relaxed)
            + self.transaction_processing_elapsed.load(Ordering::Relaxed)
            + self.forwarded_transaction_count.load(Ordering::Relaxed) as u64
            + self.forwarded_vote_count.load(Ordering::Relaxed) as u64
    }

    fn report(&mut self, report_interval_ms: u64) {
        // skip reporting metrics if stats is empty
        if self.is_empty() {
            return;
        }
        if self.last_report.should_update(report_interval_ms) {
            datapoint_info!(
                "banking_stage-loop-stats",
                ("id", self.id as i64, i64),
                (
                    "rebuffered_packets_count",
                    self.rebuffered_packets_count.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "consumed_buffered_packets_count",
                    self.consumed_buffered_packets_count
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "forwarded_transaction_count",
                    self.forwarded_transaction_count.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "forwarded_vote_count",
                    self.forwarded_vote_count.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "consume_buffered_packets_elapsed",
                    self.consume_buffered_packets_elapsed
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "filter_pending_packets_elapsed",
                    self.filter_pending_packets_elapsed
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "packet_conversion_elapsed",
                    self.packet_conversion_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "transaction_processing_elapsed",
                    self.transaction_processing_elapsed
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                )
            );
        }
    }
}

#[derive(Default)]
pub(self) struct PreBalanceInfo {
    native: Vec<Vec<u64>>,
    token: Vec<Vec<TransactionTokenBalance>>,
    mint_decimals: HashMap<Pubkey, u8>,
}

#[derive(Debug, Default)]
pub struct BatchedTransactionDetails {
    pub costs: BatchedTransactionCostDetails,
    pub errors: BatchedTransactionErrorDetails,
}

#[derive(Debug, Default)]
pub struct BatchedTransactionCostDetails {
    pub batched_signature_cost: u64,
    pub batched_write_lock_cost: u64,
    pub batched_data_bytes_cost: u64,
    pub batched_builtins_execute_cost: u64,
    pub batched_bpf_execute_cost: u64,
}

#[derive(Debug, Default)]
pub struct BatchedTransactionErrorDetails {
    pub batched_retried_txs_per_block_limit_count: u64,
    pub batched_retried_txs_per_vote_limit_count: u64,
    pub batched_retried_txs_per_account_limit_count: u64,
    pub batched_retried_txs_per_account_data_block_limit_count: u64,
    pub batched_dropped_txs_per_account_data_total_limit_count: u64,
}

/// Stores the stage's thread handle and output receiver.
pub struct BankingStage {
    bank_thread_hdls: Vec<JoinHandle<()>>,
}

#[derive(Debug, Clone)]
pub enum ForwardOption {
    NotForward,
    ForwardTpuVote,
    ForwardTransaction,
}

#[derive(Debug, Default)]
pub struct FilterForwardingResults {
    pub(crate) total_forwardable_packets: usize,
    pub(crate) total_tracer_packets_in_buffer: usize,
    pub(crate) total_forwardable_tracer_packets: usize,
    pub(crate) total_packet_conversion_us: u64,
    pub(crate) total_filter_packets_us: u64,
}

impl BankingStage {
    /// Create the stage using `bank`. Exit when `verified_receiver` is dropped.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        verified_receiver: BankingPacketReceiver,
        tpu_verified_vote_receiver: BankingPacketReceiver,
        verified_vote_receiver: BankingPacketReceiver,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        connection_cache: Arc<ConnectionCache>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        Self::new_num_threads(
            cluster_info,
            poh_recorder,
            verified_receiver,
            tpu_verified_vote_receiver,
            verified_vote_receiver,
            Self::num_threads(),
            transaction_status_sender,
            gossip_vote_sender,
            log_messages_bytes_limit,
            connection_cache,
            bank_forks,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_external_scheduler(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        transactions_receivers: Vec<ScheduledTransactionsReceiver>,
        processed_transactions_sender: ProcessedTransactionsSender,
        tpu_verified_vote_receiver: BankingPacketReceiver,
        verified_vote_receiver: BankingPacketReceiver,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        connection_cache: Arc<ConnectionCache>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        let num_threads = transactions_receivers.len();
        // Single thread to generate entries from many banks.
        // This thread talks to poh_service and broadcasts the entries once they have been recorded.
        // Once an entry has been recorded, its blockhash is registered with the bank.
        let data_budget = Arc::new(DataBudget::default());
        let batch_limit = TOTAL_BUFFERED_PACKETS / num_threads;
        // Many banks that process transactions in parallel.
        let mut bank_thread_hdls = Self::spawn_voting_threads(
            cluster_info.clone(),
            poh_recorder.clone(),
            tpu_verified_vote_receiver,
            verified_vote_receiver,
            transaction_status_sender.clone(),
            gossip_vote_sender.clone(),
            log_messages_bytes_limit,
            connection_cache.clone(),
            bank_forks,
            data_budget.clone(),
            batch_limit,
        );

        // Add non-vote transaction threads
        let index_to_id_offset = bank_thread_hdls.len() as u32;
        for (index, transactions_receiver) in transactions_receivers.into_iter().enumerate() {
            let id = index as u32 + index_to_id_offset;

            let pipe = ExecutionPipeline::run(
                id,
                transactions_receiver,
                processed_transactions_sender.clone(),
                poh_recorder.read().unwrap().bank_status.clone(),
                RecordExecutor::new(poh_recorder.read().unwrap().recorder()),
                gossip_vote_sender.clone(),
            );
            bank_thread_hdls.extend(pipe.take().into_iter());

            // let scheduler_handle = SchedulerHandle::new_external_scheduler(
            //     id,
            //     transactions_receiver,
            //     processed_transactions_sender.clone(),
            // );
            // bank_thread_hdls.push(Self::spawn_banking_thread_with_scheduler(
            //     id,
            //     scheduler_handle,
            //     poh_recorder.clone(),
            //     cluster_info.clone(),
            //     connection_cache.clone(),
            //     data_budget.clone(),
            //     log_messages_bytes_limit,
            //     transaction_status_sender.clone(),
            //     gossip_vote_sender.clone(),
            // ));
        }

        Self { bank_thread_hdls }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_num_threads(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        verified_receiver: BankingPacketReceiver,
        tpu_verified_vote_receiver: BankingPacketReceiver,
        verified_vote_receiver: BankingPacketReceiver,
        num_threads: u32,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        connection_cache: Arc<ConnectionCache>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        assert!(num_threads >= MIN_TOTAL_THREADS);
        // Single thread to generate entries from many banks.
        // This thread talks to poh_service and broadcasts the entries once they have been recorded.
        // Once an entry has been recorded, its blockhash is registered with the bank.
        let data_budget = Arc::new(DataBudget::default());
        let batch_limit =
            TOTAL_BUFFERED_PACKETS / ((num_threads - NUM_VOTE_PROCESSING_THREADS) as usize);

        // Many banks that process transactions in parallel.
        let mut bank_thread_hdls = Self::spawn_voting_threads(
            cluster_info.clone(),
            poh_recorder.clone(),
            tpu_verified_vote_receiver,
            verified_vote_receiver,
            transaction_status_sender.clone(),
            gossip_vote_sender.clone(),
            log_messages_bytes_limit,
            connection_cache.clone(),
            bank_forks.clone(),
            data_budget.clone(),
            batch_limit,
        );
        // Add non-vote transaction threads
        for id in 2..num_threads {
            let unprocessed_transaction_storage =
                UnprocessedTransactionStorage::new_transaction_storage(
                    UnprocessedPacketBatches::with_capacity(batch_limit),
                    ThreadType::Transactions,
                );
            let packet_deserializer = PacketDeserializer::new(verified_receiver.clone());
            let scheduler_handle = Self::build_thread_local_scheduler_handle(
                packet_deserializer,
                poh_recorder.clone(),
                bank_forks.clone(),
                cluster_info.clone(),
                id,
                unprocessed_transaction_storage,
            );

            bank_thread_hdls.push(Self::spawn_banking_thread_with_scheduler(
                id,
                scheduler_handle,
                poh_recorder.clone(),
                cluster_info.clone(),
                connection_cache.clone(),
                data_budget.clone(),
                log_messages_bytes_limit,
                transaction_status_sender.clone(),
                gossip_vote_sender.clone(),
            ));
        }

        Self { bank_thread_hdls }
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_voting_threads(
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        tpu_verified_vote_receiver: BankingPacketReceiver,
        verified_vote_receiver: BankingPacketReceiver,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        connection_cache: Arc<ConnectionCache>,
        bank_forks: Arc<RwLock<BankForks>>,
        data_budget: Arc<DataBudget>,
        batch_limit: usize,
    ) -> Vec<JoinHandle<()>> {
        // Keeps track of extraneous vote transactions for the vote threads
        let latest_unprocessed_votes = Arc::new(LatestUnprocessedVotes::new());
        let should_split_voting_threads = bank_forks
            .read()
            .map(|bank_forks| {
                let bank = bank_forks.root_bank();
                bank.feature_set
                    .is_active(&allow_votes_to_directly_update_vote_state::id())
            })
            .unwrap_or(false);

        vec![
            // Gossip voting thread
            Self::spawn_voting_thread(
                0,
                VoteSource::Gossip,
                should_split_voting_threads,
                batch_limit,
                latest_unprocessed_votes.clone(),
                PacketDeserializer::new(verified_vote_receiver),
                poh_recorder.clone(),
                bank_forks.clone(),
                cluster_info.clone(),
                connection_cache.clone(),
                data_budget.clone(),
                log_messages_bytes_limit,
                transaction_status_sender.clone(),
                gossip_vote_sender.clone(),
            ),
            // TPU voting thread
            Self::spawn_voting_thread(
                1,
                VoteSource::Tpu,
                should_split_voting_threads,
                batch_limit,
                latest_unprocessed_votes,
                PacketDeserializer::new(tpu_verified_vote_receiver),
                poh_recorder,
                bank_forks,
                cluster_info,
                connection_cache,
                data_budget,
                log_messages_bytes_limit,
                transaction_status_sender,
                gossip_vote_sender,
            ),
        ]
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_voting_thread(
        id: u32,
        voting_source: VoteSource,
        should_split_voting_threads: bool,
        buffer_capacity: usize,
        latest_unprocessed_votes: Arc<LatestUnprocessedVotes>,
        packet_deserializer: PacketDeserializer,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
        cluster_info: Arc<ClusterInfo>,
        connection_cache: Arc<ConnectionCache>,
        data_budget: Arc<DataBudget>,
        log_messages_bytes_limit: Option<usize>,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
    ) -> JoinHandle<()> {
        let buffer = if should_split_voting_threads {
            UnprocessedTransactionStorage::new_vote_storage(latest_unprocessed_votes, voting_source)
        } else {
            UnprocessedTransactionStorage::new_transaction_storage(
                UnprocessedPacketBatches::with_capacity(buffer_capacity),
                ThreadType::Voting(voting_source),
            )
        };

        let scheduler_handle = Self::build_thread_local_scheduler_handle(
            packet_deserializer,
            poh_recorder.clone(),
            bank_forks,
            cluster_info.clone(),
            id,
            buffer,
        );

        Self::spawn_banking_thread_with_scheduler(
            id,
            scheduler_handle,
            poh_recorder,
            cluster_info,
            connection_cache,
            data_budget,
            log_messages_bytes_limit,
            transaction_status_sender,
            gossip_vote_sender,
        )
    }

    fn spawn_banking_thread_with_scheduler(
        id: u32,
        scheduler_handle: SchedulerHandle,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: Arc<ClusterInfo>,
        connection_cache: Arc<ConnectionCache>,
        data_budget: Arc<DataBudget>,
        log_messages_bytes_limit: Option<usize>,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
    ) -> JoinHandle<()> {
        let bank_status = poh_recorder.read().unwrap().bank_status.clone();
        let (forward_executor, consume_executor) = Self::build_executors(
            id,
            poh_recorder,
            cluster_info,
            connection_cache,
            data_budget,
            log_messages_bytes_limit,
            transaction_status_sender,
            gossip_vote_sender,
        );
        Builder::new()
            .name(format!("solBanknStgTx{:02}", id))
            .spawn(move || {
                Self::process_loop(
                    id,
                    scheduler_handle,
                    bank_status,
                    forward_executor,
                    consume_executor,
                );
            })
            .unwrap()
    }

    fn build_executors(
        id: u32,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: Arc<ClusterInfo>,
        connection_cache: Arc<ConnectionCache>,
        data_budget: Arc<DataBudget>,
        log_messages_bytes_limit: Option<usize>,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
    ) -> (ForwardExecutor, ConsumeExecutor) {
        let transaction_recorder = poh_recorder.read().unwrap().recorder();
        let forward_executor = ForwardExecutor::new(
            poh_recorder,
            UdpSocket::bind("0.0.0.0:0").unwrap(),
            cluster_info,
            connection_cache,
            data_budget,
        );
        let record_executor = RecordExecutor::new(transaction_recorder);
        let commit_executor = CommitExecutor::new(transaction_status_sender, gossip_vote_sender);
        let consume_executor = ConsumeExecutor::new(
            record_executor,
            commit_executor,
            QosService::new(id),
            log_messages_bytes_limit,
        );

        (forward_executor, consume_executor)
    }

    fn build_thread_local_scheduler_handle(
        packet_deserializer: PacketDeserializer,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
        cluster_info: Arc<ClusterInfo>,
        id: u32,
        unprocessed_transaction_storage: UnprocessedTransactionStorage,
    ) -> SchedulerHandle {
        let packet_receiver = PacketReceiver::new(id, packet_deserializer);
        let decision_maker = DecisionMaker::new(cluster_info.id(), poh_recorder);
        SchedulerHandle::new_thread_local_scheduler(
            id,
            decision_maker,
            unprocessed_transaction_storage,
            bank_forks,
            packet_receiver,
        )
    }

    fn process_loop(
        id: u32,
        mut scheduler_handle: SchedulerHandle,
        bank_status: Arc<BankStatus>,
        forward_executor: ForwardExecutor,
        consume_executor: ConsumeExecutor,
    ) {
        let mut tracer_packet_stats = TracerPacketStats::new(id);
        let mut slot_metrics_tracker = LeaderSlotMetricsTracker::new(id);

        loop {
            // Do scheduled work (processing packets)
            if let Err(err) = scheduler_handle.do_scheduled_work(
                &bank_status,
                &consume_executor,
                &forward_executor,
                &mut tracer_packet_stats,
                &mut slot_metrics_tracker,
            ) {
                warn!("Banking stage scheduler error: {:?}", err);
                break;
            }

            // Do any necessary updates - check if scheduler is still valid
            if let Err(err) = scheduler_handle.tick() {
                warn!("Banking stage scheduler error: {:?}", err);
                break;
            }

            tracer_packet_stats.report(1000);
        }
    }

    pub fn num_threads() -> u32 {
        cmp::max(
            env::var("SOLANA_BANKING_THREADS")
                .map(|x| x.parse().unwrap_or(NUM_THREADS))
                .unwrap_or(NUM_THREADS),
            MIN_TOTAL_THREADS,
        )
    }

    pub fn num_non_vote_threads() -> u32 {
        Self::num_threads() - NUM_VOTE_PROCESSING_THREADS
    }

    pub fn join(self) -> thread::Result<()> {
        for bank_thread_hdl in self.bank_thread_hdls {
            bank_thread_hdl.join()?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(self) mod tests {
    use {
        super::*,
        crossbeam_channel::{unbounded, Receiver},
        itertools::Itertools,
        solana_address_lookup_table_program::state::{AddressLookupTable, LookupTableMeta},
        solana_entry::entry::{Entry, EntrySlice},
        solana_gossip::{cluster_info::Node, contact_info::ContactInfo},
        solana_ledger::{
            blockstore::Blockstore,
            genesis_utils::{create_genesis_config, GenesisConfigInfo},
            get_tmp_ledger_path_auto_delete,
            leader_schedule_cache::LeaderScheduleCache,
        },
        solana_perf::packet::to_packet_batches,
        solana_poh::{
            poh_recorder::{create_test_recorder, Record, WorkingBankEntry},
            poh_service::PohService,
        },
        solana_runtime::{bank::Bank, bank_forks::BankForks, genesis_utils::activate_feature},
        solana_sdk::{
            self,
            account::AccountSharedData,
            hash::Hash,
            poh_config::PohConfig,
            signature::{Keypair, Signer},
            system_transaction,
            transaction::{SanitizedTransaction, Transaction},
        },
        solana_streamer::socket::SocketAddrSpace,
        solana_vote_program::{
            vote_state::VoteStateUpdate, vote_transaction::new_vote_state_update_transaction,
        },
        std::{
            borrow::Cow,
            path::Path,
            sync::atomic::{AtomicBool, Ordering},
            thread::sleep,
        },
    };

    pub(crate) fn new_test_cluster_info(contact_info: ContactInfo) -> ClusterInfo {
        ClusterInfo::new(
            contact_info,
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        )
    }

    #[test]
    fn test_banking_stage_shutdown1() {
        let genesis_config = create_genesis_config(2).genesis_config;
        let bank = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let bank = Arc::new(bank_forks.read().unwrap().get(0).unwrap());
        let (verified_sender, verified_receiver) = unbounded();
        let (gossip_verified_vote_sender, gossip_verified_vote_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Arc::new(
                Blockstore::open(ledger_path.path())
                    .expect("Expected to be able to open database ledger"),
            );
            let (exit, poh_recorder, poh_service, _entry_receiever) =
                create_test_recorder(&bank, &blockstore, None, None);
            let cluster_info = new_test_cluster_info(Node::new_localhost().info);
            let cluster_info = Arc::new(cluster_info);
            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();

            let banking_stage = BankingStage::new(
                &cluster_info,
                &poh_recorder,
                verified_receiver,
                tpu_vote_receiver,
                gossip_verified_vote_receiver,
                None,
                gossip_vote_sender,
                None,
                Arc::new(ConnectionCache::default()),
                bank_forks,
            );
            drop(verified_sender);
            drop(gossip_verified_vote_sender);
            drop(tpu_vote_sender);
            exit.store(true, Ordering::Relaxed);
            banking_stage.join().unwrap();
            poh_service.join().unwrap();
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }

    #[test]
    fn test_banking_stage_tick() {
        solana_logger::setup();
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config(2);
        genesis_config.ticks_per_slot = 4;
        let num_extra_ticks = 2;
        let bank = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let bank = Arc::new(bank_forks.read().unwrap().get(0).unwrap());
        let start_hash = bank.last_blockhash();
        let (verified_sender, verified_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Arc::new(
                Blockstore::open(ledger_path.path())
                    .expect("Expected to be able to open database ledger"),
            );
            let poh_config = PohConfig {
                target_tick_count: Some(bank.max_tick_height() + num_extra_ticks),
                ..PohConfig::default()
            };
            let (exit, poh_recorder, poh_service, entry_receiver) =
                create_test_recorder(&bank, &blockstore, Some(poh_config), None);
            let cluster_info = new_test_cluster_info(Node::new_localhost().info);
            let cluster_info = Arc::new(cluster_info);
            let (verified_gossip_vote_sender, verified_gossip_vote_receiver) = unbounded();
            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();

            let banking_stage = BankingStage::new(
                &cluster_info,
                &poh_recorder,
                verified_receiver,
                tpu_vote_receiver,
                verified_gossip_vote_receiver,
                None,
                gossip_vote_sender,
                None,
                Arc::new(ConnectionCache::default()),
                bank_forks,
            );
            trace!("sending bank");
            drop(verified_sender);
            drop(verified_gossip_vote_sender);
            drop(tpu_vote_sender);
            exit.store(true, Ordering::Relaxed);
            poh_service.join().unwrap();
            drop(poh_recorder);

            trace!("getting entries");
            let entries: Vec<_> = entry_receiver
                .iter()
                .map(|(_bank, (entry, _tick_height))| entry)
                .collect();
            trace!("done");
            assert_eq!(entries.len(), genesis_config.ticks_per_slot as usize);
            assert!(entries.verify(&start_hash));
            assert_eq!(entries[entries.len() - 1].hash, bank.last_blockhash());
            banking_stage.join().unwrap();
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }

    pub fn convert_from_old_verified(
        mut with_vers: Vec<(PacketBatch, Vec<u8>)>,
    ) -> Vec<PacketBatch> {
        with_vers.iter_mut().for_each(|(b, v)| {
            b.iter_mut()
                .zip(v)
                .for_each(|(p, f)| p.meta.set_discard(*f == 0))
        });
        with_vers.into_iter().map(|(b, _)| b).collect()
    }

    #[test]
    fn test_banking_stage_entries_only() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(10);
        let bank = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let bank = Arc::new(bank_forks.read().unwrap().get(0).unwrap());
        let start_hash = bank.last_blockhash();
        let (verified_sender, verified_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let (gossip_verified_vote_sender, gossip_verified_vote_receiver) = unbounded();
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Arc::new(
                Blockstore::open(ledger_path.path())
                    .expect("Expected to be able to open database ledger"),
            );
            let poh_config = PohConfig {
                // limit tick count to avoid clearing working_bank at PohRecord then
                // PohRecorderError(MaxHeightReached) at BankingStage
                target_tick_count: Some(bank.max_tick_height() - 1),
                ..PohConfig::default()
            };
            let (exit, poh_recorder, poh_service, entry_receiver) =
                create_test_recorder(&bank, &blockstore, Some(poh_config), None);
            let cluster_info = new_test_cluster_info(Node::new_localhost().info);
            let cluster_info = Arc::new(cluster_info);
            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();

            let banking_stage = BankingStage::new(
                &cluster_info,
                &poh_recorder,
                verified_receiver,
                tpu_vote_receiver,
                gossip_verified_vote_receiver,
                None,
                gossip_vote_sender,
                None,
                Arc::new(ConnectionCache::default()),
                bank_forks,
            );

            // fund another account so we can send 2 good transactions in a single batch.
            let keypair = Keypair::new();
            let fund_tx =
                system_transaction::transfer(&mint_keypair, &keypair.pubkey(), 2, start_hash);
            bank.process_transaction(&fund_tx).unwrap();

            // good tx
            let to = solana_sdk::pubkey::new_rand();
            let tx = system_transaction::transfer(&mint_keypair, &to, 1, start_hash);

            // good tx, but no verify
            let to2 = solana_sdk::pubkey::new_rand();
            let tx_no_ver = system_transaction::transfer(&keypair, &to2, 2, start_hash);

            // bad tx, AccountNotFound
            let keypair = Keypair::new();
            let to3 = solana_sdk::pubkey::new_rand();
            let tx_anf = system_transaction::transfer(&keypair, &to3, 1, start_hash);

            // send 'em over
            let packet_batches = to_packet_batches(&[tx_no_ver, tx_anf, tx], 3);

            // glad they all fit
            assert_eq!(packet_batches.len(), 1);

            let packet_batches = packet_batches
                .into_iter()
                .map(|batch| (batch, vec![0u8, 1u8, 1u8]))
                .collect();
            let packet_batches = convert_from_old_verified(packet_batches);
            verified_sender // no_ver, anf, tx
                .send((packet_batches, None))
                .unwrap();

            drop(verified_sender);
            drop(tpu_vote_sender);
            drop(gossip_verified_vote_sender);
            // wait until banking_stage to finish up all packets
            banking_stage.join().unwrap();

            exit.store(true, Ordering::Relaxed);
            poh_service.join().unwrap();
            drop(poh_recorder);

            let mut blockhash = start_hash;
            let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
            bank.process_transaction(&fund_tx).unwrap();
            //receive entries + ticks
            loop {
                let entries: Vec<Entry> = entry_receiver
                    .iter()
                    .map(|(_bank, (entry, _tick_height))| entry)
                    .collect();

                assert!(entries.verify(&blockhash));
                if !entries.is_empty() {
                    blockhash = entries.last().unwrap().hash;
                    for entry in entries {
                        bank.process_entry_transactions(entry.transactions)
                            .iter()
                            .for_each(|x| assert_eq!(*x, Ok(())));
                    }
                }

                if bank.get_balance(&to) == 1 {
                    break;
                }

                sleep(Duration::from_millis(200));
            }

            assert_eq!(bank.get_balance(&to), 1);
            assert_eq!(bank.get_balance(&to2), 0);

            drop(entry_receiver);
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }

    #[test]
    fn test_banking_stage_entryfication() {
        solana_logger::setup();
        // In this attack we'll demonstrate that a verifier can interpret the ledger
        // differently if either the server doesn't signal the ledger to add an
        // Entry OR if the verifier tries to parallelize across multiple Entries.
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(2);
        let (verified_sender, verified_receiver) = unbounded();

        // Process a batch that includes a transaction that receives two lamports.
        let alice = Keypair::new();
        let tx =
            system_transaction::transfer(&mint_keypair, &alice.pubkey(), 2, genesis_config.hash());

        let packet_batches = to_packet_batches(&[tx], 1);
        let packet_batches = packet_batches
            .into_iter()
            .map(|batch| (batch, vec![1u8]))
            .collect();
        let packet_batches = convert_from_old_verified(packet_batches);
        verified_sender.send((packet_batches, None)).unwrap();

        // Process a second batch that uses the same from account, so conflicts with above TX
        let tx =
            system_transaction::transfer(&mint_keypair, &alice.pubkey(), 1, genesis_config.hash());
        let packet_batches = to_packet_batches(&[tx], 1);
        let packet_batches = packet_batches
            .into_iter()
            .map(|batch| (batch, vec![1u8]))
            .collect();
        let packet_batches = convert_from_old_verified(packet_batches);
        verified_sender.send((packet_batches, None)).unwrap();

        let (vote_sender, vote_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();

            let entry_receiver = {
                // start a banking_stage to eat verified receiver
                let bank = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
                let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
                let bank = Arc::new(bank_forks.read().unwrap().get(0).unwrap());
                let blockstore = Arc::new(
                    Blockstore::open(ledger_path.path())
                        .expect("Expected to be able to open database ledger"),
                );
                let poh_config = PohConfig {
                    // limit tick count to avoid clearing working_bank at
                    // PohRecord then PohRecorderError(MaxHeightReached) at BankingStage
                    target_tick_count: Some(bank.max_tick_height() - 1),
                    ..PohConfig::default()
                };
                let (exit, poh_recorder, poh_service, entry_receiver) =
                    create_test_recorder(&bank, &blockstore, Some(poh_config), None);
                let cluster_info = new_test_cluster_info(Node::new_localhost().info);
                let cluster_info = Arc::new(cluster_info);
                let _banking_stage = BankingStage::new_num_threads(
                    &cluster_info,
                    &poh_recorder,
                    verified_receiver,
                    tpu_vote_receiver,
                    vote_receiver,
                    3,
                    None,
                    gossip_vote_sender,
                    None,
                    Arc::new(ConnectionCache::default()),
                    bank_forks,
                );

                // wait for banking_stage to eat the packets
                while bank.get_balance(&alice.pubkey()) < 1 {
                    sleep(Duration::from_millis(10));
                }
                exit.store(true, Ordering::Relaxed);
                poh_service.join().unwrap();
                entry_receiver
            };
            drop(verified_sender);
            drop(vote_sender);
            drop(tpu_vote_sender);

            // consume the entire entry_receiver, feed it into a new bank
            // check that the balance is what we expect.
            let entries: Vec<_> = entry_receiver
                .iter()
                .map(|(_bank, (entry, _tick_height))| entry)
                .collect();

            let bank = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
            for entry in entries {
                bank.process_entry_transactions(entry.transactions)
                    .iter()
                    .for_each(|x| assert_eq!(*x, Ok(())));
            }

            // Assert the user doesn't hold three lamports. If the stage only outputs one
            // entry, then one of the transactions will be rejected, because it drives
            // the account balance below zero before the credit is added.
            assert!(bank.get_balance(&alice.pubkey()) != 3);
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }

    pub(crate) fn sanitize_transactions(txs: Vec<Transaction>) -> Vec<SanitizedTransaction> {
        txs.into_iter()
            .map(SanitizedTransaction::from_transaction_for_tests)
            .collect()
    }

    pub(crate) fn create_slow_genesis_config(lamports: u64) -> GenesisConfigInfo {
        let mut config_info = create_genesis_config(lamports);
        // For these tests there's only 1 slot, don't want to run out of ticks
        config_info.genesis_config.ticks_per_slot *= 8;
        config_info
    }

    pub(crate) fn simulate_poh(
        record_receiver: CrossbeamReceiver<Record>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
    ) -> JoinHandle<()> {
        let poh_recorder = poh_recorder.clone();
        let is_exited = poh_recorder.read().unwrap().is_exited.clone();
        let tick_producer = Builder::new()
            .name("solana-simulate_poh".to_string())
            .spawn(move || loop {
                PohService::read_record_receiver_and_process(
                    &poh_recorder,
                    &record_receiver,
                    Duration::from_millis(10),
                );
                if is_exited.load(Ordering::Relaxed) {
                    break;
                }
            });
        tick_producer.unwrap()
    }

    pub(crate) fn generate_new_address_lookup_table(
        authority: Option<Pubkey>,
        num_addresses: usize,
    ) -> AddressLookupTable<'static> {
        let mut addresses = Vec::with_capacity(num_addresses);
        addresses.resize_with(num_addresses, Pubkey::new_unique);
        AddressLookupTable {
            meta: LookupTableMeta {
                authority,
                ..LookupTableMeta::default()
            },
            addresses: Cow::Owned(addresses),
        }
    }

    pub(crate) fn store_address_lookup_table(
        bank: &Bank,
        account_address: Pubkey,
        address_lookup_table: AddressLookupTable<'static>,
    ) -> AccountSharedData {
        let data = address_lookup_table.serialize_for_tests().unwrap();
        let mut account =
            AccountSharedData::new(1, data.len(), &solana_address_lookup_table_program::id());
        account.set_data(data);
        bank.store_account(&account_address, &account);

        account
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn setup_conflicting_transactions(
        ledger_path: &Path,
    ) -> (
        Vec<Transaction>,
        Arc<Bank>,
        Arc<RwLock<PohRecorder>>,
        Receiver<WorkingBankEntry>,
        JoinHandle<()>,
    ) {
        Blockstore::destroy(ledger_path).unwrap();
        let genesis_config_info = create_slow_genesis_config(10_000);
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = &genesis_config_info;
        let blockstore =
            Blockstore::open(ledger_path).expect("Expected to be able to open database ledger");
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(genesis_config));
        let exit = Arc::new(AtomicBool::default());
        let (poh_recorder, entry_receiver, record_receiver) = PohRecorder::new(
            bank.tick_height(),
            bank.last_blockhash(),
            bank.clone(),
            Some((4, 4)),
            bank.ticks_per_slot(),
            &solana_sdk::pubkey::new_rand(),
            &Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            &Arc::new(PohConfig::default()),
            exit,
        );
        let poh_recorder = Arc::new(RwLock::new(poh_recorder));

        // Set up unparallelizable conflicting transactions
        let pubkey0 = solana_sdk::pubkey::new_rand();
        let pubkey1 = solana_sdk::pubkey::new_rand();
        let pubkey2 = solana_sdk::pubkey::new_rand();
        let transactions = vec![
            system_transaction::transfer(mint_keypair, &pubkey0, 1, genesis_config.hash()),
            system_transaction::transfer(mint_keypair, &pubkey1, 1, genesis_config.hash()),
            system_transaction::transfer(mint_keypair, &pubkey2, 1, genesis_config.hash()),
        ];
        let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

        (
            transactions,
            bank,
            poh_recorder,
            entry_receiver,
            poh_simulator,
        )
    }

    #[test]
    fn test_unprocessed_transaction_storage_full_send() {
        solana_logger::setup();
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(10000);
        activate_feature(
            &mut genesis_config,
            allow_votes_to_directly_update_vote_state::id(),
        );
        let bank = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let bank = Arc::new(bank_forks.read().unwrap().get(0).unwrap());
        let start_hash = bank.last_blockhash();
        let (verified_sender, verified_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let (gossip_verified_vote_sender, gossip_verified_vote_receiver) = unbounded();
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Arc::new(
                Blockstore::open(ledger_path.path())
                    .expect("Expected to be able to open database ledger"),
            );
            let poh_config = PohConfig {
                // limit tick count to avoid clearing working_bank at PohRecord then
                // PohRecorderError(MaxHeightReached) at BankingStage
                target_tick_count: Some(bank.max_tick_height() - 1),
                ..PohConfig::default()
            };
            let (exit, poh_recorder, poh_service, _entry_receiver) =
                create_test_recorder(&bank, &blockstore, Some(poh_config), None);
            let cluster_info = new_test_cluster_info(Node::new_localhost().info);
            let cluster_info = Arc::new(cluster_info);
            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();

            let banking_stage = BankingStage::new(
                &cluster_info,
                &poh_recorder,
                verified_receiver,
                tpu_vote_receiver,
                gossip_verified_vote_receiver,
                None,
                gossip_vote_sender,
                None,
                Arc::new(ConnectionCache::default()),
                bank_forks,
            );

            let keypairs = (0..100).map(|_| Keypair::new()).collect_vec();
            let vote_keypairs = (0..100).map(|_| Keypair::new()).collect_vec();
            for keypair in keypairs.iter() {
                bank.process_transaction(&system_transaction::transfer(
                    &mint_keypair,
                    &keypair.pubkey(),
                    20,
                    start_hash,
                ))
                .unwrap();
            }

            // Send a bunch of votes and transfers
            let tpu_votes = (0..100_usize)
                .map(|i| {
                    new_vote_state_update_transaction(
                        VoteStateUpdate::from(vec![
                            (0, 8),
                            (1, 7),
                            (i as u64 + 10, 6),
                            (i as u64 + 11, 1),
                        ]),
                        Hash::new_unique(),
                        &keypairs[i],
                        &vote_keypairs[i],
                        &vote_keypairs[i],
                        None,
                    );
                })
                .collect_vec();
            let gossip_votes = (0..100_usize)
                .map(|i| {
                    new_vote_state_update_transaction(
                        VoteStateUpdate::from(vec![
                            (0, 8),
                            (1, 7),
                            (i as u64 + 64 + 5, 6),
                            (i as u64 + 7, 1),
                        ]),
                        Hash::new_unique(),
                        &keypairs[i],
                        &vote_keypairs[i],
                        &vote_keypairs[i],
                        None,
                    );
                })
                .collect_vec();
            let txs = (0..100_usize)
                .map(|i| {
                    system_transaction::transfer(
                        &keypairs[i],
                        &keypairs[(i + 1) % 100].pubkey(),
                        10,
                        start_hash,
                    );
                })
                .collect_vec();

            let tpu_packet_batches = to_packet_batches(&tpu_votes, 10);
            let gossip_packet_batches = to_packet_batches(&gossip_votes, 10);
            let tx_packet_batches = to_packet_batches(&txs, 10);

            // Send em all
            [
                (tpu_packet_batches, tpu_vote_sender.clone()),
                (gossip_packet_batches, gossip_verified_vote_sender.clone()),
                (tx_packet_batches, verified_sender.clone()),
            ]
            .into_iter()
            .map(|(packet_batches, sender)| {
                Builder::new()
                    .spawn(move || sender.send((packet_batches, None)).unwrap())
                    .unwrap()
            })
            .for_each(|handle| handle.join().unwrap());

            drop(verified_sender);
            drop(tpu_vote_sender);
            drop(gossip_verified_vote_sender);
            banking_stage.join().unwrap();
            exit.store(true, Ordering::Relaxed);
            poh_service.join().unwrap();
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }
}
