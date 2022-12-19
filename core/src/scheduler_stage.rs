//! Stage for central transaction scheduler(s).
//!

use {
    crate::{
        banking_stage::{
            decision_maker::{BufferedPacketsDecision, DecisionMaker},
            multi_iterator_scheduler::MultiIteratorScheduler,
            thread_aware_account_locks::ThreadId,
            BankingStage,
        },
        packet_deserializer::{BankingPacketReceiver, PacketDeserializer},
        unprocessed_packet_batches::DeserializedPacket,
    },
    crossbeam_channel::{Receiver, Sender},
    solana_gossip::cluster_info::ClusterInfo,
    solana_poh::poh_recorder::PohRecorder,
    solana_runtime::{bank_forks::BankForks, root_bank_cache::RootBankCache},
    solana_sdk::transaction::SanitizedTransaction,
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::JoinHandle,
    },
};

pub enum SchedulerKind {
    /// Run scheduler's inside of banking stage threads.
    ThreadLocalSchedulers,
    /// Run central multi-iterator scheduler
    MultiIteratorScheduler,
}

/// Message: Scheduler -> Executor
pub struct ScheduledTransactions {
    pub thread_id: ThreadId,
    pub decision: BufferedPacketsDecision,
    pub packets: Vec<DeserializedPacket>,
    pub transactions: Vec<SanitizedTransaction>,
    /// Alows scheduler to mark transactions as invalid after they've been sent to the executor
    validity_check: Arc<AtomicBool>,
}

impl ScheduledTransactions {
    pub fn with_capacity(
        thread_id: ThreadId,
        decision: BufferedPacketsDecision,
        capacity: usize,
        validity_check: Arc<AtomicBool>,
    ) -> Self {
        Self {
            thread_id,
            decision,
            packets: Vec::with_capacity(capacity),
            transactions: Vec::with_capacity(capacity),
            validity_check,
        }
    }

    pub fn mark_as_invalid(&self) {
        self.validity_check.store(false, Ordering::Relaxed);
    }

    pub fn is_valid(&self) -> bool {
        self.validity_check.load(Ordering::Relaxed)
    }
}

/// Message: Executor -> Scheduler
#[derive(Default)]
pub struct ProcessedTransactions {
    pub thread_id: ThreadId,
    pub packets: Vec<DeserializedPacket>,
    pub transactions: Vec<SanitizedTransaction>,
    pub retryable: Vec<bool>,
    pub invalidated: bool,
}

pub type ScheduledTransactionsSender = Sender<ScheduledTransactions>;
pub type ScheduledTransactionsReceiver = Receiver<ScheduledTransactions>;
pub type ProcessedTransactionsSender = Sender<ProcessedTransactions>;
pub type ProcessedTransactionsReceiver = Receiver<ProcessedTransactions>;

#[derive(Default)]
pub struct SchedulerStage {
    /// Optional scheduler thread handles
    /// Empty if scheduler is running in banking stage threads
    scheduler_thread_handles: Vec<JoinHandle<()>>,
}

impl SchedulerStage {
    pub fn new(
        option: SchedulerKind,
        packet_receiver: BankingPacketReceiver,
        bank_forks: Arc<RwLock<BankForks>>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: &ClusterInfo,
    ) -> (
        Self,
        Option<Vec<ScheduledTransactionsReceiver>>,
        Option<ProcessedTransactionsSender>,
    ) {
        Self::new_num_threads(
            option,
            BankingStage::num_non_vote_threads() as usize,
            packet_receiver,
            bank_forks,
            poh_recorder,
            cluster_info,
        )
    }

    pub fn new_num_threads(
        option: SchedulerKind,
        num_non_vote_threads: usize,
        packet_receiver: BankingPacketReceiver,
        bank_forks: Arc<RwLock<BankForks>>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: &ClusterInfo,
    ) -> (
        Self,
        Option<Vec<ScheduledTransactionsReceiver>>,
        Option<ProcessedTransactionsSender>,
    ) {
        match option {
            SchedulerKind::ThreadLocalSchedulers => (
                Self {
                    scheduler_thread_handles: vec![],
                },
                None,
                None,
            ),
            SchedulerKind::MultiIteratorScheduler => {
                let (transaction_senders, transaction_receivers) =
                    Self::create_channel_pairs(num_non_vote_threads);
                let (processed_transactions_sender, processed_transactions_receiver) =
                    crossbeam_channel::unbounded();

                (
                    Self {
                        scheduler_thread_handles: Self::start_multi_iterator_scheduler_thread(
                            num_non_vote_threads,
                            packet_receiver,
                            bank_forks,
                            poh_recorder,
                            cluster_info,
                            transaction_senders,
                            processed_transactions_receiver,
                        ),
                    },
                    Some(transaction_receivers),
                    Some(processed_transactions_sender),
                )
            }
        }
    }

    pub fn join(self) -> std::thread::Result<()> {
        for handle in self.scheduler_thread_handles {
            handle.join()?;
        }

        Ok(())
    }

    fn start_multi_iterator_scheduler_thread(
        num_executor_threads: usize,
        packet_receiver: BankingPacketReceiver,
        bank_forks: Arc<RwLock<BankForks>>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        cluster_info: &ClusterInfo,
        transaction_senders: Vec<ScheduledTransactionsSender>,
        processed_transactions_receiver: ProcessedTransactionsReceiver,
    ) -> Vec<JoinHandle<()>> {
        let scheduler = MultiIteratorScheduler::new(
            num_executor_threads,
            DecisionMaker::new(cluster_info.my_contact_info().id, poh_recorder),
            PacketDeserializer::new(packet_receiver),
            RootBankCache::new(bank_forks),
            transaction_senders,
            processed_transactions_receiver,
            100_000,
        );

        vec![std::thread::Builder::new()
            .name("solCMISched".to_owned())
            .spawn(move || {
                scheduler.run();
            })
            .unwrap()]
    }

    fn create_channel_pairs<T>(num_executor_threads: usize) -> (Vec<Sender<T>>, Vec<Receiver<T>>) {
        let mut transaction_senders = Vec::with_capacity(num_executor_threads);
        let mut transaction_receivers = Vec::with_capacity(num_executor_threads);
        for _ in 0..num_executor_threads {
            let (transaction_sender, transaction_receiver) = crossbeam_channel::unbounded();
            transaction_senders.push(transaction_sender);
            transaction_receivers.push(transaction_receiver);
        }
        (transaction_senders, transaction_receivers)
    }
}
