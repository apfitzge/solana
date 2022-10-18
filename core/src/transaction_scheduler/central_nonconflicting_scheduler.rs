//! Implements a transaction scheduler that queues up non-conflicting batches of transactions
//! for banking threads to process. Design based on: https://github.com/solana-labs/solana/pull/26362
//! Additionally, adding the thread-aware batch building from https://github.com/solana-labs/solana/pull/26924
//!

use {
    super::{
        conflict_set::ConflictSet, BankingProcessingInstruction, ProcessedPacketBatch,
        ScheduledPacketBatch, ScheduledPacketBatchId, ScheduledPacketBatchIdGenerator,
        TransactionSchedulerBankingHandle,
    },
    crate::{
        bank_process_decision::{BankPacketProcessingDecision, BankingDecisionMaker},
        forward_packet_batches_by_accounts::ForwardPacketBatchesByAccounts,
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        packet_deserializer_stage::DeserializedPacketBatchGetter,
        unprocessed_packet_batches::DeserializedPacket,
    },
    core::panic,
    crossbeam_channel::{Receiver, RecvTimeoutError, SendError, Sender},
    min_max_heap::MinMaxHeap,
    solana_measure::measure,
    solana_runtime::{
        bank::Bank,
        bank_forks::{BankForks, ReadOnlyAtomicSlot},
    },
    solana_sdk::{
        feature_set::FeatureSet,
        hash::Hash,
        pubkey::Pubkey,
        saturating_add_assign,
        timing::AtomicInterval,
        transaction::{
            SanitizedTransaction, TransactionAccountLocks, TransactionError, MAX_TX_ACCOUNT_LOCKS,
        },
    },
    std::{
        cell::RefCell,
        collections::{BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque},
        fmt::Display,
        rc::Rc,
        sync::{atomic::Ordering, Arc, RwLock},
        thread::{current, Builder},
        time::{Duration, Instant},
    },
};

const MAX_BATCH_SIZE: usize = 64;
const MAX_QUEUED_BATCHES: usize = 8; // re-evaluate this number
const MAX_BATCH_AGE: Duration = Duration::from_millis(1);

#[derive(Debug)]
/// A sanitized transaction with the packet priority
struct SanitizedTransactionPriority {
    /// Packet priority
    priority: u64,
    /// Sanitized transaction
    transaction: SanitizedTransaction,
    /// Timestamp of when the transaction came into the scheduler
    timestamp: Instant,
}

impl PartialEq for SanitizedTransactionPriority {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
            && self.timestamp == other.timestamp
            && self.transaction.message_hash() == other.transaction.message_hash()
    }
}

impl Eq for SanitizedTransactionPriority {}

impl PartialOrd for SanitizedTransactionPriority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SanitizedTransactionPriority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.timestamp.cmp(&other.timestamp))
            .then_with(|| {
                self.transaction
                    .message_hash()
                    .cmp(&other.transaction.message_hash())
            })
    }
}

impl SanitizedTransactionPriority {
    fn try_new(packet: &ImmutableDeserializedPacket, bank: &Bank) -> Option<Self> {
        let transaction = SanitizedTransaction::try_new(
            packet.transaction().clone(),
            *packet.message_hash(),
            packet.is_simple_vote(),
            bank,
        )
        .ok()?;
        Some(Self {
            priority: packet.priority(),
            transaction,
            timestamp: Instant::now(),
        })
    }

    /// Get the message hash from the transaction
    fn message_hash(&self) -> &Hash {
        self.transaction.message_hash()
    }

    /// Get account locks from the transaction
    fn get_account_locks(&self) -> Option<TransactionAccountLocks> {
        self.transaction
            .get_account_locks(MAX_TX_ACCOUNT_LOCKS)
            .ok()
    }
}

type TransactionRef = Rc<SanitizedTransactionPriority>;

/// A scheduler that prepares batches of transactions based on priorty ordering and without conflict
/// between batches. This scheduler is intended to be run in a separate thread with multiple banking
/// stage threads processing the prepared batches.
pub struct CentralNonConflictingScheduler<D>
where
    D: DeserializedPacketBatchGetter,
{
    /// Interface for getting deserialized packets from sigverify stage
    deserialized_packet_batch_getter: D,
    /// Senders for sending batches of transactions to banking stage threads - indexed by thread index
    scheduled_packet_batch_senders: Vec<Sender<Arc<ScheduledPacketBatch>>>,
    /// Receiver for getting batches of transactions that have been processed by banking stage
    /// and potentially need to be retried.
    processed_packet_batch_receiver: Receiver<ProcessedPacketBatch>,

    /// Packets to be held after forwarding
    held_packets: Vec<TransactionRef>,
    /// Caching root bank
    root_bank_cache: RootBankCache,
    /// Forward packet filter
    forward_filter: Option<ForwardPacketBatchesByAccounts>,
    /// Determines how the scheduler should handle packets currently.
    banking_decision_maker: Arc<BankingDecisionMaker>,

    /// Queue structure for ordering and keeping track of transactions
    transaction_queue: TransactionQueue,
    /// Scheduled batch currently being processed.
    current_batches:
        HashMap<ScheduledPacketBatchId, (Arc<ScheduledPacketBatch>, BankPacketProcessingDecision)>,
    building_batches_tracker: TransactionBatchesTracker,
    metrics: SchedulerMetrics,
}

struct TransactionBatchesTracker {
    /// Batches that are currently being built, indexed by execution thread index
    building_batches: BinaryHeap<TransactionBatchBuilder>,
    /// Stats on the currently pending batches and queued transactions
    /// for each execution thread.
    execution_thread_stats: Vec<ExecutionThreadStats>,
    id_generator: ScheduledPacketBatchIdGenerator,
}

impl TransactionBatchesTracker {
    pub fn new(num_execution_threads: usize) -> Self {
        let mut id_generator = ScheduledPacketBatchIdGenerator::default();
        Self {
            building_batches: (0..num_execution_threads)
                .into_iter()
                .map(|execution_thread_index| {
                    let id = id_generator.generate_id();
                    TransactionBatchBuilder::new(id, execution_thread_index)
                })
                .collect(),
            execution_thread_stats: (0..num_execution_threads)
                .into_iter()
                .map(|execution_thread_index| ExecutionThreadStats::new())
                .collect(),
            id_generator,
        }
    }

    /// Checks if there are any batches currently being built by the scheduler
    pub fn has_batch_being_built(&self) -> bool {
        !self.building_batches.is_empty()
    }

    /// Adds a packet to the batch with lowest queued CU. Returns batch builder if it should be sent for execution
    pub fn add_deserialized_packet(
        &mut self,
        deserialized_packet: Arc<ImmutableDeserializedPacket>,
    ) -> Option<TransactionBatchBuilder> {
        assert!(self.has_batch_being_built());
        let mut builder = self.building_batches.pop().unwrap();
        builder.add_deserialized_packet(deserialized_packet);
        self.should_batch_be_sent(builder)
    }

    /// Adds a packet to the batch on the given execution thread. Returns batch builder if it should be sent for execution
    pub fn add_deserialized_packet_to_thread(
        &mut self,
        deserialized_packet: Arc<ImmutableDeserializedPacket>,
        execution_thread_index: usize,
    ) -> Option<TransactionBatchBuilder> {
        // TODO: this seems slow, maybe we can do better
        let builders: Vec<_> = self.building_batches.drain().collect();

        let mut target_builder = None;
        for mut builder in builders {
            if builder.execution_thread_index == execution_thread_index {
                // store this batch to add the packet to
                assert!(target_builder.is_none());
                builder.add_deserialized_packet(deserialized_packet.clone());
                target_builder = Some(builder);
            } else {
                // just push this one back in
                self.building_batches.push(builder);
            }
        }

        let target_builder = target_builder.unwrap();
        self.should_batch_be_sent(target_builder)
    }

    /// Updates tracking and stats for a completed batch
    pub fn complete_batch(&mut self, batch: &ScheduledPacketBatch) {
        let id = batch.id;
        let execution_thread_index = batch.execution_thread_index;

        let stats = &mut self.execution_thread_stats[execution_thread_index];
        assert_eq!(id, stats.queued_batches.pop_front().unwrap()); // check processed in correct order

        // if we aren't currently building a batch for this thread, we can start to build another
        // now that we have the capacity for it
        if !stats.has_batch_being_built {
            self.add_new_batch_builder(execution_thread_index);
        }
    }

    /// Checks if a builder should be sent. If so, return it, otherwise push back in and return None
    fn should_batch_be_sent(
        &mut self,
        builder: TransactionBatchBuilder,
    ) -> Option<TransactionBatchBuilder> {
        // If the batch should be sent, we will return it
        if builder.deserialized_packets.len() == MAX_BATCH_SIZE
            || builder.start_time.elapsed() > MAX_BATCH_AGE
        {
            let execution_thread_index = builder.execution_thread_index;
            self.execution_thread_stats[execution_thread_index].has_batch_being_built = false;

            // If we have room for another batch, add a new builder
            if self.execution_thread_stats[execution_thread_index]
                .queued_batches
                .len()
                < MAX_QUEUED_BATCHES - 1
            {
                self.add_new_batch_builder(execution_thread_index);
            }
            Some(builder)
        } else {
            // if we aren't sending it, push it back in
            self.building_batches.push(builder);
            None
        }
    }

    /// Add a new batch for `execution_thread_index`
    fn add_new_batch_builder(&mut self, execution_thread_index: usize) {
        let id = self.id_generator.generate_id();
        self.execution_thread_stats[execution_thread_index].has_batch_being_built = true;
        self.building_batches
            .push(TransactionBatchBuilder::new(id, execution_thread_index));
    }
}

#[derive(Clone)]
/// A handle to the central scheduler channels
pub struct CentralNonConflictingSchedulerBankingHandle {
    /// Receiver for getting batches of transactions from the scheduler
    scheduled_packet_batch_receiver: Receiver<Arc<ScheduledPacketBatch>>,
    /// Sender for sending processed batches of transactions to the scheduler
    processed_packet_batch_sender: Sender<ProcessedPacketBatch>,
}

/// Handle to the scheduler thread
pub struct CentralNonConflictingSchedulerThreadHandle {
    scheduler_thread: std::thread::JoinHandle<()>,
}

impl CentralNonConflictingSchedulerThreadHandle {
    pub fn join(self) -> std::thread::Result<()> {
        self.scheduler_thread.join()
    }
}

impl TransactionSchedulerBankingHandle for CentralNonConflictingSchedulerBankingHandle {
    fn get_next_transaction_batch(
        &mut self,
        timeout: Duration,
    ) -> Result<Arc<ScheduledPacketBatch>, RecvTimeoutError> {
        self.scheduled_packet_batch_receiver.recv_timeout(timeout)
    }

    fn complete_batch(&mut self, batch: ProcessedPacketBatch) {
        self.processed_packet_batch_sender.send(batch).unwrap(); // TODO: return an error here
    }

    fn join(self) -> std::thread::Result<()> {
        Ok(())
    }
}

impl<D> CentralNonConflictingScheduler<D>
where
    D: DeserializedPacketBatchGetter + Send + 'static,
{
    /// Spawn a scheduler thread and return a handle to it
    pub fn spawn(
        num_execution_threads: usize,
        deserialized_packet_batch_getter: D,
        bank_forks: Arc<RwLock<BankForks>>,
        banking_decision_maker: Arc<BankingDecisionMaker>,
        capacity: usize,
    ) -> (
        Vec<CentralNonConflictingSchedulerBankingHandle>,
        CentralNonConflictingSchedulerThreadHandle,
    ) {
        let (processed_packet_batch_sender, processed_packet_batch_receiver) =
            crossbeam_channel::bounded(num_execution_threads * MAX_QUEUED_BATCHES);

        let (scheduled_packet_batch_senders, scheduled_packet_batch_receivers) =
            Self::create_channels(num_execution_threads);

        let scheduler_thread = Builder::new()
            .name("solCtrlSchd".to_string())
            .spawn(move || {
                let mut scheduler = Self::new(
                    deserialized_packet_batch_getter,
                    scheduled_packet_batch_senders,
                    processed_packet_batch_receiver,
                    bank_forks,
                    banking_decision_maker,
                    capacity,
                );
                scheduler.run();
                error!("Scheduler thread exited");
            })
            .unwrap();

        (
            scheduled_packet_batch_receivers
                .into_iter()
                .map(|scheduled_packet_batch_receiver| {
                    CentralNonConflictingSchedulerBankingHandle {
                        scheduled_packet_batch_receiver,
                        processed_packet_batch_sender: processed_packet_batch_sender.clone(),
                    }
                })
                .collect(),
            CentralNonConflictingSchedulerThreadHandle { scheduler_thread },
        )
    }

    /// Create vec of crossbeam channels separated into senders and receivers
    fn create_channels<T>(num_execution_threads: usize) -> (Vec<Sender<T>>, Vec<Receiver<T>>) {
        let mut senders = Vec::with_capacity(num_execution_threads);
        let mut receivers = Vec::with_capacity(num_execution_threads);
        for _ in 0..num_execution_threads {
            let (sender, receiver) = crossbeam_channel::bounded(MAX_QUEUED_BATCHES);
            senders.push(sender);
            receivers.push(receiver);
        }
        (senders, receivers)
    }

    /// Create a new scheduler
    fn new(
        deserialized_packet_batch_getter: D,
        scheduled_packet_batch_senders: Vec<Sender<Arc<ScheduledPacketBatch>>>,
        processed_packet_batch_receiver: Receiver<ProcessedPacketBatch>,
        bank_forks: Arc<RwLock<BankForks>>,
        banking_decision_maker: Arc<BankingDecisionMaker>,
        capacity: usize,
    ) -> Self {
        let num_execution_threads = scheduled_packet_batch_senders.len();

        Self {
            deserialized_packet_batch_getter,
            scheduled_packet_batch_senders,
            processed_packet_batch_receiver,
            held_packets: Vec::new(),
            root_bank_cache: RootBankCache::new(bank_forks),
            forward_filter: None,
            banking_decision_maker: banking_decision_maker,
            transaction_queue: TransactionQueue::with_capacity(capacity),
            current_batches: HashMap::new(),
            metrics: SchedulerMetrics::default(),
            building_batches_tracker: TransactionBatchesTracker::new(num_execution_threads),
        }
    }

    /// Run the scheduler loop
    fn run(&mut self) {
        let mut prev_decision = self.banking_decision_maker.make_decision();
        loop {
            // Potentially receive packets
            let timeout = if self.transaction_queue.tracking_map.is_empty() {
                Duration::from_millis(100)
            } else {
                Duration::from_millis(0)
            };
            // const PACKET_RECV_TIMEOUT: Duration = Duration::from_millis(10);
            let recv_result = self.receive_and_buffer_packets(timeout);
            if matches!(recv_result, Err(RecvTimeoutError::Disconnected)) {
                break;
            }

            // Potentially receive processed batches
            const PROCESSED_BATCH_RECV_TIMEOUT: Duration = Duration::from_millis(10);
            let (recv_results, receive_completed_batches_time) = measure!(drain_channel(
                &self.processed_packet_batch_receiver,
                PROCESSED_BATCH_RECV_TIMEOUT
            ));
            let (_, complete_batches_time) = measure!({
                for processed_batch in recv_results {
                    self.complete_batch(processed_batch);
                }
            });
            saturating_add_assign!(
                self.metrics.receive_completed_batch_time_us,
                receive_completed_batches_time.as_us()
            );
            saturating_add_assign!(
                self.metrics.complete_batches_time_us,
                complete_batches_time.as_us()
            );

            // Get the next transaction batches
            let (decision, decision_making_time) =
                measure!(self.banking_decision_maker.make_decision());
            saturating_add_assign!(
                self.metrics.decision_making_time_us,
                decision_making_time.as_us()
            );

            if decision != prev_decision {
                self.move_held_packets();
                self.clear_partially_built_batches(&prev_decision);
            }
            prev_decision = decision;

            // always move these on consume, since we also use `held_packets` to store
            // packets that were blocked by lower priority transactions.
            if matches!(decision, BankPacketProcessingDecision::Consume(_)) {
                self.move_held_packets();
            }

            let (_, scheduling_time) = measure!({
                const SCHEDULE_BATCHES_TIMEOUT: Duration = Duration::from_millis(100);
                let start = Instant::now();
                self.do_scheduling(decision, &start, &SCHEDULE_BATCHES_TIMEOUT);
            });
            saturating_add_assign!(self.metrics.scheduling_time_us, scheduling_time.as_us());
            self.metrics.max_blocked_packets = self
                .metrics
                .max_blocked_packets
                .max(self.transaction_queue.num_blocked_packets);

            self.metrics.report(1000);
        }
    }

    /// Get the current root bank
    /// Note: This is blocking, should be used only when necessary
    fn get_current_bank(&mut self) -> Arc<Bank> {
        let (bank, bank_lock_time) = measure!(self.root_bank_cache.get_root_bank());
        saturating_add_assign!(self.metrics.bank_lock_time_us, bank_lock_time.as_us());
        bank
    }

    /// Do scheduling
    fn do_scheduling(
        &mut self,
        decision: BankPacketProcessingDecision,
        start: &Instant,
        timeout: &Duration,
    ) {
        if self.transaction_queue.pending_transactions.is_empty() {
            return;
        }

        while self.do_scheduling_iter(decision) && start.elapsed() < *timeout {}

        // if we're out of packets, just send out what we've built
        // if self.transaction_queue.pending_transactions.is_empty() {
        self.send_building_batches(decision);
        // }
    }

    /// Send out all batches that are currently being built
    fn send_building_batches(&mut self, decision: BankPacketProcessingDecision) {
        let builders: Vec<_> = self
            .building_batches_tracker
            .building_batches
            .drain()
            .collect();

        for builder in builders {
            if !builder.deserialized_packets.is_empty() {
                // build and send out the batch
                let execution_thread_index = builder.execution_thread_index;
                self.send_batch(builder, decision);

                if self.building_batches_tracker.execution_thread_stats[execution_thread_index]
                    .queued_batches
                    .len()
                    < MAX_QUEUED_BATCHES
                {
                    self.building_batches_tracker
                        .add_new_batch_builder(execution_thread_index);
                } else {
                    self.building_batches_tracker.execution_thread_stats[execution_thread_index]
                        .has_batch_being_built = false;
                }
            } else {
                // just push this one back in
                self.building_batches_tracker.building_batches.push(builder);
            }
        }
    }

    /// Return true if there is more scheduling to do
    fn do_scheduling_iter(&mut self, decision: BankPacketProcessingDecision) -> bool {
        if !self.building_batches_tracker.has_batch_being_built() {
            self.building_batches_tracker
                .execution_thread_stats
                .iter()
                .for_each(|stats| assert_eq!(stats.queued_batches.len(), MAX_QUEUED_BATCHES));

            return false;
        }
        if self.transaction_queue.pending_transactions.is_empty() {
            return false;
        }

        if let Some((transaction, maybe_thread_index)) = self.try_get_next_transaction(&decision) {
            let next_packet = self.transaction_queue.get_immutable_section(&transaction);
            if let Some(batch_to_send) = if let Some(execution_thread_index) = maybe_thread_index {
                let batch_builder = self.building_batches_tracker
                    .building_batches
                    .iter()
                    .find(|batch_builder| {
                        batch_builder.execution_thread_index == execution_thread_index
                    }).expect("should have found a batch builder. This is verified in `try_get_next_packet`");

                let batch_id = batch_builder.batch_id;
                self.transaction_queue
                    .lock_for_transaction(&transaction, batch_id);
                self.building_batches_tracker
                    .add_deserialized_packet_to_thread(next_packet, execution_thread_index)
            } else {
                let batch_builder = self
                    .building_batches_tracker
                    .building_batches
                    .peek()
                    .unwrap();
                let batch_id = batch_builder.batch_id;
                self.transaction_queue
                    .lock_for_transaction(&transaction, batch_id);
                self.building_batches_tracker
                    .add_deserialized_packet(next_packet)
            } {
                self.send_batch(batch_to_send, decision);
            }
        }

        return true;
    }

    /// Try to get the next packet - if it can only be scheduled to a specific thread then return as well
    fn try_get_next_transaction(
        &mut self,
        decision: &BankPacketProcessingDecision,
    ) -> Option<(TransactionRef, Option<usize>)> {
        match decision {
            BankPacketProcessingDecision::Consume(_) => {
                while let Some(transaction) = self.transaction_queue.get_next_pending_transaction()
                {
                    let (conflict_set, building_conflict_set) =
                        self.get_conflict_sets(&transaction);

                    if !building_conflict_set.can_schedule_to_any_thread() {
                        // if we have any conflicts with a batch that is currently being built, then we can't schedule this transaction
                        // to any thread. So we can block it by the minimum blocking transaction.
                        // Try to block by the minimum blocking transaction. If there is no minimum blocking transaction, because lower
                        // priority transactions were queued up, then we push into a temporary holding queue until the next iteration.
                        // if self
                        //     .transaction_queue
                        //     .can_schedule_transaction(&transaction)
                        // {
                        //     self.held_packets.push(transaction);
                        // }
                        self.held_packets.push(transaction);
                        continue;
                    }

                    if conflict_set.is_schedulable() {
                        let conflict_thread_index = conflict_set.get_conflicting_thread_index();
                        if let Some(execution_thread_index) = conflict_thread_index {
                            if !self.building_batches_tracker.execution_thread_stats
                                [execution_thread_index]
                                .has_batch_being_built
                            {
                                // Inside this if, we have a conflict with a thread that has no batch being built.
                                //     this means we've queued up all batches for this thread, and would need to wait for the
                                //     next batch to finish before we can schedule this one.
                                // For now, we'll just mark this tx as blocked by a tx in the first batch on the thread
                                // TODO: Consider just having a "blocked_by_queue" container
                                // if self
                                //     .transaction_queue
                                //     .can_schedule_transaction(&transaction)
                                // {
                                //     let blocking_batch_id = self
                                //         .building_batches_tracker
                                //         .execution_thread_stats[execution_thread_index]
                                //         .queued_batches
                                //         .iter()
                                //         .next()
                                //         .unwrap();
                                //     let blocking_transaction = self
                                //         .current_batches
                                //         .get(blocking_batch_id)
                                //         .unwrap()
                                //         .0
                                //         .deserialized_packets
                                //         .first()
                                //         .unwrap();

                                //     self.transaction_queue.block_transaction(
                                //         *blocking_transaction.message_hash(),
                                //         transaction,
                                //     );
                                // }
                                self.held_packets.push(transaction);
                                continue; // try scheduling another packet
                            }
                        }

                        return Some((transaction, conflict_thread_index));
                    } else {
                        // We have conficts with multiple threads, so we can't schedule this transaction to any thread.
                        // Try to block by the minimum blocking transaction. If there is no minimum blocking transaction, because lower
                        // priority transactions were queued up, then we push into a temporary holding queue until the next iteration.
                        // if self
                        //     .transaction_queue
                        //     .can_schedule_transaction(&transaction)
                        // {
                        //     self.held_packets.push(transaction);
                        // }
                        self.held_packets.push(transaction);
                    }
                }

                None
            }
            BankPacketProcessingDecision::Forward
            | BankPacketProcessingDecision::ForwardAndHold => match self.forward_filter {
                Some(ref mut forward_filter) => self
                    .transaction_queue
                    .try_get_next_forward_transaction(forward_filter)
                    .map(|t| (t, None)), // forward packets can be scheduled to any thread
                None => unreachable!(),
            },
            BankPacketProcessingDecision::Hold => None, // do nothing
        }
    }

    fn get_conflict_sets(
        &self,
        transaction: &SanitizedTransactionPriority,
    ) -> (ConflictSet, ConflictSet) {
        let mut conflict_set = ConflictSet::default();
        let mut inflight_conflict_set = ConflictSet::default();

        let mut check_batches = |batch_ids| {
            for batch_id in batch_ids {
                if let Some((batch, _)) = self.current_batches.get(batch_id) {
                    conflict_set.mark_conflict(batch.execution_thread_index);
                } else if let Some(batch) = self
                    .building_batches_tracker
                    .building_batches
                    .iter()
                    .find(|b| b.batch_id == *batch_id)
                {
                    inflight_conflict_set.mark_conflict(batch.execution_thread_index);
                    break;
                }
            }
        };

        // Build a set of conflicting thread indexes for the transaction
        // TODO: Exit early if we find an inflight conflict? - borrow-checker is not happy because mutable borrow
        let account_locks = transaction.transaction.get_account_locks_unchecked();
        for account in account_locks.readonly {
            let account_queue = self.transaction_queue.account_queues.get(account).unwrap();
            check_batches(account_queue.scheduled_lock.write.batch_ids.iter());
        }

        for account in account_locks.writable {
            let account_queue = self.transaction_queue.account_queues.get(account).unwrap();
            check_batches(account_queue.scheduled_lock.write.batch_ids.iter());
            check_batches(account_queue.scheduled_lock.read.batch_ids.iter());
        }

        (conflict_set, inflight_conflict_set)
    }

    /// Build and send a batch given a batch builder
    fn send_batch(
        &mut self,
        batch_builder: TransactionBatchBuilder,
        decision: BankPacketProcessingDecision,
    ) {
        let id = batch_builder.batch_id;
        let execution_thread_index = batch_builder.execution_thread_index;
        let execution_thread_stats =
            &mut self.building_batches_tracker.execution_thread_stats[execution_thread_index];

        execution_thread_stats.queued_batches.push_back(id);
        let batch = Arc::new(batch_builder.build(decision.into()));
        self.current_batches.insert(id, (batch.clone(), decision));

        saturating_add_assign!(
            self.metrics.num_packets_scheduled,
            batch.deserialized_packets.len()
        );
        saturating_add_assign!(
            self.metrics.num_packets_by_thread[execution_thread_index],
            1
        );
        if matches!(decision, BankPacketProcessingDecision::Consume(_)) {
            saturating_add_assign!(
                self.metrics.num_packets_scheduled_consume,
                batch.deserialized_packets.len()
            );
        }
        saturating_add_assign!(self.metrics.num_batches_scheduled, 1);

        // Unblock transactions that were blocked by the transactions in this batch
        //     - Now that these are being sent out, we can schedule blocked transactions
        //       because they'd be queued behind them if they conflict.
        for transaction in batch.deserialized_packets.iter() {
            let message_hash = transaction.message_hash();
            let transaction = self
                .transaction_queue
                .tracking_map
                .get(&message_hash)
                .unwrap()
                .0
                .clone();
            self.transaction_queue.unblock_transaction(&transaction);
        }

        assert!(!self.scheduled_packet_batch_senders[batch.execution_thread_index].is_full());
        self.metrics.max_batch_size = self
            .metrics
            .max_batch_size
            .max(batch.deserialized_packets.len());
        self.metrics.max_batch_size_by_thread[execution_thread_index] =
            self.metrics.max_batch_size_by_thread[execution_thread_index]
                .max(batch.deserialized_packets.len());
        self.metrics.max_queued_batches_by_thread[execution_thread_index] =
            self.metrics.max_queued_batches_by_thread[execution_thread_index].max(
                self.building_batches_tracker.execution_thread_stats[execution_thread_index]
                    .queued_batches
                    .len(),
            );
        self.scheduled_packet_batch_senders[batch.execution_thread_index]
            .send(batch)
            .unwrap();
    }

    /// Move held packets back into the queues
    fn move_held_packets(&mut self) {
        for transaction in self.held_packets.drain(..) {
            assert!(self
                .transaction_queue
                .tracking_map
                .get(transaction.message_hash())
                .is_some());
            self.transaction_queue
                .insert_transaction_into_pending_queue(&transaction);
        }
    }

    /// Clear building batches because the banking decision changed before they were sent out
    fn clear_partially_built_batches(&mut self, decision: &BankPacketProcessingDecision) {
        let should_unlock = matches!(decision, BankPacketProcessingDecision::Consume(_));

        let builders: Vec<_> = self
            .building_batches_tracker
            .building_batches
            .drain()
            .collect();

        for mut builder in builders {
            saturating_add_assign!(
                self.metrics.num_packets_unscheduled,
                builder.deserialized_packets.len()
            );

            for packet in builder.deserialized_packets.drain(..) {
                let transaction = self
                    .transaction_queue
                    .tracking_map
                    .get(packet.message_hash())
                    .unwrap()
                    .0
                    .clone();
                if should_unlock {
                    self.transaction_queue
                        .remove_account_locks_transaction(&transaction, builder.batch_id);
                }
                self.transaction_queue.unblock_transaction(&transaction);
                self.transaction_queue
                    .insert_transaction_into_pending_queue(&transaction);
            }
            builder.compute_units = 0;
            self.building_batches_tracker.building_batches.push(builder);
        }

        saturating_add_assign!(
            self.metrics.num_batches_cleared,
            self.building_batches_tracker.building_batches.len()
        );
    }

    /// Complete the processing of a batch of transactions. This function will remove the transactions
    /// from tracking and unblock any transactions that were waiting on the results of these.
    fn complete_batch(&mut self, batch: ProcessedPacketBatch) {
        let (current_batch, decision) = self
            .current_batches
            .remove(&batch.id)
            .expect("completed batch was not in current batches map");

        let num_packets = current_batch.deserialized_packets.len();
        let num_retries = (batch.retryable_packets.count_ones() as usize).min(num_packets);
        let num_success = num_packets - num_retries;

        saturating_add_assign!(self.metrics.num_batches_completed, 1);
        saturating_add_assign!(self.metrics.num_packets_retried, num_retries);
        saturating_add_assign!(self.metrics.num_packets_success, num_success);

        if !matches!(decision, BankPacketProcessingDecision::Consume(_)) {
            panic!("what am i forwarding...?");
        }

        self.building_batches_tracker.complete_batch(&current_batch);

        match decision {
            BankPacketProcessingDecision::Consume(_) | BankPacketProcessingDecision::Forward => {
                current_batch
                    .deserialized_packets
                    .iter()
                    .enumerate()
                    .for_each(|(index, packet)| {
                        let retry = (batch.retryable_packets & (1 << index)) != 0;
                        self.transaction_queue.complete_or_retry(
                            packet,
                            retry,
                            batch.id,
                            &mut self.metrics.max_completed_packet_age_us,
                        );
                    });
            }
            BankPacketProcessingDecision::ForwardAndHold => {
                current_batch
                    .deserialized_packets
                    .iter()
                    .enumerate()
                    .for_each(|(index, packet)| {
                        let retry = (batch.retryable_packets & (1 << index)) != 0;
                        if !retry {
                            self.transaction_queue
                                .mark_forwarded(packet, &mut self.held_packets);
                        } else {
                            panic!("shouldn't fail to forward");
                        }
                    });
            }
            BankPacketProcessingDecision::Hold => {
                panic!("Should never have a Hold batch complete");
            }
        }
    }

    /// Receive and buffer packets from sigverify stage
    fn receive_and_buffer_packets(&mut self, timeout: Duration) -> Result<(), RecvTimeoutError> {
        let (deserialized_packets, receive_packet_batches_time) = measure!(self
            .deserialized_packet_batch_getter
            .get_deserialized_packets(timeout, self.transaction_queue.remaining_capacity())?);

        saturating_add_assign!(self.metrics.num_packets_seen, deserialized_packets.len());
        saturating_add_assign!(
            self.metrics.receive_packet_batches_time_us,
            receive_packet_batches_time.as_us()
        );

        let bank = self.get_current_bank();
        let (_, insert_new_packets_time) = measure!({
            for packet in deserialized_packets {
                self.insert_new_packet(packet, &bank);
            }
        });
        saturating_add_assign!(
            self.metrics.insert_new_packets_time_us,
            insert_new_packets_time.as_us()
        );

        Ok(())
    }

    /// Insert a new packet into the scheduler
    fn insert_new_packet(&mut self, packet: ImmutableDeserializedPacket, bank: &Bank) {
        if self
            .transaction_queue
            .tracking_map
            .contains_key(packet.message_hash())
        {
            // error!(
            //     "ignoring packet already in tracking map: {:?}",
            //     packet.message_hash()
            // );
            // {
            //     error!(
            //         "pending: {:#?}",
            //         self.transaction_queue.pending_transactions
            //     );
            //     error!(
            //         "blocked: {:#?}",
            //         self.transaction_queue.blocked_transactions
            //     );
            //     panic!("shouldn't have duplicate packets right now");
            // }

            return;
        }

        if let Some(transaction) = SanitizedTransactionPriority::try_new(&packet, bank) {
            self.transaction_queue.insert_transaction(
                Rc::new(transaction),
                DeserializedPacket::from_immutable_section(packet),
                bank,
            );
        } else {
            error!(
                "ignoring packet that failed sanitization: {:?}",
                packet.message_hash()
            );
        }
    }
}

/// Queue structure for ordering transactions by priority without conflict.
struct TransactionQueue {
    /// Pending transactions that are not known to be blocked. Ordered by priority.
    pending_transactions: MinMaxHeap<TransactionRef>,
    /// Transaction queues and locks by account key
    account_queues: HashMap<Pubkey, AccountTransactionQueue>,
    /// Current number of blocked packets
    num_blocked_packets: usize,
    /// Map from message hash to transactions blocked by by that transaction
    blocked_transactions: HashMap<Hash, Vec<TransactionRef>>,
    /// Map from message hash transaction and packet
    tracking_map: HashMap<Hash, (TransactionRef, DeserializedPacket)>,
}

impl TransactionQueue {
    /// Create a new transaction queue with capacity
    fn with_capacity(capacity: usize) -> Self {
        Self {
            pending_transactions: MinMaxHeap::with_capacity(capacity),
            account_queues: HashMap::with_capacity(capacity.saturating_div(4)),
            num_blocked_packets: 0,
            blocked_transactions: HashMap::new(),
            tracking_map: HashMap::with_capacity(capacity),
        }
    }

    /// Get the next packet for forwarding
    fn try_get_next_forward_transaction(
        &mut self,
        forward_filter: &mut ForwardPacketBatchesByAccounts,
    ) -> Option<TransactionRef> {
        let next_transaction = self.get_next_pending_transaction()?;
        let next_packet = self.get_immutable_section(&next_transaction);
        if forward_filter.add_packet(next_packet) {
            Some(next_transaction)
        } else {
            todo!("dropping packet on forward filter")
        }
    }

    /// Get immutable section from the wrapped sanitized transaction priorty
    fn get_immutable_section(
        &self,
        transaction: &SanitizedTransactionPriority,
    ) -> Arc<ImmutableDeserializedPacket> {
        self.tracking_map
            .get(transaction.message_hash())
            .unwrap()
            .1
            .immutable_section()
            .clone()
    }

    /// Check if a transaction can be scheduled. If it cannot, add it to the blocked transactions
    fn can_schedule_transaction(&mut self, transaction: &TransactionRef) -> bool {
        match self.get_lowest_priority_blocking_transaction(transaction) {
            Some(blocking_transaction) => {
                self.block_transaction(*blocking_transaction.message_hash(), transaction.clone());
                false
            }
            None => true,
        }
    }

    /// Gets the lowest priority transaction that blocks this one
    fn get_lowest_priority_blocking_transaction(
        &self,
        transaction: &TransactionRef,
    ) -> Option<TransactionRef> {
        let account_locks = transaction.transaction.get_account_locks_unchecked();
        let min_blocking_transaction = account_locks
            .readonly
            .into_iter()
            .map(|account_key| {
                self.account_queues
                    .get(account_key)
                    .unwrap()
                    .get_min_blocking_transaction(transaction, false)
            })
            .fold(None, option_min);
        account_locks
            .writable
            .into_iter()
            .map(|account_key| {
                self.account_queues
                    .get(account_key)
                    .unwrap()
                    .get_min_blocking_transaction(transaction, true)
            })
            .fold(min_blocking_transaction, option_min)
            .cloned()
    }

    /// Lock all accounts for a transaction
    fn lock_for_transaction(
        &mut self,
        transaction: &TransactionRef,
        batch_id: ScheduledPacketBatchId,
    ) {
        let account_locks = transaction.transaction.get_account_locks_unchecked();

        for account in account_locks.readonly {
            self.account_queues
                .get_mut(account)
                .unwrap()
                .handle_schedule_transaction(false, batch_id);
        }

        for account in account_locks.writable {
            self.account_queues
                .get_mut(account)
                .unwrap()
                .handle_schedule_transaction(true, batch_id);
        }
    }

    // Get the next pending transaction from pending queue that does not exceed max age
    fn get_next_pending_transaction(&mut self) -> Option<TransactionRef> {
        // while let Some(transaction) = self.pending_transactions.pop_max() {
        //     // TODO: this shouldn't be 1s
        //     if transaction.timestamp.elapsed() < Duration::from_millis(400) {
        //         return Some(transaction);
        //     } else {
        //         self.remove_transaction(&transaction, false, ScheduledPacketBatchId::default());
        //     }
        // }

        // None
        self.pending_transactions.pop_max()
    }

    /// Insert a new transaction into the queue(s) and maps
    fn insert_transaction(
        &mut self,
        transaction: TransactionRef,
        packet: DeserializedPacket,
        bank: &Bank,
    ) {
        let already_exists = self
            .tracking_map
            .insert(
                *packet.immutable_section().message_hash(),
                (transaction.clone(), packet),
            )
            .is_some();
        assert!(!already_exists);

        self.insert_transaction_into_account_queues(&transaction, bank);
        self.insert_transaction_into_pending_queue(&transaction);
    }

    /// Insert a transaction into the account queues
    fn insert_transaction_into_account_queues(
        &mut self,
        transaction: &TransactionRef,
        bank: &Bank,
    ) {
        let account_locks = transaction.get_account_locks().unwrap();

        for account in account_locks.readonly {
            let account_queue = self.account_queues.entry(*account).or_default();
            account_queue.insert_transaction(transaction.clone(), false);
        }

        for account in account_locks.writable {
            let account_queue = self.account_queues.entry(*account).or_default();
            account_queue.insert_transaction(transaction.clone(), true);
        }
    }

    /// Insert a transaction into the pending queue
    fn insert_transaction_into_pending_queue(&mut self, transaction: &TransactionRef) {
        if self.remaining_capacity() > 0 {
            self.pending_transactions.push(transaction.clone());
        } else {
            panic!("dropped a packet");
            let dropped_packet = self.pending_transactions.push_pop_min(transaction.clone());
            // error!("dropping packet: {:?}", dropped_packet.message_hash());
            self.remove_transaction(&dropped_packet, false, ScheduledPacketBatchId::default());
        }
    }

    /// Remove a transaction from the queue(s) and maps
    ///     - This will happen if a transaction is completed or dropped
    ///     - The transaction should already be removed from the pending queue
    fn remove_transaction(
        &mut self,
        transaction: &TransactionRef,
        is_scheduled: bool,
        batch_id: ScheduledPacketBatchId,
    ) {
        let message_hash = transaction.message_hash();
        let packet = self
            .tracking_map
            .remove(message_hash)
            .expect("Transaction should exist in tracking map");

        self.remove_transaction_from_account_queues(&transaction, is_scheduled, batch_id);
        self.unblock_transaction(&transaction);
    }

    /// Remove a transaction from account queues
    fn remove_transaction_from_account_queues(
        &mut self,
        transaction: &TransactionRef,
        is_scheduled: bool,
        batch_id: ScheduledPacketBatchId,
    ) {
        // We got account locks with checks when the transaction was initially inserted. No need to rerun checks.
        let account_locks = transaction.transaction.get_account_locks_unchecked();

        for account in account_locks.readonly {
            if self
                .account_queues
                .get_mut(account)
                .expect("account should exist in account queues")
                .remove_transaction(transaction, false, is_scheduled, batch_id)
            {
                self.account_queues.remove(account);
            }
        }

        for account in account_locks.writable {
            if self
                .account_queues
                .get_mut(account)
                .expect("account should exist in account queues")
                .remove_transaction(transaction, true, is_scheduled, batch_id)
            {
                self.account_queues.remove(account);
            }
        }
    }

    /// Block a transaction by another
    fn block_transaction(
        &mut self,
        blocking_transaction_hash: Hash,
        blocked_transaction: TransactionRef,
    ) {
        self.blocked_transactions
            .entry(blocking_transaction_hash)
            .or_default()
            .push(blocked_transaction);
        saturating_add_assign!(self.num_blocked_packets, 1);
    }

    /// Unblock transactions blocked by a transaction
    fn unblock_transaction(&mut self, transaction: &TransactionRef) {
        let message_hash = transaction.message_hash();
        if let Some(blocked_transactions) = self.blocked_transactions.remove(message_hash) {
            self.num_blocked_packets -= blocked_transactions.len();
            for blocked_transaction in blocked_transactions {
                assert!(self
                    .tracking_map
                    .contains_key(blocked_transaction.message_hash()));
                self.insert_transaction_into_pending_queue(&blocked_transaction);
            }
        }
    }

    /// Unlocks all accounts for a transaction
    fn remove_account_locks_transaction(
        &mut self,
        transaction: &TransactionRef,
        batch_id: ScheduledPacketBatchId,
    ) {
        let account_locks = transaction.transaction.get_account_locks_unchecked();

        for account in account_locks.readonly {
            self.account_queues
                .get_mut(account)
                .unwrap()
                .scheduled_lock
                .unlock(false, batch_id);
        }

        for account in account_locks.writable {
            self.account_queues
                .get_mut(account)
                .unwrap()
                .scheduled_lock
                .unlock(true, batch_id);
        }
    }

    /// Mark a transaction as complete or retry
    fn complete_or_retry(
        &mut self,
        packet: &ImmutableDeserializedPacket,
        retry: bool,
        batch_id: ScheduledPacketBatchId,
        max_completed_packet_age_us: &mut u64,
    ) {
        let message_hash = packet.message_hash();
        let (transaction, deserialized_packet) = self
            .tracking_map
            .get(message_hash)
            .expect("Transaction should exist in tracking map");
        let transaction = transaction.clone();

        if retry {
            self.remove_account_locks_transaction(&transaction, batch_id);
            self.unblock_transaction(&transaction);
            self.insert_transaction_into_pending_queue(&transaction);
        } else {
            self.remove_transaction(&transaction, true, batch_id);
            *max_completed_packet_age_us = (*max_completed_packet_age_us)
                .max(transaction.timestamp.elapsed().as_micros() as u64);
        }
    }

    /// Mark a transaction as forwarded
    fn mark_forwarded(
        &mut self,
        packet: &ImmutableDeserializedPacket,
        held_packets: &mut Vec<Rc<SanitizedTransactionPriority>>,
    ) {
        let message_hash = packet.message_hash();
        let (transaction, deserialized_packet) = self
            .tracking_map
            .get_mut(message_hash)
            .expect("forwarded packet should exist in tracking map");
        deserialized_packet.forwarded = true;
        held_packets.push(transaction.clone());
    }

    /// Returns the remaining capacity of the pending queue
    fn remaining_capacity(&self) -> usize {
        self.pending_transactions
            .capacity()
            .saturating_sub(self.pending_transactions.len())
    }
}

#[derive(Default)]
/// Tracks all pending and blocked transactions for a given account, ordered by priority.
struct AccountTransactionQueue {
    /// Tree of read transacitons on the account ordered by fee-priority
    reads: BTreeSet<TransactionRef>,
    /// Tree of write transactions on the account ordered by fee-priority
    writes: BTreeSet<TransactionRef>,
    /// Tracks currently scheduled transactions on the account
    scheduled_lock: AccountLock,
}

impl AccountTransactionQueue {
    /// Insert a transaction into the queue.
    fn insert_transaction(&mut self, transaction: TransactionRef, is_write: bool) {
        if is_write {
            &mut self.writes
        } else {
            &mut self.reads
        }
        .insert(transaction);
    }

    /// Apply account locks for `transaction`
    fn handle_schedule_transaction(&mut self, is_write: bool, batch_id: ScheduledPacketBatchId) {
        self.scheduled_lock.lock(is_write, batch_id);
    }

    /// Remove transaction from the queue whether on completion or being dropped.
    ///
    /// Returns true if there are no remaining transactions in this account's queue.
    fn remove_transaction(
        &mut self,
        transaction: &TransactionRef,
        is_write: bool,
        is_scheduled: bool,
        batch_id: ScheduledPacketBatchId,
    ) -> bool {
        // Remove from appropriate tree
        if is_write {
            assert!(self.writes.remove(transaction));
        } else {
            assert!(self.reads.remove(transaction));
        }

        // Unlock
        if is_scheduled {
            self.scheduled_lock.unlock(is_write, batch_id);
        }

        // No remaining locks, nothing in the trees
        !self.scheduled_lock.write_locked()
            && !self.scheduled_lock.read_locked()
            && self.writes.len() == 0
            && self.reads.len() == 0
    }

    /// Find the minimum priority transaction that blocks this transaction if there is one.
    fn get_min_blocking_transaction<'a>(
        &'a self,
        transaction: &TransactionRef,
        is_write: bool,
    ) -> Option<&'a TransactionRef> {
        let mut min_blocking_transaction = None;

        if is_write {
            min_blocking_transaction = option_min(
                min_blocking_transaction,
                upper_bound(&self.reads, transaction.clone()),
                // self.scheduled_lock.get_lowest_priority_transaction(false), // blocked by lowest-priority read or write
            );
        }

        min_blocking_transaction = option_min(
            min_blocking_transaction,
            upper_bound(&self.writes, transaction.clone()),
            // self.scheduled_lock.get_lowest_priority_transaction(true), // blocked by lowest-priority write
        );

        min_blocking_transaction
    }
}

/// Tracks the number of outstanding write/read locks and the lowest priority
#[derive(Debug, Default)]
struct AccountLock {
    write: AccountLockInner,
    read: AccountLockInner,
}

impl AccountLock {
    fn lock(&mut self, is_write: bool, batch_id: ScheduledPacketBatchId) {
        let inner = if is_write {
            &mut self.write
        } else {
            &mut self.read
        };
        inner.lock(batch_id);
    }

    fn unlock(&mut self, is_write: bool, batch_id: ScheduledPacketBatchId) {
        let inner = if is_write {
            &mut self.write
        } else {
            &mut self.read
        };
        inner.unlock(batch_id);
    }

    fn write_locked(&self) -> bool {
        self.write.count > 0
    }

    fn read_locked(&self) -> bool {
        self.read.count > 0
    }
}

#[derive(Debug, Default)]
struct AccountLockInner {
    /// Number of outstanding locks
    count: usize,
    /// Batch Ids that are currently locking this account
    batch_ids: HashSet<ScheduledPacketBatchId>,
}

impl AccountLockInner {
    fn lock(&mut self, batch_id: ScheduledPacketBatchId) {
        self.count += 1;
        self.batch_ids.insert(batch_id); // read-locks could be inserted multiple times, so can't assert on insert
    }

    fn unlock(&mut self, batch_id: ScheduledPacketBatchId) {
        assert!(self.count > 0);
        self.count -= 1;
        self.batch_ids.remove(&batch_id); // read-locks could be inserted multiple times, so can't assert on remove
    }
}

/// Helper function to get the lowest-priority blocking transaction
fn upper_bound<'a, T: Ord>(tree: &'a BTreeSet<T>, item: T) -> Option<&'a T> {
    use std::ops::Bound::*;
    let mut iter = tree.range((Excluded(item), Unbounded));
    iter.next()
}

/// Helper function to compare options, but None is not considered less than
fn option_min<T: Ord>(lhs: Option<T>, rhs: Option<T>) -> Option<T> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(std::cmp::min(lhs, rhs)),
        (lhs, None) => lhs,
        (None, rhs) => rhs,
    }
}

/// Helper function to drain a channel into a vector
fn drain_channel<T>(channel: &Receiver<T>, timeout: Duration) -> Vec<T> {
    let start = Instant::now();
    let mut vec = vec![];
    while let Ok(item) = channel.try_recv() {
        vec.push(item);
        if start.elapsed() >= timeout {
            break;
        }
    }
    vec
}

/// A builder for transaction batches
#[derive(Debug, PartialEq, Eq)]
struct TransactionBatchBuilder {
    /// Timestamp of the batch starting to be built
    start_time: Instant,
    /// Transactions in the batch
    deserialized_packets: Vec<Arc<ImmutableDeserializedPacket>>,
    /// Queued compute-units
    compute_units: u64,
    /// Batch Id to identify the batch
    batch_id: ScheduledPacketBatchId,
    /// Thread index to be sent to
    execution_thread_index: usize,
}

impl TransactionBatchBuilder {
    fn new(batch_id: ScheduledPacketBatchId, execution_thread_index: usize) -> Self {
        Self {
            start_time: Instant::now(),
            deserialized_packets: Vec::with_capacity(MAX_BATCH_SIZE),
            compute_units: 0,
            batch_id,
            execution_thread_index,
        }
    }

    fn add_deserialized_packet(&mut self, deserialized_packet: Arc<ImmutableDeserializedPacket>) {
        // set the time when first packet is added
        if self.deserialized_packets.is_empty() {
            self.start_time = Instant::now();
        }

        saturating_add_assign!(self.compute_units, deserialized_packet.compute_unit_limit());
        self.deserialized_packets.push(deserialized_packet);
    }

    fn build(self, processing_instruction: BankingProcessingInstruction) -> ScheduledPacketBatch {
        ScheduledPacketBatch {
            id: self.batch_id,
            processing_instruction,
            deserialized_packets: self.deserialized_packets,
            execution_thread_index: self.execution_thread_index,
        }
    }
}

impl Ord for TransactionBatchBuilder {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // sort by queued compute-units first - sort so smaller batches are "larger"
        other
            .compute_units
            .cmp(&self.compute_units)
            .then_with(|| {
                // sort by time so that older batches are "larger"
                other.start_time.cmp(&self.start_time)
            })
            .then_with(|| {
                // smaller threads first
                other
                    .execution_thread_index
                    .cmp(&self.execution_thread_index)
            })
    }
}

impl PartialOrd for TransactionBatchBuilder {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Track stats for the execution threads
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ExecutionThreadStats {
    /// Currently queue batch ids
    queued_batches: VecDeque<ScheduledPacketBatchId>,
    /// Has batch being built
    has_batch_being_built: bool,
}

impl ExecutionThreadStats {
    fn new() -> Self {
        Self {
            queued_batches: VecDeque::with_capacity(MAX_QUEUED_BATCHES),
            has_batch_being_built: true, // each thread starts with batch being built
        }
    }
}

#[derive(Default)]
struct SchedulerMetrics {
    last_report: AtomicInterval,

    // Packet-wise metrics
    num_packets_seen: usize,
    num_packets_scheduled: usize,
    num_packets_scheduled_consume: usize,
    num_packets_unscheduled: usize, // due to decision change
    num_packets_retried: usize,
    num_packets_success: usize,
    max_blocked_packets: usize,

    // Batch-wise metrics
    num_batches_scheduled: usize,
    num_batches_completed: usize,
    num_batches_cleared: usize,
    max_batch_size: usize,

    // Thread-wise metrics
    num_packets_by_thread: [usize; 4],
    max_batch_size_by_thread: [usize; 4],
    max_queued_batches_by_thread: [usize; 4],

    // Timing metrics
    bank_lock_time_us: u64,
    receive_packet_batches_time_us: u64,
    insert_new_packets_time_us: u64,
    receive_completed_batch_time_us: u64,
    complete_batches_time_us: u64,
    decision_making_time_us: u64,
    scheduling_time_us: u64,
    max_completed_packet_age_us: u64,
}

impl SchedulerMetrics {
    fn report<'a>(&mut self, interval_ms: u64) {
        if self.last_report.should_update(interval_ms) {
            datapoint_info!(
                "tx-scheduler",
                ("num_packets_seen", self.num_packets_seen, i64),
                ("num_packets_scheduled", self.num_packets_scheduled, i64),
                (
                    "num_packets_scheduled_consume",
                    self.num_packets_scheduled_consume,
                    i64
                ),
                ("num_packets_unscheduled", self.num_packets_unscheduled, i64),
                ("num_packets_retried", self.num_packets_retried, i64),
                ("num_packets_success", self.num_packets_success, i64),
                ("max_blocked_packets", self.max_blocked_packets, i64),
                ("num_batches_scheduled", self.num_batches_scheduled, i64),
                ("num_batches_completed", self.num_batches_completed, i64),
                ("num_batches_cleared", self.num_batches_cleared, i64),
                ("max_batch_size", self.max_batch_size, i64),
                ("bank_lock_time_us", self.bank_lock_time_us, i64),
                (
                    "recieve_packet_batches_time_us",
                    self.receive_packet_batches_time_us,
                    i64
                ),
                (
                    "insert_new_packets_time_us",
                    self.insert_new_packets_time_us,
                    i64
                ),
                (
                    "receive_completed_batch_time_us",
                    self.receive_completed_batch_time_us,
                    i64
                ),
                (
                    "complete_batches_time_us",
                    self.complete_batches_time_us,
                    i64
                ),
                ("decision_making_time_us", self.decision_making_time_us, i64),
                ("scheduling_time_us", self.scheduling_time_us, i64),
                (
                    "max_completed_packet_age_us",
                    self.max_completed_packet_age_us,
                    i64
                ),
                ("num_packets_0", self.num_packets_by_thread[0], i64),
                ("num_packets_1", self.num_packets_by_thread[1], i64),
                ("num_packets_2", self.num_packets_by_thread[2], i64),
                ("num_packets_3", self.num_packets_by_thread[3], i64),
                ("max_batch_size_0", self.max_batch_size_by_thread[0], i64),
                ("max_batch_size_1", self.max_batch_size_by_thread[1], i64),
                ("max_batch_size_2", self.max_batch_size_by_thread[2], i64),
                ("max_batch_size_3", self.max_batch_size_by_thread[3], i64),
                (
                    "max_queued_batches_0",
                    self.max_queued_batches_by_thread[0],
                    i64
                ),
                (
                    "max_queued_batches_1",
                    self.max_queued_batches_by_thread[1],
                    i64
                ),
                (
                    "max_queued_batches_2",
                    self.max_queued_batches_by_thread[2],
                    i64
                ),
                (
                    "max_queued_batches_3",
                    self.max_queued_batches_by_thread[3],
                    i64
                ),
            );
            *self = Self::default();
        }
    }
}

/// Caches the root bank and provides an interface for getting the root bank from bank_forks
/// but only locking if the root bank has been updated since the last time the root bank was
/// fetched.
pub struct RootBankCache {
    bank_forks: Arc<RwLock<BankForks>>,
    root_slot: ReadOnlyAtomicSlot,
    root_bank: Arc<Bank>,
}

impl RootBankCache {
    pub fn new(bank_forks: Arc<RwLock<BankForks>>) -> Self {
        let (root_slot, root_bank) = {
            let lock = bank_forks.read().unwrap();
            (lock.get_atomic_root(), lock.root_bank().clone())
        };
        Self {
            bank_forks,
            root_slot,
            root_bank,
        }
    }

    pub fn get_root_bank(&mut self) -> Arc<Bank> {
        let root_slot = self.root_slot.get();
        if root_slot != self.root_bank.slot() {
            let lock = self.bank_forks.read().unwrap();
            let root_bank = lock.root_bank().clone();
            self.root_bank = root_bank;
        }
        self.root_bank.clone()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            CentralNonConflictingScheduler, SanitizedTransactionPriority, TransactionBatchBuilder,
        },
        crate::{
            bank_process_decision::BankingDecisionMaker,
            packet_deserializer_stage::InlinePacketDeserializer,
            transaction_scheduler::ScheduledPacketBatchId,
        },
        solana_ledger::blockstore::Blockstore,
        solana_poh::poh_recorder::{create_test_recorder, PohRecorder},
        solana_runtime::{bank::Bank, bank_forks::BankForks},
        solana_sdk::{
            hash::Hash, pubkey::Pubkey, signature::Keypair, signer::Signer,
            transaction::SanitizedTransaction,
        },
        std::{
            sync::{Arc, RwLock},
            time::{Duration, Instant},
        },
    };

    fn create_transfer(from_keypair: &Keypair, to_pubkey: &Pubkey) -> SanitizedTransaction {
        let tx =
            solana_sdk::system_transaction::transfer(from_keypair, to_pubkey, 1, Hash::default());
        SanitizedTransaction::from_transaction_for_tests(tx)
    }

    fn create_simple_transaction() -> SanitizedTransaction {
        let keypair = solana_sdk::signature::Keypair::new();
        let pubkey = keypair.pubkey();
        create_transfer(&keypair, &pubkey)
    }

    // fn create_transaction_scheduler(num_execution_threads: usize, capacity: usize) -> CentralNonConflictingScheduler<InlinePacketDeserializer> {
    //     // Create a simple inline deserializer - won't actually be used.
    //     let (_packet_sender, packet_receiver) = crossbeam_channel::unbounded();
    //     let deserializer = InlinePacketDeserializer::new(packet_receiver, 0);

    //     let (batch_senders, batch_receivers) = CentralNonConflictingScheduler::create_channels(num_execution_threads);
    //     let (processed_batch_sender, processed_batch_receiver) = crossbeam_channel::unbounded();

    //     let banking_decision_maker = BankingDecisionMaker::new(, Pubkey::new_unique());
    //     let bank_forks = Arc::new(RwLock::new(BankForks::new(Bank::default_for_tests())));
    //     let poh_recorder = Arc::new(RwLock::new(create_test_recorder(&bank_forks.read().unwrap().root_bank(), Arc::new(Blockstore::))));

    //     let scheduler = CentralNonConflictingScheduler::new(deserializer, batch_senders, processed_batch_receiver, bank_forks, banking_decision_maker, capcity)
    // }

    // #[test]
    // fn transaction_scheduler_single_thread() {

    // }

    #[test]
    fn transaction_batch_builder_ordering_by_compute_units() {
        let now = Instant::now();

        let b1 = TransactionBatchBuilder {
            start_time: now,
            deserialized_packets: vec![],
            compute_units: 0,
            execution_thread_index: 0,
            batch_id: ScheduledPacketBatchId::new(0),
        };
        let b2 = TransactionBatchBuilder {
            start_time: now,
            deserialized_packets: vec![],
            compute_units: 1,
            execution_thread_index: 0,
            batch_id: ScheduledPacketBatchId::new(1),
        };
        assert!(b1 > b2); // fewer compute units -> higher priority in the binary heap
    }

    #[test]
    fn transaction_batch_builder_ordering_by_age() {
        let now = Instant::now();

        let b1 = TransactionBatchBuilder {
            start_time: now,
            deserialized_packets: vec![],
            compute_units: 0,
            execution_thread_index: 0,
            batch_id: ScheduledPacketBatchId::new(0),
        };
        let b2 = TransactionBatchBuilder {
            start_time: now + Duration::from_millis(5),
            deserialized_packets: vec![],
            compute_units: 0,
            execution_thread_index: 0,
            batch_id: ScheduledPacketBatchId::new(1),
        };
        assert!(b1 > b2); // older batch is prioritized
    }

    #[test]
    fn transaction_batch_builder_ordering_by_thread_index() {
        let now = Instant::now();

        let b1 = TransactionBatchBuilder {
            start_time: now,
            deserialized_packets: vec![],
            compute_units: 0,
            execution_thread_index: 0,
            batch_id: ScheduledPacketBatchId::new(0),
        };
        let b2 = TransactionBatchBuilder {
            start_time: now,
            deserialized_packets: vec![],
            compute_units: 0,
            execution_thread_index: 1,
            batch_id: ScheduledPacketBatchId::new(1),
        };
        assert!(b1 > b2); // smaller thread index is prioritized
    }

    #[test]
    fn transaction_priority_ordering_by_priority() {
        let now = Instant::now();
        let stx = create_simple_transaction();

        let tx1 = SanitizedTransactionPriority {
            priority: 1,
            transaction: stx.clone(),
            timestamp: now,
        };
        let tx2 = SanitizedTransactionPriority {
            priority: 0,
            transaction: stx,
            timestamp: now,
        };
        assert!(tx1 > tx2); // higher priority is prioritized
    }

    #[test]
    fn transaction_priority_ordering_by_age() {
        let now = Instant::now();
        let stx = create_simple_transaction();

        let tx1 = SanitizedTransactionPriority {
            priority: 0,
            transaction: stx.clone(),
            timestamp: now + Duration::from_millis(5),
        };
        let tx2 = SanitizedTransactionPriority {
            priority: 0,
            transaction: stx,
            timestamp: now,
        };
        assert!(tx1 > tx2); // older tx is prioritized
    }
}
