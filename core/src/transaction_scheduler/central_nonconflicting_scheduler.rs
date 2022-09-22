//! Implements a transaction scheduler that queues up non-conflicting batches of transactions
//! for banking threads to process. Design based on: https://github.com/solana-labs/solana/pull/26362
//!

use {
    super::{
        ProcessedPacketBatch, ScheduledPacketBatch, ScheduledPacketBatchId,
        ScheduledPacketBatchIdGenerator, TransactionSchedulerBankingHandle,
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
        collections::{BTreeSet, HashMap},
        fmt::Display,
        rc::Rc,
        sync::{atomic::Ordering, Arc, RwLock},
        thread::{current, Builder},
        time::{Duration, Instant},
    },
};

const MAX_BATCH_SIZE: usize = 128;

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
    /// Sender for sending batches of transactions to banking stage
    scheduled_packet_batch_sender: Sender<Arc<ScheduledPacketBatch>>,
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
    /// Generator for unique batch identifiers.
    batch_id_generator: ScheduledPacketBatchIdGenerator,
    metrics: SchedulerMetrics,
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
        deserialized_packet_batch_getter: D,
        bank_forks: Arc<RwLock<BankForks>>,
        banking_decision_maker: Arc<BankingDecisionMaker>,
        capacity: usize,
    ) -> (
        CentralNonConflictingSchedulerBankingHandle,
        CentralNonConflictingSchedulerThreadHandle,
    ) {
        let (scheduled_packet_batch_sender, scheduled_packet_batch_receiver) =
            crossbeam_channel::unbounded();
        let (processed_packet_batch_sender, processed_packet_batch_receiver) =
            crossbeam_channel::unbounded();

        let scheduler_thread = Builder::new()
            .name("solCtrlSchd".to_string())
            .spawn(move || {
                let mut scheduler = Self::new(
                    deserialized_packet_batch_getter,
                    scheduled_packet_batch_sender,
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
            CentralNonConflictingSchedulerBankingHandle {
                scheduled_packet_batch_receiver,
                processed_packet_batch_sender,
            },
            CentralNonConflictingSchedulerThreadHandle { scheduler_thread },
        )
    }

    /// Create a new scheduler
    fn new(
        deserialized_packet_batch_getter: D,
        scheduled_packet_batch_sender: Sender<Arc<ScheduledPacketBatch>>,
        processed_packet_batch_receiver: Receiver<ProcessedPacketBatch>,
        bank_forks: Arc<RwLock<BankForks>>,
        banking_decision_maker: Arc<BankingDecisionMaker>,
        capacity: usize,
    ) -> Self {
        Self {
            deserialized_packet_batch_getter,
            scheduled_packet_batch_sender,
            processed_packet_batch_receiver,
            held_packets: Vec::new(),
            root_bank_cache: RootBankCache::new(bank_forks),
            forward_filter: None,
            banking_decision_maker: banking_decision_maker,
            transaction_queue: TransactionQueue::with_capacity(capacity),
            current_batches: HashMap::new(),
            batch_id_generator: ScheduledPacketBatchIdGenerator::default(),
            metrics: SchedulerMetrics::default(),
        }
    }

    /// Run the scheduler loop
    fn run(&mut self) {
        const RECV_TIMEOUT: Duration = Duration::from_millis(10);
        loop {
            // Potentially receive packets
            let recv_result = self.receive_and_buffer_packets(RECV_TIMEOUT);
            if matches!(recv_result, Err(RecvTimeoutError::Disconnected)) {
                break;
            }

            // Potentially receive processed batches
            let (recv_results, receive_completed_batches_time) = measure!(drain_channel(
                &self.processed_packet_batch_receiver,
                RECV_TIMEOUT
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
            let (_, scheduling_time) = measure!({
                while let Some(batch) = self.get_next_transaction_batch(decision) {
                    let send_result = self.scheduled_packet_batch_sender.send(batch);
                    if send_result.is_err() {
                        return;
                    }
                }
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

    /// Get the next batch of transactions to be processed by banking stage
    fn get_next_transaction_batch(
        &mut self,
        decision: BankPacketProcessingDecision,
    ) -> Option<Arc<ScheduledPacketBatch>> {
        match decision {
            BankPacketProcessingDecision::Consume(_) => {
                self.forward_filter = None;
                self.move_held_packets();
                let deserialized_packets = self.transaction_queue.get_consume_batch();
                deserialized_packets.map(|deserialized_packets| {
                    self.create_scheduled_batch(deserialized_packets, decision)
                })
            }
            BankPacketProcessingDecision::Forward
            | BankPacketProcessingDecision::ForwardAndHold => {
                // Take the forwarding filter (will replace at the end of the function)
                let current_bank = self.get_current_bank();
                let mut forward_filter = match self.forward_filter.take() {
                    Some(mut forward_filter) => {
                        forward_filter.current_bank = current_bank;
                        forward_filter
                    }
                    None => {
                        ForwardPacketBatchesByAccounts::new_with_default_batch_limits(current_bank)
                    }
                };

                let deserialized_packets = self
                    .transaction_queue
                    .get_forwarding_batch(&mut forward_filter);

                // Move the forward filter back into the scheduler for the next iteration
                self.forward_filter = Some(forward_filter);

                deserialized_packets.map(|deserialized_packets| {
                    self.create_scheduled_batch(deserialized_packets, decision)
                })
            }
            BankPacketProcessingDecision::Hold => {
                self.forward_filter = None;
                None
            }
        }
    }

    /// Create scheduled batch from deserialized packets and decision. Insert into the current
    /// batches map.
    fn create_scheduled_batch(
        &mut self,
        deserialized_packets: Vec<Arc<ImmutableDeserializedPacket>>,
        decision: BankPacketProcessingDecision,
    ) -> Arc<ScheduledPacketBatch> {
        saturating_add_assign!(self.metrics.num_batches_scheduled, 1);
        saturating_add_assign!(
            self.metrics.num_packets_scheduled,
            deserialized_packets.len()
        );

        let id = self.batch_id_generator.generate_id();
        let scheduled_batch = Arc::new(ScheduledPacketBatch {
            id,
            processing_instruction: decision.clone().into(),
            deserialized_packets,
        });
        self.current_batches
            .insert(id, (scheduled_batch.clone(), decision));
        scheduled_batch
    }

    /// Move held packets back into the queues
    fn move_held_packets(&mut self) {
        for transaction in self.held_packets.drain(..) {
            self.transaction_queue
                .insert_transaction_into_pending_queue(&transaction);
        }
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

        match decision {
            BankPacketProcessingDecision::Consume(_) | BankPacketProcessingDecision::Forward => {
                current_batch
                    .deserialized_packets
                    .iter()
                    .enumerate()
                    .for_each(|(index, packet)| {
                        let retry = (batch.retryable_packets & (1 << index)) != 0;
                        self.transaction_queue.complete_or_retry(packet, retry);
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
            error!(
                "ignoring packet already in tracking map: {:?}",
                packet.message_hash()
            );
            {
                error!(
                    "pending: {:#?}",
                    self.transaction_queue.pending_transactions
                );
                error!(
                    "blocked: {:#?}",
                    self.transaction_queue.blocked_transactions
                );
                panic!("shouldn't have duplicate packets right now");
            }

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

    /// Get a batch of transactions to be consumed by banking stage
    fn get_consume_batch(&mut self) -> Option<Vec<Arc<ImmutableDeserializedPacket>>> {
        let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
        while let Some(transaction) = self.pending_transactions.pop_max() {
            if self.can_schedule_transaction(&transaction) {
                batch.push(transaction);
                if batch.len() == MAX_BATCH_SIZE {
                    break;
                }
            }
        }

        if batch.len() > 0 {
            self.lock_batch(&batch);
            Some(
                batch
                    .into_iter()
                    .map(|transaction| {
                        self.tracking_map
                            .get(transaction.message_hash())
                            .unwrap()
                            .1
                            .immutable_section()
                            .clone()
                    })
                    .collect(),
            )
        } else {
            None
        }
    }

    /// Check if a transaction can be scheduled. If it cannot, add it to the blocked transactions
    fn can_schedule_transaction(&mut self, transaction: &TransactionRef) -> bool {
        let maybe_blocking_transaction = self.get_lowest_priority_blocking_transaction(transaction);
        if let Some(blocking_transaction) = maybe_blocking_transaction {
            self.blocked_transactions
                .entry(*blocking_transaction.message_hash())
                .or_default()
                .push(transaction.clone());
            saturating_add_assign!(self.num_blocked_packets, 1);
            false
        } else {
            true
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

    /// Lock a batch of transactions
    fn lock_batch(&mut self, batch: &[TransactionRef]) {
        for transaction in batch {
            self.lock_for_transaction(transaction);
        }
    }

    /// Lock all accounts for a transaction
    fn lock_for_transaction(&mut self, transaction: &TransactionRef) {
        let account_locks = transaction.transaction.get_account_locks_unchecked();

        for account in account_locks.readonly {
            self.account_queues
                .get_mut(account)
                .unwrap()
                .handle_schedule_transaction(transaction, false);
        }

        for account in account_locks.writable {
            self.account_queues
                .get_mut(account)
                .unwrap()
                .handle_schedule_transaction(transaction, true);
        }
    }

    /// Get a batch of transactions to be forwarded by banking stage
    fn get_forwarding_batch(
        &mut self,
        forward_filter: &mut ForwardPacketBatchesByAccounts,
    ) -> Option<Vec<Arc<ImmutableDeserializedPacket>>> {
        // Get batch of transaction simply by priority, and insert into the forwarding filter
        let mut batch = Vec::with_capacity(self.pending_transactions.len().min(MAX_BATCH_SIZE));
        while let Some(transaction) = self.pending_transactions.pop_max() {
            let packet = self
                .tracking_map
                .get(transaction.message_hash())
                .unwrap()
                .1
                .immutable_section()
                .clone();
            if forward_filter.add_packet(packet.clone()) {
                batch.push(packet);
                if batch.len() == MAX_BATCH_SIZE {
                    break;
                }
            } else {
                // drop it?
                panic!("forwarding filter is full - probably should drop, not sure yet.");
            }
        }
        (batch.len() > 0).then(|| batch)
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
            let dropped_packet = self.pending_transactions.push_pop_min(transaction.clone());
            error!("dropping packet: {:?}", dropped_packet.message_hash());
            self.remove_transaction(&dropped_packet);
        }
    }

    /// Remove a transaction from the queue(s) and maps
    ///     - This will happen if a transaction is completed or dropped
    ///     - The transaction should already be removed from the pending queue
    fn remove_transaction(&mut self, transaction: &TransactionRef) {
        let message_hash = transaction.message_hash();
        let packet = self
            .tracking_map
            .remove(message_hash)
            .expect("Transaction should exist in tracking map");

        self.remove_transaction_from_account_queues(&transaction);
        self.unblock_transaction(&transaction);
    }

    /// Remove a transaction from account queues
    fn remove_transaction_from_account_queues(&mut self, transaction: &TransactionRef) {
        // We got account locks with checks when the transaction was initially inserted. No need to rerun checks.
        let account_locks = transaction.transaction.get_account_locks_unchecked();

        for account in account_locks.readonly {
            if self
                .account_queues
                .get_mut(account)
                .expect("account should exist in account queues")
                .remove_transaction(transaction, false)
            {
                self.account_queues.remove(account);
            }
        }

        for account in account_locks.writable {
            if self
                .account_queues
                .get_mut(account)
                .expect("account should exist in account queues")
                .remove_transaction(transaction, true)
            {
                self.account_queues.remove(account);
            }
        }
    }

    /// Unblock transactions blocked by a transaction
    fn unblock_transaction(&mut self, transaction: &TransactionRef) {
        let message_hash = transaction.message_hash();
        if let Some(blocked_transactions) = self.blocked_transactions.remove(message_hash) {
            self.num_blocked_packets = self.num_blocked_packets - blocked_transactions.len();
            for blocked_transaction in blocked_transactions {
                self.insert_transaction_into_pending_queue(&blocked_transaction);
            }
        }
    }

    /// Unlocks all accounts for a transaction
    fn remove_account_locks_transaction(&mut self, transaction: &TransactionRef) {
        let account_locks = transaction.transaction.get_account_locks_unchecked();

        for account in account_locks.readonly {
            self.account_queues
                .get_mut(account)
                .unwrap()
                .scheduled_lock
                .unlock_on_transaction(transaction, false);
        }

        for account in account_locks.writable {
            self.account_queues
                .get_mut(account)
                .unwrap()
                .scheduled_lock
                .unlock_on_transaction(transaction, true);
        }
    }

    /// Mark a transaction as complete or retry
    fn complete_or_retry(&mut self, packet: &ImmutableDeserializedPacket, retry: bool) {
        let message_hash = packet.message_hash();
        let (transaction, deserialized_packet) = self
            .tracking_map
            .get(message_hash)
            .expect("Transaction should exist in tracking map");
        let transaction = transaction.clone();

        if retry {
            self.remove_account_locks_transaction(&transaction);
            self.unblock_transaction(&transaction);
            self.insert_transaction_into_pending_queue(&transaction);
        } else {
            self.remove_transaction(&transaction);
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
    fn handle_schedule_transaction(&mut self, transaction: &TransactionRef, is_write: bool) {
        self.scheduled_lock
            .lock_on_transaction(&transaction, is_write);
    }

    /// Remove transaction from the queue whether on completion or being dropped.
    ///
    /// Returns true if there are no remaining transactions in this account's queue.
    fn remove_transaction(&mut self, transaction: &TransactionRef, is_write: bool) -> bool {
        // Remove from appropriate tree
        if is_write {
            assert!(self.writes.remove(transaction));
        } else {
            assert!(self.reads.remove(transaction));
        }

        // Unlock
        self.scheduled_lock
            .unlock_on_transaction(transaction, is_write);

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
                self.scheduled_lock.get_lowest_priority_transaction(false), // blocked by lowest-priority read or write
            );
        }

        min_blocking_transaction = option_min(
            min_blocking_transaction,
            self.scheduled_lock.get_lowest_priority_transaction(true), // blocked by lowest-priority write
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
    fn lock_on_transaction(&mut self, transaction: &TransactionRef, is_write: bool) {
        let inner = if is_write {
            &mut self.write
        } else {
            &mut self.read
        };
        inner.lock_for_transaction(transaction);
    }

    fn unlock_on_transaction(&mut self, transaction: &TransactionRef, is_write: bool) {
        let inner = if is_write {
            &mut self.write
        } else {
            &mut self.read
        };
        inner.unlock_for_transaction(transaction);
    }

    fn write_locked(&self) -> bool {
        self.write.count > 0
    }

    fn read_locked(&self) -> bool {
        self.read.count > 0
    }

    fn get_lowest_priority_transaction(&self, is_write: bool) -> Option<&TransactionRef> {
        let inner = if is_write { &self.write } else { &self.read };
        inner.lowest_priority_transaction.as_ref()
    }
}

#[derive(Debug, Default)]
struct AccountLockInner {
    count: usize,
    lowest_priority_transaction: Option<TransactionRef>,
}

impl AccountLockInner {
    fn lock_for_transaction(&mut self, transaction: &TransactionRef) {
        self.count += 1;

        match self.lowest_priority_transaction.as_ref() {
            Some(tx) => {
                if transaction.cmp(tx).is_lt() {
                    self.lowest_priority_transaction = Some(transaction.clone());
                }
            }
            None => self.lowest_priority_transaction = Some(transaction.clone()),
        }
    }

    fn unlock_for_transaction(&mut self, transaction: &TransactionRef) {
        assert!(self.count > 0);
        self.count -= 1;

        // This works because we are scheduling by priority order.
        // So the lowest priority transaction scheduled is guaranteed to finish last
        if self.count == 0 {
            self.lowest_priority_transaction = None;
        }
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

#[derive(Default)]
struct SchedulerMetrics {
    last_report: AtomicInterval,

    // Packet-wise metrics
    num_packets_seen: usize,
    num_packets_scheduled: usize,
    num_packets_retried: usize,
    num_packets_success: usize,
    max_blocked_packets: usize,

    // Batch-wise metrics
    num_batches_scheduled: usize,
    num_batches_completed: usize,

    // Timing metrics
    bank_lock_time_us: u64,
    receive_packet_batches_time_us: u64,
    insert_new_packets_time_us: u64,
    receive_completed_batch_time_us: u64,
    complete_batches_time_us: u64,
    decision_making_time_us: u64,
    scheduling_time_us: u64,
}

impl SchedulerMetrics {
    fn report<'a>(&mut self, interval_ms: u64) {
        if self.last_report.should_update(interval_ms) {
            datapoint_info!(
                "tx-scheduler",
                ("num_packets_seen", self.num_packets_seen, i64),
                ("num_packets_scheduled", self.num_packets_scheduled, i64),
                ("num_packets_retried", self.num_packets_retried, i64),
                ("num_packets_success", self.num_packets_success, i64),
                ("num_batches_scheduled", self.num_batches_scheduled, i64),
                ("num_batches_completed", self.num_batches_completed, i64),
                ("max_blocked_packets", self.max_blocked_packets, i64),
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
