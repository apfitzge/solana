//! Implements a transaction scheduler

use {
    crate::unprocessed_packet_batches::{self, ImmutableDeserializedPacket},
    crossbeam_channel::{select, Receiver, Sender},
    solana_measure::measure,
    solana_perf::packet::PacketBatch,
    solana_runtime::bank::Bank,
    solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::SanitizedTransaction},
    std::{
        collections::{BTreeSet, BinaryHeap, HashMap},
        hash::Hash,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::JoinHandle,
        time::Instant,
    },
};
/// Wrapper to store a sanitized transaction and priority
#[derive(Clone, Debug)]
pub struct TransactionPriority {
    /// Transaction priority
    priority: u64,
    /// Sanitized transaction
    transaction: SanitizedTransaction,
    /// Timestamp the scheduler received the transaction - only used for ordering
    timestamp: Instant,
}

impl Ord for TransactionPriority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.priority.cmp(&other.priority) {
            std::cmp::Ordering::Equal => match self
                .transaction
                .message_hash()
                .cmp(other.transaction.message_hash())
            {
                std::cmp::Ordering::Equal => self.timestamp.cmp(&other.timestamp),
                ordering => ordering,
            },
            ordering => ordering,
        }
    }
}

impl PartialOrd for TransactionPriority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TransactionPriority {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
            && self.transaction.message_hash() == other.transaction.message_hash()
            && self.timestamp == other.timestamp
    }
}

impl Eq for TransactionPriority {}

impl Hash for TransactionPriority {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.transaction.signature().hash(state);
    }
}

type TransactionRef = Arc<TransactionPriority>;

impl TransactionPriority {
    fn try_new(packet: &ImmutableDeserializedPacket, bank: &Bank) -> Option<TransactionRef> {
        let priority = packet.priority();
        let transaction = SanitizedTransaction::try_new(
            packet.transaction().clone(),
            *packet.message_hash(),
            packet.is_simple_vote(),
            bank,
        )
        .ok()?;
        transaction.verify_precompiles(&bank.feature_set).ok()?;
        Some(Arc::new(Self {
            transaction,
            priority,
            timestamp: Instant::now(),
        }))
    }
}

type PacketBatchMessage = Vec<PacketBatch>;
type TransactionMessage = TransactionRef;
type TransactionBatchMessage = Vec<TransactionMessage>;

/// Stores state for scheduling transactions and channels for communicating
/// with other threads: SigVerify and Banking
pub struct TransactionScheduler {
    /// Channel for receiving deserialized packet batches from SigVerify
    packet_batch_receiver: Receiver<PacketBatchMessage>,
    /// Channels for sending transaction batches to banking threads
    transaction_batch_senders: Vec<Sender<TransactionBatchMessage>>,
    /// Channel for receiving completed transactions from any banking thread
    completed_transaction_receiver: Receiver<TransactionMessage>,
    /// Bank that we are currently scheduling for
    bank: Arc<Bank>,
    /// Max number of transactions to send to a single banking-thread in a batch
    max_batch_size: usize,
    /// Exit signal
    exit: Arc<AtomicBool>,

    /// Pending transactions that are not known to be blocked
    pending_transactions: BinaryHeap<TransactionRef>,
    /// Transaction queues and locks by account key
    transactions_by_account: HashMap<Pubkey, AccountTransactionQueue>,
    /// Map from transaction signature to transactions blocked by the signature
    blocked_transactions: HashMap<Signature, Vec<TransactionRef>>,
    /// Tracks the current number of blocked transactions
    num_blocked_transactions: usize,
    /// Tracks the current number of executing transacitons
    num_executing_transactions: usize,

    /// Track metrics for scheduler thread
    metrics: SchedulerMetrics,
}

impl TransactionScheduler {
    /// Create and start transaction scheduler thread
    pub fn spawn_scheduler(
        packet_batch_receiver: Receiver<PacketBatchMessage>,
        transaction_batch_senders: Vec<Sender<TransactionBatchMessage>>,
        completed_transaction_receiver: Receiver<TransactionMessage>,
        bank: Arc<Bank>,
        max_batch_size: usize,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let scheduler = TransactionScheduler {
            packet_batch_receiver,
            transaction_batch_senders,
            completed_transaction_receiver,
            bank,
            max_batch_size,
            exit,
            pending_transactions: BinaryHeap::default(),
            transactions_by_account: HashMap::default(),
            blocked_transactions: HashMap::default(),
            num_blocked_transactions: 0,
            num_executing_transactions: 0,
            metrics: SchedulerMetrics::default(),
        };

        std::thread::spawn(move || scheduler.main())
    }

    /// Driving loop
    fn main(mut self) {
        loop {
            if self.exit.load(Ordering::Relaxed) {
                break;
            }
            self.iter();
        }
    }

    /// Performs work in a loop - Handles different channel receives/timers and performs scheduling
    fn iter(&mut self) {
        select! {
            recv(self.completed_transaction_receiver) -> maybe_completed_tx => {
                let (_, completed_transaction_time) = measure!(
                    if let Ok(completed_tx) = maybe_completed_tx {
                        self.handle_completed_transaction(completed_tx);
                    }
                );
                self.metrics.completed_transactions_time_us += completed_transaction_time.as_us();
            }
            recv(self.packet_batch_receiver) -> maybe_tx_batch_message => {
                let (_, packet_batch_time) = measure!({
                    if let Ok(packet_batch_message) = maybe_tx_batch_message {
                        self.handle_packet_batches(packet_batch_message);
                    }
                });
                self.metrics.packet_batch_time_us += packet_batch_time.as_us();
            }
            default() => {
                let (_, scheduling_time) = measure!(self.do_scheduling());
                self.metrics.scheduling_time_us += scheduling_time.as_us();
            }
        }

        self.metrics.report();
    }

    /// Handles packet batches as we receive them from the channel
    fn handle_packet_batches(&mut self, packet_batch_message: PacketBatchMessage) {
        for packet_batch in packet_batch_message {
            let (packet_indices, filter_time) = measure!({
                packet_batch
                    .into_iter()
                    .enumerate()
                    .filter_map(|(idx, p)| if !p.meta.discard() { Some(idx) } else { None })
                    .collect::<Vec<_>>()
            });
            let (transactions, deserialize_time) = measure!(
                unprocessed_packet_batches::deserialize_packets(&packet_batch, &packet_indices)
                    .filter_map(|deserialized_packet| {
                        TransactionPriority::try_new(
                            deserialized_packet.immutable_section(),
                            &self.bank,
                        )
                    })
                    .collect::<Vec<_>>()
            );
            let (_, insert_time) = measure!({
                for transaction in transactions {
                    self.insert_transaction(transaction);
                }
            });

            self.metrics.packet_batch_filter_time_us += filter_time.as_us();
            self.metrics.packet_batch_deserialize_time_us += deserialize_time.as_us();
            self.metrics.packet_batch_insert_time_us += insert_time.as_us();
        }

        self.metrics.max_pending_transactions = self
            .metrics
            .max_pending_transactions
            .max(self.pending_transactions.len());
    }

    /// Handle completed transactions
    fn handle_completed_transaction(&mut self, transaction: TransactionMessage) {
        let (_, update_queues_time) =
            measure!(self.update_queues_on_completed_transaction(&transaction));
        let (_, unblock_transactions_time) =
            measure!(self.push_unblocked_transactions(transaction.transaction.signature()));

        self.num_executing_transactions -= 1;
        self.metrics.completed_transactions_update_queues_us += update_queues_time.as_us();
        self.metrics.completed_transactions_unblock_transactions_us +=
            unblock_transactions_time.as_us();
    }

    /// Performs scheduling operations on currently pending transactions
    fn do_scheduling(&mut self) {
        let (batches, prepare_batches_time) = measure!({
            // Allocate batches to be sent to threads
            let mut batches =
                vec![Vec::with_capacity(self.max_batch_size); self.transaction_batch_senders.len()];

            // Do scheduling work
            let mut batch_index = 0;
            while let Some(transaction) = self.pending_transactions.pop() {
                if self.try_schedule_transaction(transaction, &mut batches[batch_index]) {
                    // break if we reach max batch size on any of the batches
                    // TODO: just don't add to this batch if it's full
                    if batches[batch_index].len() == self.max_batch_size {
                        break;
                    }
                    batch_index = (batch_index + 1) % batches.len();
                } else {
                    self.num_blocked_transactions += 1;
                }
            }

            batches
        });

        let (_, send_batches_time) = measure!({
            // Send batches to banking threads
            for (batch, sender) in batches
                .into_iter()
                .zip(self.transaction_batch_senders.iter())
            {
                // Only send if we have a non-empty batch
                if batch.len() > 0 {
                    self.metrics.num_transactions_scheduled += batch.len();
                    self.num_executing_transactions += batch.len();
                    sender.send(batch).unwrap();
                }
            }
        });

        self.metrics.max_blocked_transactions = self
            .metrics
            .max_blocked_transactions
            .max(self.num_blocked_transactions);
        self.metrics.max_executing_transactions = self
            .metrics
            .max_executing_transactions
            .max(self.num_executing_transactions);
        self.metrics.scheduling_prepare_batches_us += prepare_batches_time.as_us();
        self.metrics.scheduling_send_batches_us += send_batches_time.as_us();
    }

    /// Insert transaction into account queues and pending queue
    fn insert_transaction(&mut self, transaction: TransactionRef) {
        if let Ok(account_locks) = transaction
            .transaction
            .get_account_locks(&self.bank.feature_set)
        {
            // Insert into readonly queues
            for account in account_locks.readonly {
                self.transactions_by_account
                    .entry(*account)
                    .or_default()
                    .reads
                    .insert(transaction.clone());
            }
            // Insert into writeonly queues
            for account in account_locks.writable {
                self.transactions_by_account
                    .entry(*account)
                    .or_default()
                    .writes
                    .insert(transaction.clone());
            }
        }

        self.pending_transactions.push(transaction);
    }

    /// Update account queues on transaction completion
    fn update_queues_on_completed_transaction(&mut self, transaction: &TransactionMessage) {
        // Should always be able to get account locks here since it was a pre-requisite to scheduling
        let account_locks = transaction
            .transaction
            .get_account_locks(&self.bank.feature_set)
            .unwrap();

        for account in account_locks.readonly {
            if self
                .transactions_by_account
                .get_mut(account)
                .unwrap()
                .handle_completed_transaction(&transaction, false)
            {
                self.transactions_by_account.remove(account);
            }
        }

        for account in account_locks.writable {
            if self
                .transactions_by_account
                .get_mut(account)
                .unwrap()
                .handle_completed_transaction(&transaction, true)
            {
                self.transactions_by_account.remove(account);
            }
        }
    }

    /// Check for unblocked transactions on `signature` and push into `pending_transactions`
    fn push_unblocked_transactions(&mut self, signature: &Signature) {
        if let Some(blocked_transactions) = self.blocked_transactions.remove(signature) {
            self.num_blocked_transactions -= blocked_transactions.len();
            self.pending_transactions
                .extend(blocked_transactions.into_iter());

            self.metrics.max_pending_transactions = self
                .metrics
                .max_pending_transactions
                .max(self.pending_transactions.len());
        }
    }

    /// Tries to schedule a transaction:
    ///     - If it cannot be scheduled, it is inserted into `blocked_transaction`
    ///         with the current lowest priority blocking transaction's signature as the key
    ///     - If it can be scheduled, locks are taken, it is pushed into the provided batch.
    ///
    /// Returns true if the transaction was scheduled, and false otherwise
    fn try_schedule_transaction(
        &mut self,
        transaction: TransactionRef,
        batch: &mut TransactionBatchMessage,
    ) -> bool {
        let (maybe_blocking_transaction, get_lowest_blocking_transaction_time) =
            measure!(self.get_lowest_priority_blocking_transaction(&transaction));
        self.metrics.scheduling_find_blocking_transaction_us +=
            get_lowest_blocking_transaction_time.as_us();

        if let Some(blocking_transaction) = maybe_blocking_transaction {
            let (_, insert_blocked_transaction_time) = measure!(self
                .blocked_transactions
                .entry(*blocking_transaction.transaction.signature())
                .or_default()
                .push(transaction));
            self.metrics.scheduling_insert_blocking_transaction_us +=
                insert_blocked_transaction_time.as_us();

            false
        } else {
            let (_, lock_scheduled_transaction_time) =
                measure!(self.lock_for_transaction(&transaction));
            self.metrics.scheduling_lock_transaction_accounts_us +=
                lock_scheduled_transaction_time.as_us();
            batch.push(transaction);
            true
        }
    }

    /// Gets the lowest priority transaction that blocks this one
    fn get_lowest_priority_blocking_transaction(
        &self,
        transaction: &TransactionRef,
    ) -> Option<TransactionRef> {
        transaction
            .transaction
            .get_account_locks(&self.bank.feature_set)
            .ok()
            .and_then(|account_locks| {
                let min_blocking_transaction = account_locks
                    .readonly
                    .into_iter()
                    .map(|account_key| {
                        self.transactions_by_account
                            .get(account_key)
                            .unwrap()
                            .get_min_blocking_transaction(transaction, false)
                    })
                    .fold(None, option_min);

                account_locks
                    .writable
                    .into_iter()
                    .map(|account_key| {
                        self.transactions_by_account
                            .get(account_key)
                            .unwrap()
                            .get_min_blocking_transaction(transaction, true)
                    })
                    .fold(min_blocking_transaction, option_min)
                    .map(|tx| tx.clone())
            })
    }

    /// Apply account locks for a transaction
    fn lock_for_transaction(&mut self, transaction: &TransactionRef) {
        if let Ok(account_locks) = transaction
            .transaction
            .get_account_locks(&self.bank.feature_set)
        {
            for account in account_locks.readonly {
                self.transactions_by_account
                    .get_mut(account)
                    .unwrap()
                    .handle_schedule_transaction(transaction, false);
            }
            for account in account_locks.writable {
                self.transactions_by_account
                    .get_mut(account)
                    .unwrap()
                    .handle_schedule_transaction(transaction, true);
            }
        }
    }
}

/// Tracks all pending and blocked transacitons, ordered by priority, for a single account
#[derive(Default)]
struct AccountTransactionQueue {
    /// Tree of read transactions on the account ordered by fee-priority
    reads: BTreeSet<TransactionRef>,
    /// Tree of write transactions on the account ordered by fee-priority
    writes: BTreeSet<TransactionRef>,
    /// Tracks currently scheduled transactions on the account
    scheduled_lock: AccountLock,
}

impl AccountTransactionQueue {
    /// Insert a transaction into the queue
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
            .lock_on_transaction(transaction, is_write);
    }

    /// Update account queues and lock for completed `transaction`
    ///     Returns true if the account queue can now be cleared
    ///     Returns false if the account queue cannot be cleared
    fn handle_completed_transaction(
        &mut self,
        transaction: &TransactionRef,
        is_write: bool,
    ) -> bool {
        // remove from tree
        if is_write {
            assert!(self.writes.remove(transaction));
        } else {
            assert!(self.reads.remove(transaction));
        }
        // unlock
        self.scheduled_lock.unlock_on_transaction(is_write);

        // Returns true if there are no more transactions in this account queue
        self.writes.len() == 0 && self.reads.len() == 0
    }

    /// Find the minimum-priority transaction that blocks this transaction if there is one
    fn get_min_blocking_transaction<'a>(
        &'a self,
        transaction: &TransactionRef,
        is_write: bool,
    ) -> Option<&'a TransactionRef> {
        let mut min_blocking_transaction = None;
        // Write transactions will be blocked by higher-priority reads, but read transactions will not
        if is_write {
            min_blocking_transaction = option_min(
                min_blocking_transaction,
                upper_bound(&self.reads, transaction.clone()),
            );
        }

        // All transactions are blocked by higher-priority write-transactions
        min_blocking_transaction = option_min(
            min_blocking_transaction,
            upper_bound(&self.writes, transaction.clone()),
        );

        // Schedule write transactions block transactions, regardless of priorty or read/write
        // Scheduled read transactions block write transactions, regardless of priority
        let scheduled_blocking_transaction = if is_write {
            self.scheduled_lock.lowest_priority_transaction.as_ref()
        } else {
            if self.scheduled_lock.lock.is_write() {
                self.scheduled_lock.lowest_priority_transaction.as_ref()
            } else {
                None
            }
        };

        option_min(min_blocking_transaction, scheduled_blocking_transaction)
    }
}

/// Tracks the currently scheduled lock type and the lowest-fee blocking transaction
#[derive(Debug, Default)]
struct AccountLock {
    lock: AccountLockKind,
    count: usize,
    lowest_priority_transaction: Option<TransactionRef>,
}

impl AccountLock {
    fn lock_on_transaction(&mut self, transaction: &TransactionRef, is_write: bool) {
        if is_write {
            assert!(self.lock.is_none()); // no outstanding lock if scheduling a write
            assert!(self.lowest_priority_transaction.is_none());

            self.lock = AccountLockKind::Write;
            self.lowest_priority_transaction = Some(transaction.clone());
        } else {
            assert!(!self.lock.is_write()); // no outstanding write lock if scheduling a read
            self.lock = AccountLockKind::Read;

            match self.lowest_priority_transaction.as_ref() {
                Some(tx) => {
                    if transaction.cmp(tx).is_lt() {
                        self.lowest_priority_transaction = Some(transaction.clone());
                    }
                }
                None => self.lowest_priority_transaction = Some(transaction.clone()),
            }
        }

        self.count += 1;
    }

    fn unlock_on_transaction(&mut self, is_write: bool) {
        assert!(self.lowest_priority_transaction.is_some());
        if is_write {
            assert!(self.lock.is_write());
            assert!(self.count == 1);
        } else {
            assert!(self.lock.is_read());
            assert!(self.count >= 1);
        }

        self.count -= 1;
        if self.count == 0 {
            self.lock = AccountLockKind::None;
            self.lowest_priority_transaction = None;
        }
    }
}

#[derive(Debug)]
enum AccountLockKind {
    None,
    Read,
    Write,
}

impl Default for AccountLockKind {
    fn default() -> Self {
        Self::None
    }
}

impl AccountLockKind {
    fn is_none(&self) -> bool {
        match self {
            Self::None => true,
            _ => false,
        }
    }

    fn is_write(&self) -> bool {
        match self {
            Self::Write => true,
            _ => false,
        }
    }

    fn is_read(&self) -> bool {
        match self {
            Self::Read => true,
            _ => false,
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

/// Track metrics for the scheduler thread
struct SchedulerMetrics {
    /// Last timestamp reported
    last_reported: Instant,
    /// Number of transactions scheduled
    num_transactions_scheduled: usize,
    /// Maximum pending_transactions length
    max_pending_transactions: usize,
    /// Maximum number of blocked transactions
    max_blocked_transactions: usize,
    /// Maximum executing transactions
    max_executing_transactions: usize,

    /// Total time spent processing completed transactions in microseconds
    completed_transactions_time_us: u64,
    /// Completed Transaction - Time spent updating queues
    completed_transactions_update_queues_us: u64,
    /// Completed Transaciton - Time spent unblocking transactions
    completed_transactions_unblock_transactions_us: u64,

    /// Total time spent processing packet batches in microseconds
    packet_batch_time_us: u64,
    /// Packet Batch - Time spent filtering packets
    packet_batch_filter_time_us: u64,
    /// Packet Batch - Time spent deserializing packets
    packet_batch_deserialize_time_us: u64,
    /// Packet Batch - Time spent inserting transactions
    packet_batch_insert_time_us: u64,

    /// Total time spent scheduling transactions in microseconds
    scheduling_time_us: u64,
    /// Scheduling - Time spent preparing batches
    scheduling_prepare_batches_us: u64,
    /// Scheduling - Time spent sending batches
    scheduling_send_batches_us: u64,
    /// Scheduling - Time spent finding blocking transaction in microseconds
    scheduling_find_blocking_transaction_us: u64,
    /// Scheduling - Time spent inserting blocking transactions in microseconds
    scheduling_insert_blocking_transaction_us: u64,
    /// Scheduling - Time spent locking scheduled transactions in microseconds
    scheduling_lock_transaction_accounts_us: u64,
}

impl Default for SchedulerMetrics {
    fn default() -> Self {
        Self {
            last_reported: Instant::now(),
            num_transactions_scheduled: Default::default(),
            max_pending_transactions: Default::default(),
            max_blocked_transactions: Default::default(),
            max_executing_transactions: Default::default(),
            completed_transactions_time_us: Default::default(),
            completed_transactions_update_queues_us: Default::default(),
            completed_transactions_unblock_transactions_us: Default::default(),
            packet_batch_time_us: Default::default(),
            packet_batch_filter_time_us: Default::default(),
            packet_batch_deserialize_time_us: Default::default(),
            packet_batch_insert_time_us: Default::default(),
            scheduling_time_us: Default::default(),
            scheduling_prepare_batches_us: Default::default(),
            scheduling_send_batches_us: Default::default(),
            scheduling_find_blocking_transaction_us: Default::default(),
            scheduling_insert_blocking_transaction_us: Default::default(),
            scheduling_lock_transaction_accounts_us: Default::default(),
        }
    }
}

impl SchedulerMetrics {
    /// Report metrics if the interval has passed and reset metrics
    fn report(&mut self) {
        const REPORT_INTERVAL_MILLIS: u128 = 1000;

        let elapsed = self.last_reported.elapsed();
        if elapsed.as_millis() >= REPORT_INTERVAL_MILLIS {
            datapoint_info!(
                "transaction-scheduler",
                (
                    "num_transactions_scheduled",
                    self.num_transactions_scheduled as i64,
                    i64
                ),
                (
                    "max_pending_transactions",
                    self.max_pending_transactions as i64,
                    i64
                ),
                (
                    "max_blocked_transactions",
                    self.max_blocked_transactions as i64,
                    i64
                ),
                (
                    "max_executing_transactions",
                    self.max_executing_transactions as i64,
                    i64
                ),
                (
                    "completed_transactions_time_us",
                    self.completed_transactions_time_us as i64,
                    i64
                ),
                (
                    "completed_transactions_update_queues_us",
                    self.completed_transactions_update_queues_us as i64,
                    i64
                ),
                (
                    "completed_transactions_unblock_transactions_us",
                    self.completed_transactions_unblock_transactions_us as i64,
                    i64
                ),
                (
                    "packet_batch_time_us",
                    self.packet_batch_time_us as i64,
                    i64
                ),
                (
                    "packet_batch_filter_time_us",
                    self.packet_batch_filter_time_us as i64,
                    i64
                ),
                (
                    "packet_batch_deserialize_time_us",
                    self.packet_batch_deserialize_time_us as i64,
                    i64
                ),
                (
                    "packet_batch_insert_time_us",
                    self.packet_batch_insert_time_us as i64,
                    i64
                ),
                ("scheduling_time_us", self.scheduling_time_us as i64, i64),
                (
                    "scheduling_prepare_batches_us",
                    self.scheduling_prepare_batches_us as i64,
                    i64
                ),
                (
                    "scheduling_send_batches_us",
                    self.scheduling_send_batches_us as i64,
                    i64
                ),
                (
                    "scheduling_find_blocking_transaction_us",
                    self.scheduling_find_blocking_transaction_us as i64,
                    i64
                ),
                (
                    "scheduling_insert_blocking_transaction_us",
                    self.scheduling_insert_blocking_transaction_us as i64,
                    i64
                ),
                (
                    "scheduling_lock_transaction_accounts_us",
                    self.scheduling_lock_transaction_accounts_us as i64,
                    i64
                ),
            );

            *self = Self::default();
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_perf::packet::Packet,
        solana_sdk::{
            hash::Hash,
            instruction::{AccountMeta, Instruction},
            message::Message,
            signature::Keypair,
            signer::Signer,
            system_program, system_transaction,
            transaction::Transaction,
        },
    };

    fn create_transfer(from: &Keypair, to: &Pubkey, priority: u64) -> Arc<TransactionPriority> {
        Arc::new(TransactionPriority {
            priority,
            transaction: SanitizedTransaction::from_transaction_for_tests(
                system_transaction::transfer(from, to, 0, Hash::default()),
            ),
        })
    }

    fn create_transaction(
        reads: &[&Keypair],
        writes: &[&Keypair],
        priority: u64,
    ) -> Arc<TransactionPriority> {
        let mut accounts: Vec<_> = reads
            .into_iter()
            .map(|account| AccountMeta::new_readonly(account.pubkey(), false))
            .collect();
        accounts.extend(
            writes
                .into_iter()
                .map(|account| AccountMeta::new(account.pubkey(), false)),
        );

        let instruction = Instruction {
            program_id: Pubkey::default(),
            accounts,
            data: vec![],
        };
        let message = Message::new(&[instruction], Some(&writes.first().unwrap().pubkey()));
        let transaction = Transaction::new(&[writes[0].to_owned()], message, Hash::default());
        let transaction = SanitizedTransaction::from_transaction_for_tests(transaction);
        Arc::new(TransactionPriority {
            priority,
            transaction,
        })
    }

    fn create_scheduler() -> (TransactionScheduler, Vec<Receiver<TransactionBatchMessage>>) {
        const NUM_BANKING_THREADS: usize = 1;

        let (_, pb_rx) = crossbeam_channel::unbounded();
        let mut tb_txs = Vec::with_capacity(NUM_BANKING_THREADS);
        let mut tb_rxs = Vec::with_capacity(NUM_BANKING_THREADS);
        for _ in 0..NUM_BANKING_THREADS {
            let (tx, rx) = crossbeam_channel::unbounded();
            tb_txs.push(tx);
            tb_rxs.push(rx);
        }
        let (_, ct_rx) = crossbeam_channel::unbounded();

        let scheduler = TransactionScheduler {
            packet_batch_receiver: pb_rx,
            transaction_batch_senders: tb_txs,
            completed_transaction_receiver: ct_rx,
            bank: Arc::new(Bank::default_for_tests()),
            max_batch_size: 128,
            exit: Arc::new(AtomicBool::default()),
            pending_transactions: BinaryHeap::default(),
            transactions_by_account: HashMap::default(),
            blocked_transactions: HashMap::default(),
        };

        (scheduler, tb_rxs)
    }

    fn check_batch(
        rx: &Receiver<TransactionBatchMessage>,
        expected_batch: &[TransactionRef],
    ) -> TransactionBatchMessage {
        let maybe_tx_batch = rx.try_recv();

        if expected_batch.len() > 0 {
            assert!(maybe_tx_batch.is_ok());
            let tx_batch = maybe_tx_batch.unwrap();

            assert_eq!(
                expected_batch.len(),
                tx_batch.len(),
                "expected: {:#?}, actual: {:#?}",
                expected_batch,
                tx_batch
            );
            for (expected_tx, tx) in expected_batch.into_iter().zip(tx_batch.iter()) {
                assert_eq!(expected_tx, tx);
            }
            tx_batch
        } else {
            assert!(maybe_tx_batch.is_err());
            TransactionBatchMessage::default()
        }
    }

    fn complete_batch(scheduler: &mut TransactionScheduler, batch: &[TransactionRef]) {
        for transaction in batch.into_iter().cloned() {
            scheduler.handle_completed_transaction(transaction);
        }
    }

    #[test]
    fn test_transaction_scheduler_insert_transaction() {
        let (mut scheduler, _) = create_scheduler();

        let account1 = Keypair::new();
        let account2 = Pubkey::new_unique();
        let tx1 = create_transfer(&account1, &account2, 1);
        scheduler.insert_transaction(tx1.clone());

        assert_eq!(1, scheduler.pending_transactions.len());
        assert_eq!(3, scheduler.transactions_by_account.len());

        assert!(scheduler
            .transactions_by_account
            .contains_key(&account1.pubkey()));
        assert_eq!(
            1,
            scheduler
                .transactions_by_account
                .get(&account1.pubkey())
                .unwrap()
                .writes
                .len()
        );

        assert!(scheduler.transactions_by_account.contains_key(&account2));
        assert_eq!(
            1,
            scheduler
                .transactions_by_account
                .get(&account2)
                .unwrap()
                .writes
                .len()
        );

        assert!(scheduler
            .transactions_by_account
            .contains_key(&system_program::id()));
        assert_eq!(
            1,
            scheduler
                .transactions_by_account
                .get(&system_program::id())
                .unwrap()
                .reads
                .len()
        );

        let tx2 = create_transfer(&account1, &account2, 2);
        scheduler.insert_transaction(tx2.clone());

        assert_eq!(2, scheduler.pending_transactions.len());
        assert_eq!(3, scheduler.transactions_by_account.len());
        assert_eq!(
            2,
            scheduler
                .transactions_by_account
                .get(&account1.pubkey())
                .unwrap()
                .writes
                .len()
        );
        assert_eq!(
            2,
            scheduler
                .transactions_by_account
                .get(&account2)
                .unwrap()
                .writes
                .len()
        );
        assert_eq!(
            2,
            scheduler
                .transactions_by_account
                .get(&system_program::id())
                .unwrap()
                .reads
                .len()
        );
    }

    #[test]
    fn test_transaction_scheduler_conflicting_writes() {
        let (mut scheduler, tb_rxs) = create_scheduler();

        let account1 = Keypair::new();
        let account2 = Pubkey::new_unique();
        let tx1 = create_transfer(&account1, &account2, 1);
        let tx2 = create_transfer(&account1, &account2, 2);
        scheduler.insert_transaction(tx1.clone());
        scheduler.insert_transaction(tx2.clone());

        // First batch should only be tx2, since it has higher priority and conflicts with tx1
        {
            scheduler.do_scheduling();
            let tx_batch = check_batch(&tb_rxs[0], &[tx2.clone()]);
            complete_batch(&mut scheduler, &tx_batch);
        }

        // Second batch should have tx1, since it is now unblocked
        {
            scheduler.do_scheduling();
            let tx_batch = check_batch(&tb_rxs[0], &[tx1.clone()]);

            // Nothing left to schedule while we wait for the transactions to be completed
            {
                scheduler.do_scheduling();
                let maybe_tx_batch = tb_rxs[0].try_recv();
                assert!(maybe_tx_batch.is_err());
            }

            complete_batch(&mut scheduler, &tx_batch);
        }

        // Nothing left to schedule
        {
            scheduler.do_scheduling();
            let tx_batch = check_batch(&tb_rxs[0], &[]);
        }
    }

    #[test]
    fn test_transaction_scheduler_non_conflicting_writes() {
        let (mut scheduler, tb_rxs) = create_scheduler();

        let account1 = Keypair::new();
        let account2 = Pubkey::new_unique();
        let account3 = Keypair::new();
        let account4 = Pubkey::new_unique();
        let tx1 = create_transfer(&account1, &account2, 1);
        let tx2 = create_transfer(&account3, &account4, 2);
        scheduler.insert_transaction(tx1.clone());
        scheduler.insert_transaction(tx2.clone());

        // First batch should contain tx2 and tx1 since they don't conflict
        {
            scheduler.do_scheduling();
            let tx_batch = check_batch(&tb_rxs[0], &[tx2.clone(), tx1.clone()]);
            complete_batch(&mut scheduler, &tx_batch);
        }

        // Nothing left to schedule
        {
            scheduler.do_scheduling();
            let tx_batch = check_batch(&tb_rxs[0], &[]);
        }
    }

    #[test]
    fn test_transaction_scheduler_higher_priority_transaction_comes_in_after_scheduling() {
        let (mut scheduler, tb_rxs) = create_scheduler();

        let account1 = Keypair::new();
        let account2 = Pubkey::new_unique();
        let tx1 = create_transfer(&account1, &account2, 1);
        let tx2 = create_transfer(&account1, &account2, 2);
        scheduler.insert_transaction(tx1.clone());

        // First batch should only have tx1 since it is the only transaction inserted
        scheduler.do_scheduling();
        let tx_batch1 = check_batch(&tb_rxs[0], &[tx1.clone()]);

        // Higher priority transaction (conflicting with tx1) comes in AFTER scheduling
        scheduler.insert_transaction(tx2.clone());

        // Nothing to schedule while tx_batch1 is outstanding
        scheduler.do_scheduling();
        let _ = check_batch(&tb_rxs[0], &[]);

        // Once tx1 completes, we are able to schedule tx2
        complete_batch(&mut scheduler, &tx_batch1);
        scheduler.do_scheduling();
        let tx_batch2 = check_batch(&tb_rxs[0], &[tx2.clone()]);
        complete_batch(&mut scheduler, &tx_batch2);

        // Nothing to schedule since nothing is left
        scheduler.do_scheduling();
        let _ = check_batch(&tb_rxs[0], &[]);
    }

    /// Tests the following case:
    /// 400: A(W)      C(R)
    /// 200:           C(R)      F(W)
    /// 500: A(W) B(W)
    /// 300:           C(R) E(W)
    /// 350:           C(W) G(W)
    #[test]
    fn test_transaction_scheduler_case0() {
        let (mut scheduler, tb_rxs) = create_scheduler();

        let a = Keypair::new();
        let b = Keypair::new();
        let c = Keypair::new();
        let e = Keypair::new();
        let f = Keypair::new();
        let g = Keypair::new();

        let tx_400 = create_transaction(&[&c], &[&a], 400);
        let tx_200 = create_transaction(&[&c], &[&f], 200);
        let tx_500 = create_transaction(&[], &[&a, &b], 500);
        let tx_300 = create_transaction(&[&c], &[&e], 300);
        let tx_350 = create_transaction(&[], &[&c, &g], 350);

        scheduler.insert_transaction(tx_400.clone());
        scheduler.insert_transaction(tx_200.clone());
        scheduler.insert_transaction(tx_500.clone());
        scheduler.insert_transaction(tx_300.clone());
        scheduler.insert_transaction(tx_350.clone());

        // First batch should only have 500, since the next highest tx (400) is blocked
        {
            scheduler.do_scheduling();
            let tx_batch = check_batch(&tb_rxs[0], &[tx_500.clone()]);
            complete_batch(&mut scheduler, &tx_batch);
        }

        // Second batch should only have 400, since the next highest tx (350) is blocked
        {
            scheduler.do_scheduling();
            let tx_batch = check_batch(&tb_rxs[0], &[tx_400.clone()]);
            complete_batch(&mut scheduler, &tx_batch);
        }

        // Third batch should only have 350, since the next highest txs (300, 200) are blocked
        {
            scheduler.do_scheduling();
            let tx_batch = check_batch(&tb_rxs[0], &[tx_350.clone()]);
            complete_batch(&mut scheduler, &tx_batch);
        }

        // Final batch should have (300, 200)
        {
            scheduler.do_scheduling();
            let tx_batch = check_batch(&tb_rxs[0], &[tx_300.clone(), tx_200.clone()]);
            complete_batch(&mut scheduler, &tx_batch);
        }

        // Nothing left to schedule
        {
            scheduler.do_scheduling();
            let tx_batch = check_batch(&tb_rxs[0], &[]);
        }
    }

    /// Tests the following case:
    /// 400: A(W)      C(R)
    /// 200:           C(R)      F(W)
    /// 500: A(W) B(W)
    /// 300:           C(R) E(W)
    #[test]
    fn test_transaction_scheduler_case1() {
        let (mut scheduler, tb_rxs) = create_scheduler();

        let a = Keypair::new();
        let b = Keypair::new();
        let c = Keypair::new();
        let e = Keypair::new();
        let f = Keypair::new();

        let tx_400 = create_transaction(&[&c], &[&a], 400);
        let tx_200 = create_transaction(&[&c], &[&f], 200);
        let tx_500 = create_transaction(&[], &[&a, &b], 500);
        let tx_300 = create_transaction(&[&c], &[&e], 300);

        scheduler.insert_transaction(tx_400.clone());
        scheduler.insert_transaction(tx_200.clone());
        scheduler.insert_transaction(tx_500.clone());
        scheduler.insert_transaction(tx_300.clone());

        // First batch should have (500, 300, 200). 500 blocks 400, but 300 and 200 locks do not conflict with 400.
        {
            scheduler.do_scheduling();
            let tx_batch = check_batch(
                &tb_rxs[0],
                &[tx_500.clone(), tx_300.clone(), tx_200.clone()],
            );
            complete_batch(&mut scheduler, &tx_batch);
        }

        // Second batch should only have 400
        {
            scheduler.do_scheduling();
            let tx_batch = check_batch(&tb_rxs[0], &[tx_400.clone()]);
            complete_batch(&mut scheduler, &tx_batch);
        }

        // Nothing left to schedule
        {
            scheduler.do_scheduling();
            let tx_batch = check_batch(&tb_rxs[0], &[]);
        }
    }

    #[test]
    fn test_account_transaction_queue_insert() {
        let mut queue = AccountTransactionQueue::default();

        let account1 = Keypair::new();
        let account2 = Pubkey::new_unique();

        queue.insert_transaction(create_transfer(&account1, &account2, 1), true);
        assert_eq!(1, queue.writes.len());
        assert_eq!(0, queue.reads.len());
        assert!(queue.scheduled_lock.lock.is_none());
        assert_eq!(0, queue.scheduled_lock.count);
        assert_eq!(None, queue.scheduled_lock.lowest_priority_transaction);

        queue.insert_transaction(create_transfer(&account1, &account2, 2), true);
        assert_eq!(2, queue.writes.len());
        assert_eq!(0, queue.reads.len());
        assert!(queue.scheduled_lock.lock.is_none());
        assert_eq!(0, queue.scheduled_lock.count);
        assert_eq!(None, queue.scheduled_lock.lowest_priority_transaction);

        queue.insert_transaction(create_transfer(&account1, &account2, 3), false);
        assert_eq!(2, queue.writes.len());
        assert_eq!(1, queue.reads.len());
        assert!(queue.scheduled_lock.lock.is_none());
        assert_eq!(0, queue.scheduled_lock.count);
        assert_eq!(None, queue.scheduled_lock.lowest_priority_transaction);

        queue.insert_transaction(create_transfer(&account1, &account2, 4), false);
        assert_eq!(2, queue.writes.len());
        assert_eq!(2, queue.reads.len());
        assert!(queue.scheduled_lock.lock.is_none());
        assert_eq!(0, queue.scheduled_lock.count);
        assert_eq!(None, queue.scheduled_lock.lowest_priority_transaction);
    }

    #[test]
    #[should_panic]
    fn test_account_transaction_queue_handle_schedule_write_transaction() {
        let mut queue = AccountTransactionQueue::default();

        let account1 = Keypair::new();
        let account2 = Pubkey::new_unique();

        let tx1 = create_transfer(&account1, &account2, 1);
        let tx2 = create_transfer(&account1, &account2, 5);
        queue.insert_transaction(tx1.clone(), true);
        queue.insert_transaction(tx2.clone(), false);
        queue.handle_schedule_transaction(&tx1, true);

        assert_eq!(1, queue.writes.len()); // still exists in the write queue
        assert_eq!(1, queue.reads.len());
        assert!(queue.scheduled_lock.lock.is_write()); // write-lock taken
        assert_eq!(1, queue.scheduled_lock.count);
        assert_eq!(Some(tx1), queue.scheduled_lock.lowest_priority_transaction);

        queue.handle_schedule_transaction(&tx2, false); // should panic since write-lock is taken
    }

    #[test]
    #[should_panic]
    fn test_account_transaction_queue_handle_schedule_read_transactions() {
        let mut queue = AccountTransactionQueue::default();

        let account1 = Keypair::new();
        let account2 = Pubkey::new_unique();

        let tx1 = create_transfer(&account1, &account2, 1);
        let tx2 = create_transfer(&account1, &account2, 5);
        let tx3 = create_transfer(&account1, &account2, 10);
        queue.insert_transaction(tx2.clone(), true);
        queue.insert_transaction(tx1.clone(), false);
        queue.insert_transaction(tx3.clone(), false);
        queue.handle_schedule_transaction(&tx1, false);

        assert_eq!(1, queue.writes.len());
        assert_eq!(2, queue.reads.len()); // still exists in the read queue
        assert!(queue.scheduled_lock.lock.is_read()); // read-lock taken
        assert_eq!(1, queue.scheduled_lock.count);
        assert_eq!(
            Some(tx1.clone()),
            queue.scheduled_lock.lowest_priority_transaction
        );

        queue.handle_schedule_transaction(&tx3, false);

        assert_eq!(1, queue.writes.len()); // still exists in the write queue
        assert_eq!(2, queue.reads.len());
        assert!(queue.scheduled_lock.lock.is_read()); // read-lock taken
        assert_eq!(2, queue.scheduled_lock.count);
        assert_eq!(Some(tx1), queue.scheduled_lock.lowest_priority_transaction);

        queue.handle_schedule_transaction(&tx2, true); // should panic because we cannot schedule a write when read-lock is taken
    }

    #[test]
    fn test_account_transaction_queue_handle_completed_transaction() {
        let mut queue = AccountTransactionQueue::default();

        let account1 = Keypair::new();
        let account2 = Pubkey::new_unique();
        let tx1 = create_transfer(&account1, &account2, 1);
        let tx2 = create_transfer(&account1, &account2, 2);

        queue.insert_transaction(tx1.clone(), true);
        queue.insert_transaction(tx2.clone(), true);
        assert_eq!(2, queue.writes.len());
        assert_eq!(0, queue.reads.len());
        assert!(queue.scheduled_lock.lock.is_none());
        assert_eq!(0, queue.scheduled_lock.count);
        assert_eq!(None, queue.scheduled_lock.lowest_priority_transaction);

        queue.handle_schedule_transaction(&tx2, true);
        assert!(!queue.handle_completed_transaction(&tx2, true)); // queue is not empty, so it should return false
        assert_eq!(1, queue.writes.len());
        assert_eq!(0, queue.reads.len());
        assert!(queue.scheduled_lock.lock.is_none());
        assert_eq!(0, queue.scheduled_lock.count);
        assert_eq!(None, queue.scheduled_lock.lowest_priority_transaction);

        queue.handle_schedule_transaction(&tx1, true);
        assert!(queue.handle_completed_transaction(&tx1, true)); // queue is now empty, so it should return true
        assert_eq!(0, queue.writes.len());
        assert_eq!(0, queue.reads.len());
        assert!(queue.scheduled_lock.lock.is_none());
        assert_eq!(0, queue.scheduled_lock.count);
        assert_eq!(None, queue.scheduled_lock.lowest_priority_transaction);
    }

    #[test]
    fn test_account_transaction_queue_get_min_blocking_transaction() {
        let mut queue = AccountTransactionQueue::default();

        let account1 = Keypair::new();
        let account2 = Pubkey::new_unique();
        let tx1 = create_transfer(&account1, &account2, 1);
        let tx2 = create_transfer(&account1, &account2, 2);
        queue.insert_transaction(tx1.clone(), false);
        queue.insert_transaction(tx2.clone(), true);

        // write blocks read
        assert_eq!(Some(&tx2), queue.get_min_blocking_transaction(&tx1, false));
        assert_eq!(None, queue.get_min_blocking_transaction(&tx2, true));

        let tx3 = create_transfer(&account1, &account2, 3);
        queue.insert_transaction(tx3.clone(), false);

        // read blocks write
        assert_eq!(Some(&tx2), queue.get_min_blocking_transaction(&tx1, false));
        assert_eq!(Some(&tx3), queue.get_min_blocking_transaction(&tx2, true));
        assert_eq!(None, queue.get_min_blocking_transaction(&tx3, false));

        // scheduled transaction blocks regardless of priority
        queue.handle_schedule_transaction(&tx1, false);
        assert_eq!(Some(&tx1), queue.get_min_blocking_transaction(&tx2, true));
        assert_eq!(None, queue.get_min_blocking_transaction(&tx3, false));
    }

    #[test]
    fn test_upper_bound_normal() {
        let tree: BTreeSet<_> = [2, 3, 1].into_iter().collect();
        assert_eq!(Some(&2), upper_bound(&tree, 1));
    }

    #[test]
    fn test_upper_bound_duplicates() {
        let tree: BTreeSet<_> = [2, 2, 3, 1, 1].into_iter().collect();
        assert_eq!(Some(&2), upper_bound(&tree, 1));
    }

    #[test]
    fn test_option_min_none_and_none() {
        assert_eq!(None, option_min::<u32>(None, None));
    }

    #[test]
    fn test_option_min_none_and_some() {
        assert_eq!(Some(1), option_min(None, Some(1)));
    }

    #[test]
    fn test_option_min_some_and_none() {
        assert_eq!(Some(1), option_min(Some(1), None));
    }

    #[test]
    fn test_option_min_some_and_some() {
        assert_eq!(Some(1), option_min(Some(1), Some(2)));
        assert_eq!(Some(1), option_min(Some(2), Some(1)));
    }
}
