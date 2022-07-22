//! Implements a transaction sche&duler

use {
    crate::unprocessed_packet_batches::{self, ImmutableDeserializedPacket},
    crossbeam_channel::{select, Receiver, Sender, TryRecvError},
    dashmap::DashMap,
    solana_measure::{measure, measure::Measure},
    solana_perf::packet::PacketBatch,
    solana_runtime::bank::Bank,
    solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::SanitizedTransaction},
    std::{
        collections::{BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque},
        hash::Hash,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        thread::JoinHandle,
        time::Instant,
    },
};
/// Wrapper to store a sanitized transaction and priority
#[derive(Clone, Debug)]
pub struct TransactionPriority {
    /// Transaction priority
    pub priority: u64,
    /// Sanitized transaction
    pub transaction: SanitizedTransaction,
    /// Timestamp the scheduler received the transaction - only used for ordering
    pub timestamp: Instant,
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
        self.timestamp.hash(state);
        self.priority.hash(state);
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
type TransactionBatchMessage = (TransactionBatchId, Vec<SanitizedTransaction>);

/// Separate packet deserialization and ordering
struct PacketBatchHandler {
    /// Exit signal
    exit: Arc<AtomicBool>,
    /// Bank
    bank: Arc<Bank>,
    /// Channel for receiving deserialized packet batches from SigVerify
    packet_batch_receiver: Receiver<PacketBatchMessage>,
    /// Pending transactions to be send to the scheduler
    pending_transactions: Arc<Mutex<BinaryHeap<TransactionRef>>>,
    /// Account Queues
    transactions_by_account: Arc<DashMap<Pubkey, AccountTransactionQueue>>,
}

impl PacketBatchHandler {
    /// Driving loop
    fn main(mut self) {
        loop {
            if self.exit.load(Ordering::Relaxed) {
                break;
            }
            self.iter();
        }
    }

    /// Try receiving packets or send out buffered transactions
    fn iter(&mut self) {
        if let Ok(packet_batches) = self.packet_batch_receiver.try_recv() {
            self.handle_packet_batches(packet_batches);
        }
    }

    /// Handle received packet batches - deserialize and put into the buffer
    fn handle_packet_batches(&mut self, packet_batches: Vec<PacketBatch>) {
        for packet_batch in packet_batches {
            let packet_indices = packet_batch
                .into_iter()
                .enumerate()
                .filter_map(|(idx, p)| if !p.meta.discard() { Some(idx) } else { None })
                .collect::<Vec<_>>();
            let transactions =
                unprocessed_packet_batches::deserialize_packets(&packet_batch, &packet_indices)
                    .filter_map(|deserialized_packet| {
                        TransactionPriority::try_new(
                            deserialized_packet.immutable_section(),
                            &self.bank,
                        )
                    })
                    .collect::<Vec<_>>();
            self.insert_transactions(transactions);
        }
    }

    /// Insert transactions into queues and pending
    fn insert_transactions(&self, transactions: Vec<TransactionRef>) {
        for tx in &transactions {
            // Get account locks
            let account_locks = tx.transaction.get_account_locks().unwrap();
            for account in account_locks.readonly.into_iter() {
                self.transactions_by_account.entry(*account).or_default();
                // .reads
                // .insert(tx.clone());
            }

            for account in account_locks.writable.into_iter() {
                self.transactions_by_account.entry(*account).or_default();
                // .writes
                // .insert(tx.clone());
            }
        }
        self.pending_transactions
            .lock()
            .unwrap()
            .extend(transactions.into_iter());
    }
}

/// Stores state for scheduling transactions and channels for communicating
/// with other threads: SigVerify and Banking
pub struct TransactionScheduler {
    /// Channels for sending transaction batches to banking threads
    transaction_batch_senders: Vec<Sender<TransactionBatchMessage>>,
    /// Channel for receiving completed transactions from any banking thread
    completed_batch_receiver: Receiver<TransactionBatchId>,
    /// Bank that we are currently scheduling for
    bank: Arc<Bank>,
    /// Max number of transactions to send to a single banking-thread in a batch
    max_batch_size: usize,
    /// Exit signal
    exit: Arc<AtomicBool>,

    /// Pending transactions that are not known to be blocked
    pending_transactions: Arc<Mutex<BinaryHeap<TransactionRef>>>,
    /// Transaction queues and locks by account key
    transactions_by_account: Arc<DashMap<Pubkey, AccountTransactionQueue>>,
    /// Map from transaction signature to transactions blocked by the signature
    // blocked_transactions: HashMap<Signature, Vec<TransactionRef>>,
    /// Map from blocking BatchId to transactions
    blocked_transactions_by_batch_id: HashMap<TransactionBatchId, HashSet<TransactionRef>>,
    /// Transactions blocked by batches need to count how many they're blocked by
    // blocked_transactions_batch_count: HashMap<TransactionRef, usize>,
    /// Tracks the current number of blocked transactions
    num_blocked_transactions: usize,
    /// Tracks the current number of executing transacitons
    num_executing_transactions: usize,

    /// Generates TransactionBatchIds
    next_transaction_batch_id: TransactionBatchId,
    /// Tracks TransactionBatchDetails by TransactionBatchId
    transaction_batches: HashMap<TransactionBatchId, TransactionBatch>,
    /// Currently in-progress batches (references into `transaction_batches`)
    in_progress_batches: HashSet<TransactionBatchId>,
    oldest_in_progress_batch: Option<TransactionBatchId>,

    /// Number of execution threads
    num_execution_threads: usize,
    /// Tracks status of exeuction threads
    execution_thread_stats: Vec<ExecutionThreadStats>,

    /// Track metrics for scheduler thread
    metrics: SchedulerMetrics,
}

impl TransactionScheduler {
    /// Create and start transaction scheduler thread
    pub fn spawn_scheduler(
        packet_batch_receiver: Receiver<PacketBatchMessage>,
        transaction_batch_senders: Vec<Sender<TransactionBatchMessage>>,
        completed_batch_receiver: Receiver<TransactionBatchId>,
        bank: Arc<Bank>,
        max_batch_size: usize,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let pending_transactions = Arc::new(Mutex::new(BinaryHeap::default()));
        let transactions_by_account = Arc::new(DashMap::default());

        let packet_handler = PacketBatchHandler {
            exit: exit.clone(),
            bank: bank.clone(),
            packet_batch_receiver,
            pending_transactions: pending_transactions.clone(),
            transactions_by_account: transactions_by_account.clone(),
        };

        std::thread::spawn(move || packet_handler.main());
        let num_execution_threads = transaction_batch_senders.len();
        let execution_thread_stats = (0..num_execution_threads)
            .into_iter()
            .map(|_| ExecutionThreadStats::default())
            .collect();

        let mut scheduler = TransactionScheduler {
            transaction_batch_senders,
            completed_batch_receiver,
            bank,
            max_batch_size,
            exit,
            pending_transactions,
            transactions_by_account,
            // blocked_transactions: HashMap::default(),
            blocked_transactions_by_batch_id: HashMap::default(),
            // blocked_transactions_batch_count: HashMap::default(),
            num_blocked_transactions: 0,
            num_executing_transactions: 0,
            next_transaction_batch_id: 0,
            transaction_batches: HashMap::default(),
            in_progress_batches: HashSet::with_capacity(num_execution_threads),
            oldest_in_progress_batch: None,
            num_execution_threads,
            execution_thread_stats,
            metrics: SchedulerMetrics::default(),
        };

        // Initialize batches
        for execution_thread_index in 0..num_execution_threads {
            scheduler.create_new_batch(execution_thread_index);
        }

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
        fn try_recv<T>(receiver: &Receiver<T>) -> (Result<T, TryRecvError>, Measure) {
            measure!(receiver.try_recv())
        }

        // Try receiving completed batches
        let (_, completed_batch_time) = measure!({
            let (maybe_completed_batch, recv_time) = try_recv(&self.completed_batch_receiver);
            let (_, handle_batch_time) = measure!({
                if let Ok(completed_batch) = maybe_completed_batch {
                    self.handle_completed_batch(completed_batch);
                }
            });

            self.metrics.compeleted_batch_try_recv_time_us += recv_time.as_us();
            self.metrics.completed_batch_handle_batch_time_us += handle_batch_time.as_us();
        });
        self.metrics.completed_transactions_time_us += completed_batch_time.as_us();

        // Scheduling time
        let (_, scheduling_time) = measure!(self.do_scheduling());
        self.metrics.scheduling_time_us += scheduling_time.as_us();

        // Check if oldest batch should be sent due to age
        if let Some(oldest_batch_id) = self.oldest_in_progress_batch {
            let batch = self.transaction_batches.get(&oldest_batch_id).unwrap();
            if batch.start_time.elapsed() >= std::time::Duration::from_millis(1000) {
                // if batch.transactions.len() > 0 {
                self.send_batch(oldest_batch_id);
                // } else {
                //     assert!(self
                //         .blocked_transactions_by_batch_id
                //         .get(&oldest_batch_id)
                //         .is_none());
                // }
            }
        }

        // if self.metrics.last_reported.elapsed().as_millis() >= 1000 {
        //     let pending = self.pending_transactions.lock().unwrap().len();
        //     let in_progress_batches = &self.in_progress_batches;
        //     let batches = self
        //         .transaction_batches
        //         .iter()
        //         .map(|(batch_id, batch)| (*batch_id, batch.scheduled, batch.num_transactions))
        //         .collect::<Vec<_>>();
        //     let blocked_batches = self
        //         .blocked_transactions_by_batch_id
        //         .iter()
        //         .map(|(batch_id, txs)| (*batch_id, txs.len()))
        //         .collect::<Vec<_>>();

        //     error!("pending: {pending} in_progress_batches: {in_progress_batches:?} batches: {batches:?} blocked_batches: {blocked_batches:?}");
        // }
        let (_, metrics_time) = measure!(self.metrics.report());
        self.metrics.metrics_us += metrics_time.as_us();
    }

    /// Handle completed transaction batch
    fn handle_completed_batch(&mut self, batch_id: TransactionBatchId) {
        // Check batch exists in the tracking and matches the queue for the execution thread
        let batch = self.transaction_batches.remove(&batch_id).unwrap();
        assert_eq!(
            batch_id,
            self.execution_thread_stats[batch.execution_thread_index]
                .queued_batches
                .pop_front()
                .unwrap()
        );

        // Update number of executing transactions
        self.num_executing_transactions -= batch.num_transactions;
        let execution_thread_stats = &mut self.execution_thread_stats[batch.execution_thread_index];
        // TODO: actually track CUs instead of just num transacitons
        execution_thread_stats.queued_transactions -= batch.num_transactions;
        execution_thread_stats.queued_compute_units -= batch.num_transactions;

        // Remove account locks
        let (_, remove_account_locks_time) = measure!({
            for (account, _lock) in batch.account_locks {
                self.unlock_account(account, batch_id);
            }
        });

        // Push transactions blocked (by batch) back into the pending queue
        let (_, unblock_transactions_time) = measure!({
            if let Some(blocked_transactions) =
                self.blocked_transactions_by_batch_id.remove(&batch_id)
            {
                // let unblocked_transactions = blocked_transactions.into_iter().filter(|tx| {
                //     let blocked_batches_count =
                //         self.blocked_transactions_batch_count.get_mut(tx).unwrap();
                //     *blocked_batches_count -= 1;
                //     if *blocked_batches_count == 0 {
                //         self.blocked_transactions_batch_count.remove(tx);
                //         self.num_blocked_transactions -= 1;
                //         true
                //     } else {
                //         false
                //     }
                // });
                self.num_blocked_transactions -= blocked_transactions.len();
                self.pending_transactions
                    .lock()
                    .unwrap()
                    .extend(blocked_transactions.into_iter());
            }
        });

        // Create a new batch for the thread (if necessary)
        self.create_new_batch(batch.execution_thread_index);

        self.metrics.completed_transactions_remove_account_locks_us +=
            remove_account_locks_time.as_us();
        self.metrics.completed_transactions_unblock_transactions_us +=
            unblock_transactions_time.as_us();
    }

    /// Remove account locks for the batch id
    fn unlock_account(&mut self, account: Pubkey, batch_id: TransactionBatchId) {
        self.transactions_by_account
            .get_mut(&account)
            .unwrap()
            .unlock(batch_id)
    }

    /// Create new batches if necessary, and create batch reference vector
    fn create_new_batch(&mut self, execution_thread_index: usize) {
        const MAX_QUEUED_BATCHES_PER_THREAD: usize = 2;

        // Check if we should create a new batch for this thread:
        //  1. Thread does not exceed the queued batch limit
        //  2. There is not a batch currently being built
        // if self.execution_thread_stats[execution_thread_index]
        //     .queued_batches
        //     .len()
        //     < MAX_QUEUED_BATCHES_PER_THREAD
        //     && self.execution_thread_stats[execution_thread_index]
        //         .queued_batches
        //         .back()
        //         .filter(|batch_id| self.transaction_batches.get(*batch_id).unwrap().scheduled)
        //         .is_none()
        // {
        //     let batch_id = self.next_transaction_batch_id;
        //     self.next_transaction_batch_id += 1;
        //     self.transaction_batches.insert(
        //         batch_id,
        //         TransactionBatch {
        //             scheduled: false,
        //             start_time: Instant::now(),
        //             id: batch_id,
        //             num_transactions: 0,
        //             transactions: Vec::with_capacity(self.max_batch_size),
        //             account_locks: HashMap::default(),
        //             execution_thread_index,
        //         },
        //     );
        //     self.execution_thread_stats[execution_thread_index]
        //         .queued_batches
        //         .push_back(batch_id);
        //     self.in_progress_batches.insert(batch_id);
        //     self.oldest_in_progress_batch.get_or_insert(batch_id);
        // }

        let execution_thread_stats = &mut self.execution_thread_stats[execution_thread_index];
        if execution_thread_stats.queued_batches.len() < MAX_QUEUED_BATCHES_PER_THREAD {
            let should_create_new_batch =
                if let Some(most_recent_batch_id) = execution_thread_stats.queued_batches.back() {
                    self.transaction_batches
                        .get(most_recent_batch_id)
                        .unwrap()
                        .scheduled
                } else {
                    true
                };
            if should_create_new_batch {
                let batch_id = self.next_transaction_batch_id;
                self.next_transaction_batch_id += 1;
                self.transaction_batches.insert(
                    batch_id,
                    TransactionBatch {
                        scheduled: false,
                        start_time: Instant::now(),
                        id: batch_id,
                        num_transactions: 0,
                        transactions: Vec::with_capacity(self.max_batch_size),
                        account_locks: HashMap::default(),
                        execution_thread_index,
                    },
                );
                self.execution_thread_stats[execution_thread_index]
                    .queued_batches
                    .push_back(batch_id);
                self.in_progress_batches.insert(batch_id);
                self.oldest_in_progress_batch.get_or_insert(batch_id);
            }
        }
    }

    /// Send an in-progress batch
    fn send_batch(&mut self, batch_id: usize) {
        let batch = self.transaction_batches.get_mut(&batch_id).unwrap();
        self.metrics.max_batch_age = self
            .metrics
            .max_batch_age
            .max(batch.start_time.elapsed().as_micros() as u64);
        let execution_thread_index = batch.execution_thread_index;
        // 1. Send the batch to thread
        {
            // Build the batch
            let transactions = batch.build_account_locks_on_send();

            // Update execution thread stats
            let execution_thread_stats = &mut self.execution_thread_stats[execution_thread_index];
            execution_thread_stats.queued_transactions += transactions.len();

            // Update metrics
            self.num_executing_transactions += transactions.len();
            self.metrics.num_transactions_scheduled += transactions.len();
            self.metrics.max_blocked_transactions = self
                .metrics
                .max_blocked_transactions
                .max(self.num_blocked_transactions);
            self.metrics.max_executing_transactions = self
                .num_executing_transactions
                .max(self.num_executing_transactions);

            // Send the batch
            self.transaction_batch_senders[execution_thread_index].send((batch_id, transactions));
        }
        // 2. Remove from in-progress batch set
        self.in_progress_batches.remove(&batch_id);
        // 3. Check if we should create a new batch for the thread
        self.create_new_batch(execution_thread_index);
        // 4. Possibly update the oldest batch
        if batch_id == self.oldest_in_progress_batch.unwrap() {
            self.oldest_in_progress_batch = self.in_progress_batches.iter().min().cloned()
        }
    }

    /// Performs scheduling operations on currently pending transactions
    fn do_scheduling(&mut self) {
        const MAX_QUEUED_BATCHES_PER_THREAD: usize = 2;
        // self.create_new_batches(); // create new in-progress batches if necessary

        // for _ in 0..self.max_batch_size {
        if self.in_progress_batches.is_empty() {
            return;
        }
        let maybe_transaction = self.pending_transactions.lock().unwrap().pop();
        if let Some(transaction) = maybe_transaction {
            self.try_schedule_transaction(transaction);
        } else {
            // break;
        }
        // }

        // // Create batch builders for each currently open thread (not over the queued batch limit)
        // let mut all_batches = (0..self.num_execution_threads)
        //     .into_iter()
        //     .filter_map(|execution_thread_index| {
        //         (self.execution_thread_stats[execution_thread_index]
        //             .queued_batches
        //             .len()
        //             < MAX_QUEUED_BATCHES_PER_THREAD)
        //             .then(|| {
        //                 let id = self.next_transaction_batch_id;
        //                 self.next_transaction_batch_id += 1;
        //                 TransactionBatchBuilder {
        //                     id,
        //                     transactions: Vec::with_capacity(self.max_batch_size),
        //                     execution_thread_index,
        //                 }
        //             })
        //     })
        //     .collect::<Vec<_>>();

        // if all_batches.is_empty() {
        //     return;
        // }

        // // Try scheduling highest-priority transactions into a batch
        // let mut batches = all_batches
        //     .iter_mut()
        //     .map(|batch| (batch.execution_thread_index, batch))
        //     .collect::<HashMap<_, _>>();
        // loop {
        //     if batches.is_empty() {
        //         break;
        //     }
        //     let mtx = self.pending_transactions.lock().unwrap().pop();
        //     if let Some(tx) = mtx {
        //         let (_, try_schedule_time) =
        //             measure!(self.try_schedule_transaction(&tx, &mut batches));
        //         self.metrics.scheduling_try_schedule_time_us += try_schedule_time.as_us();
        //     } else {
        //         break;
        //     }
        // }
        // drop(batches);

        // for batch_builder in all_batches {
        //     if batch_builder.transactions.len() > 0 {
        //         // Build the batch
        //         let (batch, transactions) = batch_builder.build(&self.bank);
        //         let batch_id = batch.id;
        //         let execution_thread_index = batch.execution_thread_index;

        //         // Update execution thread stats
        //         let execution_thread_stats =
        //             &mut self.execution_thread_stats[execution_thread_index];
        //         execution_thread_stats.queued_batches.push_back(batch_id);
        //         execution_thread_stats.queued_transactions += transactions.len();

        //         // Insert batch for tracking
        //         self.transaction_batches.insert(batch_id, batch);

        //         // Update metrics
        //         self.num_executing_transactions += transactions.len();
        //         self.metrics.num_transactions_scheduled += transactions.len();
        //         self.metrics.max_blocked_transactions = self
        //             .metrics
        //             .max_blocked_transactions
        //             .max(self.num_blocked_transactions);
        //         self.metrics.max_executing_transactions = self
        //             .num_executing_transactions
        //             .max(self.num_executing_transactions);

        //         // Send the batch
        //         self.transaction_batch_senders[execution_thread_index]
        //             .send((batch_id, transactions));
        //     }
        // }
    }

    /// Try to schedule a transaction
    fn try_schedule_transaction(&mut self, transaction: TransactionRef) {
        // Check for blocking transactions batches in scheduled locks (this includes batches currently being built)
        let (conflicting_batches, get_conflicting_batches_time) =
            measure!(self.get_conflicting_batches(&transaction));
        self.metrics.get_conflicting_batches_time += get_conflicting_batches_time.as_us();

        let maybe_batch_id = if let Some(conflicting_batches) = conflicting_batches.as_ref() {
            let mut schedulable_thread_index = None;
            for batch_id in conflicting_batches {
                let thread_index = self
                    .transaction_batches
                    .get(batch_id)
                    .unwrap()
                    .execution_thread_index;

                if thread_index != *schedulable_thread_index.get_or_insert(thread_index) {
                    schedulable_thread_index = None;
                    break;
                }
            }
            schedulable_thread_index
                .map(|thread_index| {
                    self.execution_thread_stats[thread_index]
                        .queued_batches
                        .back()
                        .unwrap()
                })
                .filter(|batch_id| self.in_progress_batches.contains(*batch_id))
                .cloned()
        } else {
            // Find the lowest-thread in-progress batch
            self.in_progress_batches
                .iter()
                .map(|x| {
                    (
                        self.transaction_batches
                            .get(x)
                            .unwrap()
                            .execution_thread_index,
                        *x,
                    )
                })
                .min()
                .map(|(_, x)| x)
        };

        if maybe_batch_id.is_none() {
            if let Some(conflicting_batches) = conflicting_batches {
                self.num_blocked_transactions += 1;
                let newest_conflicting_batch = conflicting_batches.into_iter().max().unwrap();
                assert!(self
                    .blocked_transactions_by_batch_id
                    .entry(newest_conflicting_batch)
                    .or_default()
                    .insert(transaction.clone()));
            }
            return;
        }

        let batch_id = maybe_batch_id.unwrap();
        // Schedule the transaction:
        let (_, batching_time) = measure!({
            // 1. Add account locks with the batch id
            self.lock_accounts_for_transaction(&transaction, batch_id);
            // 2. Add to Batch
            let batch = self.transaction_batches.get_mut(&batch_id).unwrap();
            assert!(!batch.scheduled);
            batch.transactions.push(transaction);
            // 3. Update queued execution stats
            self.execution_thread_stats[batch.execution_thread_index].queued_compute_units += 1; // TODO: actually use CU instead of # tx
            self.execution_thread_stats[batch.execution_thread_index].queued_transactions += 1;
            // 4. Remove transaction from account queues
            // self.remove_transaction_from_queues(transaction);
            // 5. Unblock transactions that were blocked by this one - they can be scheduled on the same thread now
            // self.unblock_transactions(transaction);
            // 6. Check if batch should be removed from schedulable batches
            // if batch.transactions.len() == self.max_batch_size {
            //     batches.remove(&thread_index);
            // }

            // Check if batch should be sent
            if batch.transactions.len() == self.max_batch_size {
                let batch_id = batch.id;
                self.send_batch(batch_id);
            }
        });
        self.metrics.batching_time += batching_time.as_us();
    }

    /// Checks for blocking transactions in the account queues
    ///     - If blocked, adds to `blocked_transactions`
    // fn check_blocking_transactions(&mut self, transaction: &TransactionRef) -> bool {
    //     if let Some(blocking_transaction) =
    //         self.get_lowest_priority_blocking_transaction(transaction)
    //     {
    //         self.blocked_transactions
    //             .entry(*blocking_transaction.transaction.signature())
    //             .or_default()
    //             .push(transaction.clone());
    //         true
    //     } else {
    //         false
    //     }
    // }

    /// Gets batches that conflict with the current transaction
    ///     - Conflict does not necessarily mean block as they can be scheduled on the same thread
    fn get_conflicting_batches(
        &self,
        transaction: &TransactionRef,
    ) -> Option<HashSet<TransactionBatchId>> {
        let mut conflicting_batches = HashSet::default();

        let account_locks = transaction.transaction.get_account_locks().unwrap();

        // Read accounts will only be blocked by writes on other threads
        for account in account_locks.readonly.into_iter() {
            for batch_id in self
                .transactions_by_account
                .get(account)
                .unwrap()
                .scheduled_lock
                .write_batches
                .iter()
            {
                conflicting_batches.insert(*batch_id);
            }
        }

        // Write accounts will be blocked by reads or writes on other threads
        for account in account_locks.writable.into_iter() {
            let scheduled_lock = &self
                .transactions_by_account
                .get(account)
                .unwrap()
                .scheduled_lock;
            for batch_id in scheduled_lock.write_batches.iter() {
                conflicting_batches.insert(*batch_id);
            }
            for batch_id in scheduled_lock.read_batches.iter() {
                conflicting_batches.insert(*batch_id);
            }
        }

        (conflicting_batches.len() > 0).then(|| conflicting_batches)
    }

    // /// Removes transaction from the account queues since it's been scheduled
    // fn remove_transaction_from_queues(&mut self, transaction: &TransactionRef) {
    //     let account_locks = transaction.transaction.get_account_locks().unwrap();

    //     for account in account_locks.readonly.into_iter() {
    //         self.transactions_by_account
    //             .get_mut(account)
    //             .unwrap()
    //             .reads
    //             .remove(transaction);
    //     }

    //     for account in account_locks.writable.into_iter() {
    //         self.transactions_by_account
    //             .get_mut(account)
    //             .unwrap()
    //             .writes
    //             .remove(transaction);
    //     }
    // }

    /// Unblock transactions blocked by a higher-priority transaction getting picked up for scheduling
    // fn unblock_transactions(&mut self, transaction: &TransactionRef) {
    //     if let Some(blocked_transactions) = self
    //         .blocked_transactions
    //         .remove(transaction.transaction.signature())
    //     {
    //         self.num_blocked_transactions -= blocked_transactions.len();
    //         self.pending_transactions
    //             .lock()
    //             .unwrap()
    //             .extend(blocked_transactions.into_iter());
    //     }
    // }

    /// Locks accounts for a scheduled batch
    fn lock_accounts(&mut self, batch: &TransactionBatch) {
        for (account, lock) in &batch.account_locks {
            self.transactions_by_account
                .get_mut(account)
                .unwrap()
                .scheduled_lock
                .lock_on_batch(batch.id, lock.is_write());
        }
    }

    /// Lock accounts for a transaction by batch id
    fn lock_accounts_for_transaction(
        &mut self,
        transaction: &TransactionRef,
        batch_id: TransactionBatchId,
    ) {
        let accounts = transaction.transaction.get_account_locks().unwrap();
        for account in accounts.readonly {
            self.transactions_by_account
                .get_mut(account)
                .unwrap()
                .scheduled_lock
                .lock_on_batch(batch_id, false);
        }
        for account in accounts.writable {
            self.transactions_by_account
                .get_mut(account)
                .unwrap()
                .scheduled_lock
                .lock_on_batch(batch_id, true);
        }
    }

    // /// Gets the lowest priority transaction that blocks this one
    // fn get_lowest_priority_blocking_transaction(
    //     &self,
    //     transaction: &TransactionRef,
    // ) -> Option<TransactionRef> {
    //     transaction
    //         .transaction
    //         .get_account_locks()
    //         .ok()
    //         .and_then(|account_locks| {
    //             let min_blocking_transaction = account_locks
    //                 .readonly
    //                 .into_iter()
    //                 .map(|account_key| {
    //                     self.transactions_by_account
    //                         .get(account_key)
    //                         .unwrap()
    //                         .get_min_blocking_transaction(transaction, false)
    //                 })
    //                 .fold(None, option_min);

    //             account_locks
    //                 .writable
    //                 .into_iter()
    //                 .map(|account_key| {
    //                     self.transactions_by_account
    //                         .get(account_key)
    //                         .unwrap()
    //                         .get_min_blocking_transaction(transaction, true)
    //                 })
    //                 .fold(min_blocking_transaction, option_min)
    //                 .map(|tx| tx.clone())
    //         })
    // }
}

/// Tracks all pending and blocked transacitons, ordered by priority, for a single account
#[derive(Default)]
struct AccountTransactionQueue {
    // /// Tree of read transactions on the account ordered by fee-priority
    // reads: BTreeSet<TransactionRef>,
    // /// Tree of write transactions on the account ordered by fee-priority
    // writes: BTreeSet<TransactionRef>,
    /// Tracks currently scheduled transactions on the account
    scheduled_lock: AccountLock,
}

impl AccountTransactionQueue {
    /// Unlocks the account queue for `batch_id`
    fn unlock(&mut self, batch_id: TransactionBatchId) {
        self.scheduled_lock.unlock_on_batch(batch_id);
    }

    // /// Find the minimum-priority transaction that blocks this transaction if there is one
    // fn get_min_blocking_transaction(
    //     &self,
    //     transaction: &TransactionRef,
    //     is_write: bool,
    // ) -> Option<TransactionRef> {
    //     let mut min_blocking_transaction = None;
    //     // Write transactions will be blocked by higher-priority reads, but read transactions will not
    //     if is_write {
    //         min_blocking_transaction = option_min(
    //             min_blocking_transaction,
    //             upper_bound(&self.reads, transaction.clone()),
    //         );
    //     }

    //     // All transactions are blocked by higher-priority write-transactions
    //     option_min(
    //         min_blocking_transaction,
    //         upper_bound(&self.writes, transaction.clone()),
    //     )
    //     .map(|txr| txr.clone())
    // }
}

/// Tracks the lock status of an account by batch id
#[derive(Debug, Default)]
struct AccountLock {
    read_batches: HashSet<TransactionBatchId>,
    write_batches: HashSet<TransactionBatchId>,
}

impl AccountLock {
    fn lock_on_batch(&mut self, batch_id: TransactionBatchId, is_write: bool) {
        if is_write {
            // override read lock
            self.read_batches.remove(&batch_id);
            self.write_batches.insert(batch_id);
        } else {
            // underride write lock
            if !self.write_batches.contains(&batch_id) {
                self.read_batches.insert(batch_id);
            }
        }
    }

    fn unlock_on_batch(&mut self, batch_id: TransactionBatchId) {
        self.read_batches.remove(&batch_id);
        self.write_batches.remove(&batch_id);
    }
}

#[derive(Debug, Clone)]
enum AccountLockKind {
    Read,
    Write,
}

impl AccountLockKind {
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

/// Identified for TransactionBatches
pub type TransactionBatchId = usize;

/// Transactions in a batch
#[derive(Debug)]
struct TransactionBatch {
    /// Has the transaction been sent
    scheduled: bool,
    /// Timestamp of the batch starting to be built
    start_time: Instant,
    /// Identifier
    id: TransactionBatchId,
    /// Number of transactions
    num_transactions: usize,
    /// Transactions (only valid before send)
    transactions: Vec<TransactionRef>,
    /// Locked Accounts and Kind Set (only built on send)
    account_locks: HashMap<Pubkey, AccountLockKind>,
    /// Thread it is scheduled on
    execution_thread_index: usize,
}

impl TransactionBatch {
    fn build_account_locks_on_send(&mut self) -> Vec<SanitizedTransaction> {
        self.scheduled = true;
        self.num_transactions = self.transactions.len();
        for transaction in self.transactions.iter() {
            let account_locks = transaction.transaction.get_account_locks().unwrap();

            for account in account_locks.readonly.into_iter() {
                self.account_locks
                    .entry(*account)
                    .or_insert(AccountLockKind::Read);
            }
            for account in account_locks.writable.into_iter() {
                self.account_locks.insert(*account, AccountLockKind::Write);
            }
        }

        let mut transactions = Vec::new();
        std::mem::swap(&mut transactions, &mut self.transactions);

        transactions
            .into_iter()
            .map(|arc_tx| Arc::try_unwrap(arc_tx).unwrap().transaction)
            .collect()
    }
}

/// Track stats for the execution threads - Order of members matters for derived implementations of PartialCmp
#[derive(Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct ExecutionThreadStats {
    /// Currently queued compute-units
    queued_compute_units: usize,
    /// Currently queued number of transactions
    queued_transactions: usize,
    /// Currently queued batch ids
    queued_batches: VecDeque<TransactionBatchId>,
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
    /// Completed transactions - TryRecv time
    compeleted_batch_try_recv_time_us: u64,
    /// Completed transactions - Handle completed batch
    completed_batch_handle_batch_time_us: u64,
    completed_transactions_remove_account_locks_us: u64,
    completed_transactions_unblock_transactions_us: u64,
    /// Total time spent scheduling transactions in microseconds
    scheduling_time_us: u64,
    scheduling_try_schedule_time_us: u64,
    check_blocking_transactions_time_us: u64,
    get_conflicting_batches_time: u64,
    find_thread_index_time: u64,
    batching_time: u64,
    max_batch_age: u64,

    /// Time spent checking and reporting metrics
    metrics_us: u64,
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
            scheduling_time_us: Default::default(),
            compeleted_batch_try_recv_time_us: Default::default(),
            completed_batch_handle_batch_time_us: Default::default(),
            metrics_us: Default::default(),
            completed_transactions_remove_account_locks_us: Default::default(),
            completed_transactions_unblock_transactions_us: Default::default(),
            scheduling_try_schedule_time_us: Default::default(),
            check_blocking_transactions_time_us: Default::default(),
            get_conflicting_batches_time: Default::default(),
            find_thread_index_time: Default::default(),
            batching_time: Default::default(),
            max_batch_age: Default::default(),
        }
    }
}

impl SchedulerMetrics {
    /// Report metrics if the interval has passed and reset metrics
    fn report(&mut self) {
        const REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1000);

        if self.last_reported.elapsed() >= REPORT_INTERVAL {
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
                    "compeleted_batch_try_recv_time_us",
                    self.compeleted_batch_try_recv_time_us as i64,
                    i64
                ),
                (
                    "completed_batch_handle_batch_time_us",
                    self.completed_batch_handle_batch_time_us as i64,
                    i64
                ),
                (
                    "completed_transactions_remove_account_locks_us",
                    self.completed_transactions_remove_account_locks_us as i64,
                    i64
                ),
                (
                    "completed_transactions_unblock_transactions_us",
                    self.completed_transactions_unblock_transactions_us as i64,
                    i64
                ),
                ("scheduling_time_us", self.scheduling_time_us as i64, i64),
                (
                    "scheduling_try_schedule_time_us",
                    self.scheduling_try_schedule_time_us as i64,
                    i64
                ),
                (
                    "check_blocking_transactions_time_us",
                    self.check_blocking_transactions_time_us as i64,
                    i64
                ),
                (
                    "get_conflicting_batches_time",
                    self.get_conflicting_batches_time as i64,
                    i64
                ),
                (
                    "find_thread_index_time",
                    self.find_thread_index_time as i64,
                    i64
                ),
                ("batching_time", self.batching_time as i64, i64),
                ("metrics_us", self.metrics_us as i64, i64),
                ("max_batch_age", self.max_batch_age as i64, i64),
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
            completed_batch_receiver: ct_rx,
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
            scheduler.handle_completed_batch(transaction);
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
