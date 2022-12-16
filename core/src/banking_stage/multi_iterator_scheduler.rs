//! Central scheduler using a multi-iterator to schedule transactions for consuming.
//!

use {
    super::{
        decision_maker::{BufferedPacketsDecision, DecisionMaker},
        scheduler_error::SchedulerError,
        thread_aware_account_locks::{ThreadAwareAccountLocks, ThreadId, ThreadSet},
    },
    crate::{
        multi_iterator_scanner::{MultiIteratorScanner, ProcessingDecision},
        packet_deserializer::{PacketDeserializer, ReceivePacketResults},
        read_write_account_set::ReadWriteAccountSet,
        scheduler_stage::{
            ProcessedTransactions, ProcessedTransactionsReceiver, ScheduledTransactions,
            ScheduledTransactionsSender,
        },
        unprocessed_packet_batches::DeserializedPacket,
    },
    crossbeam_channel::{RecvTimeoutError, TryRecvError},
    itertools::Itertools,
    min_max_heap::MinMaxHeap,
    solana_measure::{measure, measure_us},
    solana_perf::perf_libs,
    solana_poh::poh_recorder::BankStart,
    solana_runtime::{
        bank::{Bank, BankStatusCache},
        blockhash_queue::BlockhashQueue,
        root_bank_cache::RootBankCache,
        transaction_error_metrics::TransactionErrorMetrics,
    },
    solana_sdk::{
        clock::{
            FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET, MAX_PROCESSING_AGE,
            MAX_TRANSACTION_FORWARDING_DELAY, MAX_TRANSACTION_FORWARDING_DELAY_GPU,
        },
        nonce::state::DurableNonce,
        timing::AtomicInterval,
        transaction::SanitizedTransaction,
    },
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
};

const BATCH_SIZE: u32 = 64;

#[derive(Debug, PartialEq, Eq)]
struct TransactionPacket {
    packet: DeserializedPacket,
    transaction: SanitizedTransaction,
}

impl PartialOrd for TransactionPacket {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TransactionPacket {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.packet.cmp(&other.packet)
    }
}

pub struct MultiIteratorScheduler {
    /// Number of executing threads
    num_threads: usize,
    /// Makes decisions about whether to consume or forward packets
    decision_maker: DecisionMaker,
    /// Priority queue of buffered packets and sanitized transactions
    priority_queue: MinMaxHeap<TransactionPacket>,
    /// Receiver for packets from sigverify
    packet_deserializer: PacketDeserializer,
    /// Cached root bank for sanitizing transactions - updates only as new root banks are set
    root_bank_cache: RootBankCache,
    /// Sends scheduled transactions to the execution threads for consuming or forwarding
    transaction_senders: Vec<ScheduledTransactionsSender>,
    /// Scheduled account locks for pending transactions
    account_locks: ThreadAwareAccountLocks,
    /// Receives processed transactions from the execution threads
    processed_transaction_receiver: ProcessedTransactionsReceiver,
    /// Scheduling metrics
    metrics: MultiIteratorSchedulerMetrics,
}

impl MultiIteratorScheduler {
    pub fn new(
        num_threads: usize,
        decision_maker: DecisionMaker,
        packet_deserializer: PacketDeserializer,
        root_bank_cache: RootBankCache,
        transaction_senders: Vec<ScheduledTransactionsSender>,
        processed_transaction_receiver: ProcessedTransactionsReceiver,
        buffer_capacity: usize,
    ) -> Self {
        assert_eq!(num_threads, transaction_senders.len());
        Self {
            num_threads,
            decision_maker,
            priority_queue: MinMaxHeap::with_capacity(buffer_capacity),
            packet_deserializer,
            root_bank_cache,
            transaction_senders,
            account_locks: ThreadAwareAccountLocks::new(num_threads as u8),
            processed_transaction_receiver,
            metrics: MultiIteratorSchedulerMetrics::default(),
        }
    }

    pub fn run(mut self) {
        loop {
            let decision = self.decision_maker.make_consume_or_forward_decision();
            match decision {
                BufferedPacketsDecision::Consume(bank_start) => self.schedule_consume(bank_start),
                BufferedPacketsDecision::Forward => self.schedule_forward(false),
                BufferedPacketsDecision::ForwardAndHold => self.schedule_forward(true),
                BufferedPacketsDecision::Hold => {}
            }

            let (receive_result, receive_time_us) = measure_us!(self.receive_and_buffer_packets());
            self.metrics.receive_packet_time_us += receive_time_us;
            if receive_result.is_err() {
                break;
            }

            if self.receive_processed_transactions().is_err() {
                break;
            }

            self.metrics.report(1000);
        }
    }

    fn schedule_consume(&mut self, bank_start: BankStart) {
        let decision = BufferedPacketsDecision::Consume(bank_start.clone());

        // Drain priority queue into a vector of transactions
        let (transaction_packets, queue_drain_time_us) =
            measure_us!(self.priority_queue.drain_desc().collect_vec());
        self.metrics.max_consumed_buffer_size = self
            .metrics
            .max_consumed_buffer_size
            .max(transaction_packets.len());
        self.metrics.consume_drain_queue_time_us += queue_drain_time_us;

        // Create a multi-iterator scanner over the transactions
        let mut scanner = MultiIteratorScanner::new(
            &transaction_packets,
            self.num_threads * BATCH_SIZE as usize,
            MultiIteratorSchedulerConsumePayload::new(self),
            #[inline(always)]
            |transaction_packet: &TransactionPacket,
             payload: &mut MultiIteratorSchedulerConsumePayload| {
                Self::should_consume(transaction_packet, payload)
            },
        );

        // Validity check for sent transactions
        let validity_check = Arc::new(AtomicBool::new(true));

        // Loop over batches of transactions
        let mut iterate_time = Instant::now();
        while let Some((transactions, payload)) = scanner.iterate() {
            // Grab reference to scheduler from payload
            let scheduler = &mut *payload.scheduler;

            assert_eq!(transactions.len(), payload.thread_indices.len()); // TOOD: Remove after testing

            let iterate_time_us = iterate_time.elapsed().as_micros() as u64;
            scheduler.metrics.total_consume_iterator_time_us += iterate_time_us;
            scheduler.metrics.max_consume_iterator_time_us = scheduler
                .metrics
                .max_consume_iterator_time_us
                .max(iterate_time_us);

            // TODO: Consider receiving and unlocking processed transactions here
            // NOTE: If we do this, we need to update the priority queue extend below

            // Batches of transactions to send
            let mut batches = (0..scheduler.num_threads)
                .map(|idx| {
                    ScheduledTransactions::with_capacity(
                        idx as ThreadId,
                        decision.clone(),
                        BATCH_SIZE as usize,
                        validity_check.clone(),
                    )
                })
                .collect_vec();

            // Move all transactions into their respective batches
            // TODO: Optimize cloning - sanitized transaction clone.
            for (transaction, thread_index) in
                transactions.iter().zip(payload.thread_indices.iter())
            {
                batches[*thread_index as usize]
                    .packets
                    .push(transaction.packet.clone());
                batches[*thread_index as usize]
                    .transactions
                    .push(transaction.transaction.clone());
            }

            // Send batches to the execution threads
            for (thread_index, batch) in batches.into_iter().enumerate() {
                if batch.packets.is_empty() {
                    continue;
                }

                let packet_count = batch.packets.len();
                scheduler.metrics.consumed_batch_count += 1;
                scheduler.metrics.consumed_packet_count += packet_count;
                scheduler.metrics.consumed_min_batch_size =
                    scheduler.metrics.consumed_min_batch_size.min(packet_count);
                scheduler.metrics.consumed_max_batch_size =
                    scheduler.metrics.consumed_min_batch_size.max(packet_count);

                scheduler.transaction_senders[thread_index]
                    .send(batch)
                    .expect("transaction sender should be connected");
            }

            // Reset the iterator payload for next iteration
            payload.reset();

            // Check if we've reached the end of the slot
            if bank_start.reached_max_tick_height()
                || !bank_start.should_working_bank_still_be_processing_txs()
            {
                validity_check.store(false, Ordering::Relaxed);
                break;
            }

            // Stop scheduling if a scheduled transaction batch was invalidated
            if !validity_check.load(Ordering::Relaxed) {
                break;
            }

            // Reset time for next iteration
            iterate_time = Instant::now();
        }

        // Get the final payload from the scanner and whether or not each packet was handled
        let (_payload, already_handled) = scanner.finalize();

        // Push unprocessed packets back into the priority queue
        let (_, push_queue_time_us) = measure_us!({
            self.priority_queue.extend(
                transaction_packets
                    .into_iter()
                    .zip(already_handled.into_iter())
                    .filter(|(_, handled)| !handled)
                    .map(|(transaction_packet, _)| transaction_packet),
            )
        });
        self.metrics.consume_push_queue_time_us += push_queue_time_us;
    }

    fn schedule_forward(&mut self, hold: bool) {
        let decision = if hold {
            BufferedPacketsDecision::ForwardAndHold
        } else {
            BufferedPacketsDecision::Forward
        };

        // Drain priority queue into a vector of transactions
        let transaction_packets = self.priority_queue.drain_desc().collect_vec();

        // Grab read locks for checking age and status cache
        let root_bank = self.root_bank_cache.root_bank();
        let blockhash_queue = root_bank.blockhash_queue.read().unwrap();
        let status_cache = root_bank.status_cache.read().unwrap();
        let next_durable_nonce = DurableNonce::from_blockhash(&blockhash_queue.last_hash());

        // Calculate max forwarding age
        let max_age = (MAX_PROCESSING_AGE)
            .saturating_sub(if perf_libs::api().is_some() {
                MAX_TRANSACTION_FORWARDING_DELAY
            } else {
                MAX_TRANSACTION_FORWARDING_DELAY_GPU
            })
            .saturating_sub(FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET as usize);
        let mut error_counters = TransactionErrorMetrics::default();

        // Create a multi-iterator scanner over the transactions
        let mut scanner = MultiIteratorScanner::new(
            &transaction_packets,
            self.num_threads * BATCH_SIZE as usize,
            MultiIteratorSchedulerForwardPayload::default(),
            #[inline(always)]
            |transaction_packet: &TransactionPacket,
             payload: &mut MultiIteratorSchedulerForwardPayload| {
                Self::should_forward(
                    transaction_packet,
                    &root_bank,
                    &blockhash_queue,
                    &status_cache,
                    &next_durable_nonce,
                    max_age,
                    &mut error_counters,
                    payload,
                )
            },
        );

        let validity_check = Arc::new(AtomicBool::new(true));
        // Loop over batches of transactions
        while let Some((transactions, payload)) = scanner.iterate() {
            // Create batches to fill with transactions
            let mut batches = (0..self.num_threads)
                .map(|idx| {
                    ScheduledTransactions::with_capacity(
                        idx as ThreadId,
                        decision.clone(),
                        BATCH_SIZE as usize,
                        validity_check.clone(),
                    )
                })
                .collect_vec();

            // Fill batches - striped access
            for (index, transaction) in transactions.iter().copied().enumerate() {
                let batch = &mut batches[index % self.num_threads];
                batch.packets.push(transaction.packet.clone());
                batch.transactions.push(transaction.transaction.clone());
            }

            // Send batches to the execution threads
            for (thread_index, batch) in batches.into_iter().enumerate() {
                if batch.packets.is_empty() {
                    continue;
                }

                self.transaction_senders[thread_index]
                    .send(batch)
                    .expect("transaction sender should be connected")
            }

            // Reset the iterator payload for next iteration
            payload.reset();
        }

        // Get the final payload from the scanner and whether or not each packet was handled
        let (_payload, _already_handled) = scanner.finalize();
    }

    fn receive_and_buffer_packets(&mut self) -> Result<(), SchedulerError> {
        const EMPTY_RECEIVE_TIMEOUT: Duration = Duration::from_millis(100);
        const NON_EMPTY_RECEIVE_TIMEOUT: Duration = Duration::from_millis(0);
        let timeout = if self.priority_queue.is_empty() {
            EMPTY_RECEIVE_TIMEOUT
        } else {
            NON_EMPTY_RECEIVE_TIMEOUT
        };
        let remaining_capacity = self.remaining_capacity();

        match self
            .packet_deserializer
            .handle_received_packets(timeout, remaining_capacity)
        {
            Ok(receive_packet_results) => self.insert_received_packets(receive_packet_results),
            Err(RecvTimeoutError::Disconnected) => {
                return Err(SchedulerError::PacketReceiverDisconnected)
            }
            Err(RecvTimeoutError::Timeout) => {}
        }

        Ok(())
    }

    fn receive_processed_transactions(&mut self) -> Result<(), SchedulerError> {
        loop {
            let processed_transactions = self.processed_transaction_receiver.try_recv();
            match processed_transactions {
                Ok(processed_transactions) => {
                    self.handle_processed_transactions(processed_transactions)?;
                }
                Err(TryRecvError::Disconnected) => {
                    return Err(SchedulerError::ProcessedTransactionsReceiverDisconnected);
                }
                Err(TryRecvError::Empty) => break,
            }
        }
        Ok(())
    }

    fn handle_processed_transactions(
        &mut self,
        processed_transactions: ProcessedTransactions,
    ) -> Result<(), SchedulerError> {
        for ((packet, transaction), retryable) in processed_transactions
            .packets
            .into_iter()
            .zip(processed_transactions.transactions.into_iter())
            .zip(processed_transactions.retryable.into_iter())
        {
            // Unlock accounts
            let transaction_account_locks = transaction.get_account_locks_unchecked();
            self.account_locks.unlock_accounts(
                transaction_account_locks.writable.into_iter(),
                transaction_account_locks.readonly.into_iter(),
                processed_transactions.thread_id,
            );

            // Push retryable packets back into the buffer
            if retryable {
                self.metrics.retryable_packet_count += 1;
                self.push_priority_queue(TransactionPacket {
                    packet,
                    transaction,
                });
            }
        }

        Ok(())
    }

    /// Inserts received packets, after sanitizing the transactions, into the priority queue.
    // TODO: Collect stats
    fn insert_received_packets(&mut self, receive_packet_results: ReceivePacketResults) {
        let root_bank = self.root_bank_cache.root_bank();
        let tx_account_lock_limit = root_bank.get_transaction_account_lock_limit();

        self.metrics.received_packet_count += receive_packet_results.deserialized_packets.len();

        for (packet, transaction) in receive_packet_results
            .deserialized_packets
            .into_iter()
            .filter_map(|packet| {
                packet
                    .build_sanitized_transaction(
                        &root_bank.feature_set,
                        root_bank.vote_only_bank(),
                        root_bank.as_ref(),
                    )
                    .map(|tx| (packet, tx))
            })
            .filter(|(_, transaction)| {
                SanitizedTransaction::validate_account_locks(
                    transaction.message(),
                    tx_account_lock_limit,
                )
                .is_ok()
            })
        {
            let transaction_packet = TransactionPacket {
                packet: DeserializedPacket::from_immutable_section(packet),
                transaction,
            };

            self.push_priority_queue(transaction_packet);
        }
    }

    fn remaining_capacity(&self) -> usize {
        self.priority_queue.capacity() - self.priority_queue.len()
    }

    fn should_consume(
        transaction_packet: &TransactionPacket,
        payload: &mut MultiIteratorSchedulerConsumePayload,
    ) -> ProcessingDecision {
        // If locks clash with the current batch of transactions, then we should process
        // the transaction later.
        let (has_batch_conflict, conflict_batch_time_us) = measure_us!(!payload
            .batch_account_locks
            .check_sanitized_message_account_locks(transaction_packet.transaction.message()));
        payload.scheduler.metrics.conflict_batch_time_us += conflict_batch_time_us;

        if has_batch_conflict {
            payload.scheduler.metrics.conflict_batch_count += 1;
            return ProcessingDecision::Later;
        }

        // Check if we can schedule to any thread.
        let transaction_account_locks =
            transaction_packet.transaction.get_account_locks_unchecked();

        let (schedulable_threads, schedulable_threads_time_us) = measure_us!(payload
            .scheduler
            .account_locks
            .accounts_schedulable_threads(
                transaction_account_locks.writable.iter().copied(),
                transaction_account_locks.readonly.iter().copied(),
            ));

        // Combine with non-full threads
        let schedulable_threads = schedulable_threads & payload.schedulable_threads;
        payload.scheduler.metrics.conflict_locks_time_us += schedulable_threads_time_us;

        if schedulable_threads.is_empty() {
            payload.scheduler.metrics.conflict_locks_count += 1;
            return ProcessingDecision::Later;
        }

        // Iterate over schedulable threads and find the thread with the least number of transactions in current batch.
        // TODO: Might want to also consider the number of transactions in the queue.
        let thread_id = schedulable_threads
            .threads_iter()
            .map(|thread_id| (thread_id, payload.batch_counts[thread_id as usize]))
            .min_by_key(|(_, count)| *count)
            .unwrap()
            .0;

        // Take locks
        payload
            .batch_account_locks
            .add_sanitized_message_account_locks(transaction_packet.transaction.message());
        payload.scheduler.account_locks.lock_accounts(
            transaction_account_locks.writable.into_iter(),
            transaction_account_locks.readonly.into_iter(),
            thread_id,
        );

        // Update payload
        payload.thread_indices.push(thread_id);
        payload.batch_counts[thread_id as usize] += 1;
        if payload.batch_counts[thread_id as usize] == BATCH_SIZE {
            payload.schedulable_threads.remove(thread_id);
        }

        ProcessingDecision::Now
    }

    fn should_forward(
        transaction_packet: &TransactionPacket,
        bank: &Bank,
        blockhash_queue: &BlockhashQueue,
        status_cache: &BankStatusCache,
        next_durable_nonce: &DurableNonce,
        max_age: usize,
        error_counters: &mut TransactionErrorMetrics,
        payload: &mut MultiIteratorSchedulerForwardPayload,
    ) -> ProcessingDecision {
        // Throw out transactions that are too old.
        if bank
            .check_transaction_age(
                &transaction_packet.transaction,
                max_age,
                next_durable_nonce,
                blockhash_queue,
                error_counters,
            )
            .0
            .is_err()
        {
            return ProcessingDecision::Never;
        }

        // If the transaction is already in the bank, then we don't need to forward it.
        if bank.is_transaction_already_processed(&transaction_packet.transaction, status_cache) {
            error_counters.already_processed += 1;
            return ProcessingDecision::Never;
        }

        // If locks clash with the current batch of transactions, then we should forward
        // the transaction later.
        if payload
            .account_locks
            .try_locking(transaction_packet.transaction.message())
        {
            ProcessingDecision::Now
        } else {
            ProcessingDecision::Later
        }
    }

    fn push_priority_queue(&mut self, transaction_packet: TransactionPacket) {
        if self.priority_queue.len() == self.priority_queue.capacity() {
            self.priority_queue.push_pop_min(transaction_packet);
        } else {
            self.priority_queue.push(transaction_packet);
        }
    }
}

struct MultiIteratorSchedulerConsumePayload<'a> {
    /// Mutable reference to the scheduler struct
    scheduler: &'a mut MultiIteratorScheduler,
    /// Read and write accounts that are used by the current batch of transactions.
    batch_account_locks: ReadWriteAccountSet,
    /// Thread index for each transaction in the batch.
    thread_indices: Vec<ThreadId>,
    /// Batch counts
    batch_counts: Vec<u32>,
    /// Schedulable threads (based on batch_counts)
    schedulable_threads: ThreadSet,
}

impl<'a> MultiIteratorSchedulerConsumePayload<'a> {
    fn new(scheduler: &'a mut MultiIteratorScheduler) -> Self {
        let num_threads = scheduler.num_threads;
        Self {
            scheduler,
            batch_account_locks: ReadWriteAccountSet::default(),
            thread_indices: Vec::with_capacity(num_threads * BATCH_SIZE as usize),
            batch_counts: vec![0; num_threads],
            schedulable_threads: ThreadSet::any(num_threads as u8),
        }
    }

    fn reset(&mut self) {
        self.batch_account_locks.clear();
        self.thread_indices.clear();
        self.batch_counts.fill(0);
        self.schedulable_threads = ThreadSet::any(self.scheduler.num_threads as u8);
    }
}

#[derive(Default)]
struct MultiIteratorSchedulerForwardPayload {
    /// Account locks used to prevent us from spam forwarding hot accounts
    account_locks: ReadWriteAccountSet,
}

impl MultiIteratorSchedulerForwardPayload {
    fn reset(&mut self) {
        self.account_locks.clear();
    }
}

struct MultiIteratorSchedulerMetrics {
    last_reported: AtomicInterval,
    received_packet_count: usize,
    receive_packet_time_us: u64,
    conflict_batch_count: usize,
    conflict_locks_count: usize,
    conflict_batch_time_us: u64,
    conflict_locks_time_us: u64,
    consumed_batch_count: usize,
    consumed_packet_count: usize,
    consumed_min_batch_size: usize,
    consumed_max_batch_size: usize,
    max_consumed_buffer_size: usize,
    max_consume_iterator_time_us: u64,
    total_consume_iterator_time_us: u64,
    consume_drain_queue_time_us: u64,
    consume_push_queue_time_us: u64,
    retryable_packet_count: usize,
}

impl Default for MultiIteratorSchedulerMetrics {
    fn default() -> Self {
        Self {
            last_reported: AtomicInterval::default(),
            received_packet_count: 0,
            receive_packet_time_us: 0,
            conflict_batch_count: 0,
            conflict_locks_count: 0,
            conflict_batch_time_us: 0,
            conflict_locks_time_us: 0,
            consumed_batch_count: 0,
            consumed_packet_count: 0,
            consumed_min_batch_size: usize::MAX,
            consumed_max_batch_size: 0,
            max_consumed_buffer_size: 0,
            max_consume_iterator_time_us: 0,
            consume_drain_queue_time_us: 0,
            consume_push_queue_time_us: 0,
            total_consume_iterator_time_us: 0,
            retryable_packet_count: 0,
        }
    }
}

impl MultiIteratorSchedulerMetrics {
    fn report(&mut self, report_interval_ms: u64) {
        if self.last_reported.should_update(report_interval_ms) {
            datapoint_info!(
                "multi_iterator_scheduler",
                ("received_packet_count", self.received_packet_count, i64),
                ("receive_packet_time_us", self.receive_packet_time_us, i64),
                ("conflict_batch_count", self.conflict_batch_count, i64),
                ("conflict_locks_count", self.conflict_locks_count, i64),
                ("conflict_batch_time_us", self.conflict_batch_time_us, i64),
                ("conflict_locks_time_us", self.conflict_locks_time_us, i64),
                ("consumed_batch_count", self.consumed_batch_count, i64),
                ("consumed_packet_count", self.consumed_packet_count, i64),
                ("consumed_min_batch_size", self.consumed_min_batch_size, i64),
                ("consumed_max_batch_size", self.consumed_max_batch_size, i64),
                (
                    "max_consumed_buffer_size",
                    self.max_consumed_buffer_size,
                    i64
                ),
                (
                    "total_consume_iterator_time_us",
                    self.total_consume_iterator_time_us,
                    i64
                ),
                (
                    "max_consume_iterator_time_us",
                    self.max_consume_iterator_time_us,
                    i64
                ),
                (
                    "consume_drain_queue_time_us",
                    self.consume_drain_queue_time_us,
                    i64
                ),
                (
                    "consume_push_queue_time_us",
                    self.consume_push_queue_time_us,
                    i64
                ),
                ("retryable_packet_count", self.retryable_packet_count, i64),
            );
            self.reset();
        }
    }

    fn reset(&mut self) {
        self.received_packet_count = 0;
        self.receive_packet_time_us = 0;
        self.conflict_batch_count = 0;
        self.conflict_locks_count = 0;
        self.conflict_batch_time_us = 0;
        self.conflict_locks_time_us = 0;
        self.consumed_batch_count = 0;
        self.consumed_packet_count = 0;
        self.consumed_min_batch_size = usize::MAX;
        self.consumed_max_batch_size = 0;
        self.max_consumed_buffer_size = 0;
        self.total_consume_iterator_time_us = 0;
        self.max_consume_iterator_time_us = 0;
        self.consume_drain_queue_time_us = 0;
        self.consume_push_queue_time_us = 0;
        self.retryable_packet_count = 0;
    }
}
