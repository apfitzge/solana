//! Central scheduler using a multi-iterator to schedule transactions for consuming.
//!

use {
    super::{
        decision_maker::{BufferedPacketsDecision, DecisionMaker},
        scheduler_error::SchedulerError,
        thread_aware_account_locks::{ThreadAwareAccountLocks, ThreadId, ThreadSet, MAX_THREADS},
    },
    crate::{
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        multi_iterator_scanner::{MultiIteratorScanner, ProcessingDecision},
        packet_deserializer::{PacketDeserializer, ReceivePacketResults},
        read_write_account_set::ReadWriteAccountSet,
        scheduler_stage::{
            ProcessedTransactions, ProcessedTransactionsReceiver, ScheduledTransactions,
            ScheduledTransactionsSender,
        },
    },
    crossbeam_channel::{RecvTimeoutError, TryRecvError},
    itertools::Itertools,
    min_max_heap::MinMaxHeap,
    solana_poh::poh_recorder::BankStart,
    solana_runtime::root_bank_cache::RootBankCache,
    solana_sdk::transaction::SanitizedTransaction,
    std::{sync::Arc, time::Duration},
};

const BATCH_SIZE: usize = 64;

#[derive(Debug, PartialEq, Eq)]
struct TransactionPacket {
    packet: Arc<ImmutableDeserializedPacket>,
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
            account_locks: ThreadAwareAccountLocks::default(),
            processed_transaction_receiver,
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

            if self.receive_and_buffer_packets().is_err() {
                break;
            }
            if self.receive_processed_transactions().is_err() {
                break;
            }
        }
    }

    fn schedule_consume(&mut self, bank_start: BankStart) {
        let decision = BufferedPacketsDecision::Consume(bank_start.clone());

        // Drain priority queue into a vector of transactions
        let transaction_packets = self.priority_queue.drain_desc().collect_vec();

        // Create a multi-iterator scanner over the transactions
        let mut scanner = MultiIteratorScanner::new(
            &transaction_packets,
            self.num_threads * BATCH_SIZE,
            MultiIteratorSchedulerPayload::default(),
            #[inline(always)]
            |transaction_packet: &TransactionPacket,
             payload: &mut MultiIteratorSchedulerPayload| {
                self.should_process(transaction_packet, payload)
            },
        );

        // Loop over batches of transactions
        while let Some((transactions, payload)) = scanner.iterate() {
            assert_eq!(transactions.len(), payload.thread_indices.len()); // TOOD: Remove after testing

            // TODO: Consider receiving and unlocking processed transactions here

            // Batches of transactions to send
            let mut batches = (0..self.num_threads)
                .map(|idx| {
                    ScheduledTransactions::with_capacity(
                        idx as ThreadId,
                        decision.clone(),
                        BATCH_SIZE,
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

                self.transaction_senders[thread_index]
                    .send(batch)
                    .expect("transaction sender should be connected")
            }

            // Reset the iterator payload for next iteration
            payload.reset();

            // Check if we've reached the end of the slot
            if !bank_start.should_working_bank_still_be_processing_txs() {
                break;
            }
        }

        // Get the final payload from the scanner and whether or not each packet was handled
        let (_payload, already_handled) = scanner.finalize();

        // Push unprocessed packets back into the priority queue
        for (transaction_packet, handled) in transaction_packets
            .into_iter()
            .zip(already_handled.into_iter())
        {
            if !handled {
                self.priority_queue.push(transaction_packet);
            }
        }
    }

    fn schedule_forward(&mut self, _hold: bool) {
        todo!()
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
                self.priority_queue.push(TransactionPacket {
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
                packet: Arc::new(packet),
                transaction,
            };
            if self.priority_queue.capacity() == self.priority_queue.len() {
                self.priority_queue.push_pop_min(transaction_packet);
            } else {
                self.priority_queue.push(transaction_packet);
            }
        }
    }

    fn remaining_capacity(&self) -> usize {
        self.priority_queue.capacity() - self.priority_queue.len()
    }

    fn should_process(
        &self,
        transaction_packet: &TransactionPacket,
        payload: &mut MultiIteratorSchedulerPayload,
    ) -> ProcessingDecision {
        // If locks clash with the current batch of transactions, then we should process
        // the transaction later.
        if !payload
            .account_locks
            .check_sanitized_message_account_locks(transaction_packet.transaction.message())
        {
            return ProcessingDecision::Later;
        }

        // Check if we can schedule to any thread.
        let transaction_account_locks =
            transaction_packet.transaction.get_account_locks_unchecked();
        let schedulable_threads = self.account_locks.accounts_schedulable_threads(
            transaction_account_locks.writable.into_iter(),
            transaction_account_locks.readonly.into_iter(),
        );

        // Combine with non-full threads
        let schedulable_threads = schedulable_threads & payload.schedulable_threads;

        if schedulable_threads.is_empty() {
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

        // Update payload
        payload
            .account_locks
            .add_sanitized_message_account_locks(transaction_packet.transaction.message());
        payload.batch_counts[thread_id as usize] += 1;
        if payload.batch_counts[thread_id as usize] == BATCH_SIZE {
            payload.schedulable_threads.remove(thread_id);
        }

        ProcessingDecision::Now
    }
}

struct MultiIteratorSchedulerPayload {
    /// Read and write accounts that are used by the current batch of transactions.
    account_locks: ReadWriteAccountSet,
    /// Thread index for each transaction in the batch.
    thread_indices: Vec<ThreadId>,
    /// Batch counts
    batch_counts: [usize; MAX_THREADS],
    /// Schedulable threads (based on batch_counts)
    schedulable_threads: ThreadSet,
}

impl MultiIteratorSchedulerPayload {
    fn reset(&mut self) {
        self.account_locks.clear();
        self.thread_indices.clear();
        self.batch_counts.fill(0);
        self.schedulable_threads = ThreadSet::any();
    }
}

impl Default for MultiIteratorSchedulerPayload {
    fn default() -> Self {
        Self {
            account_locks: ReadWriteAccountSet::default(),
            thread_indices: Vec::with_capacity(MAX_THREADS * BATCH_SIZE),
            batch_counts: [0; MAX_THREADS],
            schedulable_threads: ThreadSet::any(),
        }
    }
}
