use {
    super::{
        in_flight_tracker::InFlightTracker,
        prio_graph_scheduler::{
            Batches, PrioGraphScheduler, TransactionSchedulingError, TransactionSchedulingInfo,
        },
        scheduler::{Scheduler, SchedulingSummary},
        scheduler_error::SchedulerError,
        thread_aware_account_locks::{ThreadAwareAccountLocks, ThreadId, ThreadSet},
        transaction_priority_id::TransactionPriorityId,
        transaction_state::{SanitizedTransactionTTL, TransactionState},
        transaction_state_container::TransactionStateContainer,
    },
    crate::banking_stage::{
        consumer::TARGET_NUM_TRANSACTIONS_PER_BATCH,
        read_write_account_set::ReadWriteAccountSet,
        scheduler_messages::{ConsumeWork, FinishedConsumeWork, TransactionBatchId},
        transaction_scheduler::thread_aware_account_locks::MAX_THREADS,
    },
    crossbeam_channel::{Receiver, Sender, TryRecvError},
    itertools::izip,
    solana_cost_model::block_cost_limits::MAX_BLOCK_UNITS,
    solana_sdk::{saturating_add_assign, transaction::SanitizedTransaction},
    solana_svm_transaction::svm_message::SVMMessage,
};

pub(crate) struct GreedySchedulerConfig {
    pub target_scheduled_cus: u64,
    pub max_scanned_transactions_per_scheduling_pass: usize,
    pub target_transactions_per_batch: usize,
}

impl Default for GreedySchedulerConfig {
    fn default() -> Self {
        Self {
            target_scheduled_cus: MAX_BLOCK_UNITS / 4,
            max_scanned_transactions_per_scheduling_pass: 100_000,
            target_transactions_per_batch: TARGET_NUM_TRANSACTIONS_PER_BATCH,
        }
    }
}

/// Dead-simple scheduler that is efficient and will attempt to schedule
/// in priority order, scheduling anything that can be immediately
/// scheduled, up to the limits.
pub struct GreedyScheduler {
    in_flight_tracker: InFlightTracker,
    account_locks: ThreadAwareAccountLocks,
    consume_work_senders: Vec<Sender<ConsumeWork>>,
    finished_consume_work_receiver: Receiver<FinishedConsumeWork>,
    working_account_set: ReadWriteAccountSet,
    unschedulables: Vec<TransactionPriorityId>,
    config: GreedySchedulerConfig,
}

impl GreedyScheduler {
    pub(crate) fn new(
        consume_work_senders: Vec<Sender<ConsumeWork>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork>,
        config: GreedySchedulerConfig,
    ) -> Self {
        let num_threads = consume_work_senders.len();
        assert!(num_threads > 0, "must have at least one worker");
        assert!(
            num_threads <= MAX_THREADS,
            "cannot have more than {MAX_THREADS} workers"
        );
        Self {
            in_flight_tracker: InFlightTracker::new(num_threads),
            account_locks: ThreadAwareAccountLocks::new(num_threads),
            consume_work_senders,
            finished_consume_work_receiver,
            working_account_set: ReadWriteAccountSet::default(),
            unschedulables: Vec::with_capacity(config.max_scanned_transactions_per_scheduling_pass),
            config,
        }
    }
}

impl Scheduler for GreedyScheduler {
    fn schedule(
        &mut self,
        container: &mut TransactionStateContainer,
        _pre_graph_filter: impl Fn(&[&SanitizedTransaction], &mut [bool]),
        pre_lock_filter: impl Fn(&SanitizedTransaction) -> bool,
    ) -> Result<SchedulingSummary, SchedulerError> {
        let num_threads = self.consume_work_senders.len();
        let target_cu_per_thread = self.config.target_scheduled_cus / num_threads as u64;

        let mut schedulable_threads = ThreadSet::any(num_threads);
        for thread_id in 0..num_threads {
            if self.in_flight_tracker.cus_in_flight_per_thread()[thread_id] >= target_cu_per_thread
            {
                schedulable_threads.remove(thread_id);
            }
        }
        if schedulable_threads.is_empty() {
            return Ok(SchedulingSummary::default());
        }

        // Track metrics on filter.
        let mut num_filtered_out: usize = 0;
        let mut num_scanned: usize = 0;
        let mut num_scheduled: usize = 0;
        let mut num_sent: usize = 0;
        let mut num_unschedulable: usize = 0;

        let mut batches = Batches::new(num_threads);
        while num_scanned < self.config.max_scanned_transactions_per_scheduling_pass
            && !schedulable_threads.is_empty()
            && !container.is_empty()
        {
            let Some(id) = container.pop() else {
                unreachable!("container is not empty")
            };

            num_scanned += 1;

            // Should always be in the container, during initial testing phase panic.
            // Later, we can replace with a continue in case this does happen.
            let Some(transaction_state) = container.get_mut_transaction_state(&id.id) else {
                panic!("transaction state must exist")
            };

            // If there is a conflict with any of the transactions in the current batches,
            // we should immediately send out the batches, so this transaction may be scheduled.
            if !self
                .working_account_set
                .check_locks(transaction_state.transaction_ttl().transaction.message())
            {
                self.working_account_set.clear();
                num_sent += self.send_batches(&mut batches)?;
            }

            // Now check if the transaction can actually be scheduled.
            match try_schedule_transaction(
                transaction_state,
                &pre_lock_filter,
                &mut self.account_locks,
                schedulable_threads,
                |thread_set| {
                    PrioGraphScheduler::select_thread(
                        thread_set,
                        &batches.total_cus,
                        self.in_flight_tracker.cus_in_flight_per_thread(),
                        &batches.transactions,
                        self.in_flight_tracker.num_in_flight_per_thread(),
                    )
                },
            ) {
                Err(TransactionSchedulingError::Filtered) => {
                    num_filtered_out += 1;
                    container.remove_by_id(&id.id);
                }
                Err(TransactionSchedulingError::UnschedulableConflicts) => {
                    num_unschedulable += 1;
                    self.unschedulables.push(id);
                }
                Ok(TransactionSchedulingInfo {
                    thread_id,
                    transaction,
                    max_age,
                    cost,
                }) => {
                    assert!(
                        self.working_account_set.take_locks(transaction.message()),
                        "locks must be available"
                    );
                    saturating_add_assign!(num_scheduled, 1);
                    batches.transactions[thread_id].push(transaction);
                    batches.ids[thread_id].push(id.id);
                    batches.max_ages[thread_id].push(max_age);
                    saturating_add_assign!(batches.total_cus[thread_id], cost);

                    // If target batch size is reached, send all the batches
                    if batches.ids[thread_id].len() >= self.config.target_transactions_per_batch {
                        self.working_account_set.clear();
                        num_sent += self.send_batches(&mut batches)?;
                    }

                    // if the thread is at target_cu_per_thread, remove it from the schedulable threads
                    // if there are no more schedulable threads, stop scheduling.
                    if self.in_flight_tracker.cus_in_flight_per_thread()[thread_id]
                        + batches.total_cus[thread_id]
                        >= target_cu_per_thread
                    {
                        schedulable_threads.remove(thread_id);
                        if schedulable_threads.is_empty() {
                            break;
                        }
                    }
                }
            }
        }

        self.working_account_set.clear();
        num_sent += self.send_batches(&mut batches)?;
        assert_eq!(
            num_scheduled, num_sent,
            "number of scheduled and sent transactions must match"
        );

        // Push unschedulables back into the queue
        for id in self.unschedulables.drain(..) {
            container.push_id_into_queue(id);
        }

        Ok(SchedulingSummary {
            num_scheduled,
            num_unschedulable,
            num_filtered_out,
            filter_time_us: 0,
        })
    }

    /// Receive completed batches of transactions without blocking.
    /// Returns (num_transactions, num_retryable_transactions) on success.
    fn receive_completed(
        &mut self,
        container: &mut TransactionStateContainer,
    ) -> Result<(usize, usize), SchedulerError> {
        let mut total_num_transactions: usize = 0;
        let mut total_num_retryable: usize = 0;
        loop {
            let (num_transactions, num_retryable) = self.try_receive_completed(container)?;
            if num_transactions == 0 {
                break;
            }
            saturating_add_assign!(total_num_transactions, num_transactions);
            saturating_add_assign!(total_num_retryable, num_retryable);
        }
        Ok((total_num_transactions, total_num_retryable))
    }
}

impl GreedyScheduler {
    /// Receive completed batches of transactions.
    /// Returns `Ok((num_transactions, num_retryable))` if a batch was received, `Ok((0, 0))` if no batch was received.
    fn try_receive_completed(
        &mut self,
        container: &mut TransactionStateContainer,
    ) -> Result<(usize, usize), SchedulerError> {
        match self.finished_consume_work_receiver.try_recv() {
            Ok(FinishedConsumeWork {
                work:
                    ConsumeWork {
                        batch_id,
                        ids,
                        transactions,
                        max_ages,
                    },
                retryable_indexes,
            }) => {
                let num_transactions = ids.len();
                let num_retryable = retryable_indexes.len();

                // Free the locks
                self.complete_batch(batch_id, &transactions);

                // Retryable transactions should be inserted back into the container
                let mut retryable_iter = retryable_indexes.into_iter().peekable();
                for (index, (id, transaction, max_age)) in
                    izip!(ids, transactions, max_ages).enumerate()
                {
                    if let Some(retryable_index) = retryable_iter.peek() {
                        if *retryable_index == index {
                            container.retry_transaction(
                                id,
                                SanitizedTransactionTTL {
                                    transaction,
                                    max_age,
                                },
                            );
                            retryable_iter.next();
                            continue;
                        }
                    }
                    container.remove_by_id(&id);
                }

                Ok((num_transactions, num_retryable))
            }
            Err(TryRecvError::Empty) => Ok((0, 0)),
            Err(TryRecvError::Disconnected) => Err(SchedulerError::DisconnectedRecvChannel(
                "finished consume work",
            )),
        }
    }

    /// Mark a given `TransactionBatchId` as completed.
    /// This will update the internal tracking, including account locks.
    fn complete_batch(
        &mut self,
        batch_id: TransactionBatchId,
        transactions: &[SanitizedTransaction],
    ) {
        let thread_id = self.in_flight_tracker.complete_batch(batch_id);
        for transaction in transactions {
            let account_keys = transaction.account_keys();
            let write_account_locks = account_keys
                .iter()
                .enumerate()
                .filter_map(|(index, key)| transaction.is_writable(index).then_some(key));
            let read_account_locks = account_keys
                .iter()
                .enumerate()
                .filter_map(|(index, key)| (!transaction.is_writable(index)).then_some(key));
            self.account_locks
                .unlock_accounts(write_account_locks, read_account_locks, thread_id);
        }
    }

    /// Send all batches of transactions to the worker threads.
    /// Returns the number of transactions sent.
    fn send_batches(&mut self, batches: &mut Batches) -> Result<usize, SchedulerError> {
        (0..self.consume_work_senders.len())
            .map(|thread_index| self.send_batch(batches, thread_index))
            .sum()
    }

    /// Send a batch of transactions to the given thread's `ConsumeWork` channel.
    /// Returns the number of transactions sent.
    fn send_batch(
        &mut self,
        batches: &mut Batches,
        thread_index: usize,
    ) -> Result<usize, SchedulerError> {
        if batches.ids[thread_index].is_empty() {
            return Ok(0);
        }

        let (ids, transactions, max_ages, total_cus) = batches.take_batch(thread_index);

        let batch_id = self
            .in_flight_tracker
            .track_batch(ids.len(), total_cus, thread_index);

        let num_scheduled = ids.len();
        let work = ConsumeWork {
            batch_id,
            ids,
            transactions,
            max_ages,
        };
        self.consume_work_senders[thread_index]
            .send(work)
            .map_err(|_| SchedulerError::DisconnectedSendChannel("consume work sender"))?;

        Ok(num_scheduled)
    }
}

fn try_schedule_transaction(
    transaction_state: &mut TransactionState,
    pre_lock_filter: impl Fn(&SanitizedTransaction) -> bool,
    account_locks: &mut ThreadAwareAccountLocks,
    schedulable_threads: ThreadSet,
    thread_selector: impl Fn(ThreadSet) -> ThreadId,
) -> Result<TransactionSchedulingInfo, TransactionSchedulingError> {
    let transaction = &transaction_state.transaction_ttl().transaction;
    if !pre_lock_filter(transaction) {
        return Err(TransactionSchedulingError::Filtered);
    }

    // Schedule the transaction if it can be.
    let account_keys = transaction.account_keys();
    let write_account_locks = account_keys
        .iter()
        .enumerate()
        .filter_map(|(index, key)| transaction.is_writable(index).then_some(key));
    let read_account_locks = account_keys
        .iter()
        .enumerate()
        .filter_map(|(index, key)| (!transaction.is_writable(index)).then_some(key));

    let Some(thread_id) = account_locks.try_lock_accounts(
        write_account_locks,
        read_account_locks,
        schedulable_threads,
        thread_selector,
    ) else {
        return Err(TransactionSchedulingError::UnschedulableConflicts);
    };

    let sanitized_transaction_ttl = transaction_state.transition_to_pending();
    let cost = transaction_state.cost();

    Ok(TransactionSchedulingInfo {
        thread_id,
        transaction: sanitized_transaction_ttl.transaction,
        max_age: sanitized_transaction_ttl.max_age,
        cost,
    })
}
