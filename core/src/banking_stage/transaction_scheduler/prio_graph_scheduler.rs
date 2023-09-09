use super::central_scheduler_banking_stage::SchedulerError;

use {
    super::{
        batch_id_generator::BatchIdGenerator,
        in_flight_tracker::InFlightTracker,
        thread_aware_account_locks::{ThreadAwareAccountLocks, ThreadId},
        transaction_packet_container::TransactionPacketContainer,
    },
    crate::banking_stage::{
        scheduler_messages::{ConsumeWork, FinishedConsumeWork},
        transaction_scheduler::{
            thread_aware_account_locks::ThreadSet,
            transaction_packet_container::SanitizedTransactionTTL,
            transaction_priority_id::TransactionPriorityId,
        },
    },
    crossbeam_channel::{Receiver, Sender},
    itertools::{izip, Itertools},
    prio_graph::{GraphNode, PrioGraph, Selection},
    solana_sdk::pubkey::Pubkey,
};

pub(crate) struct PrioGraphScheduler {
    in_flight_tracker: InFlightTracker,
    account_locks: ThreadAwareAccountLocks<Pubkey>,
    batch_id_generator: BatchIdGenerator,
    consume_work_senders: Vec<Sender<ConsumeWork>>,
    finished_consume_work_receiver: Receiver<FinishedConsumeWork>,
}

impl PrioGraphScheduler {
    pub(crate) fn new(
        consume_work_senders: Vec<Sender<ConsumeWork>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork>,
    ) -> Self {
        Self {
            in_flight_tracker: InFlightTracker::new(consume_work_senders.len()),
            account_locks: ThreadAwareAccountLocks::new(consume_work_senders.len()),
            batch_id_generator: BatchIdGenerator::default(),
            consume_work_senders,
            finished_consume_work_receiver,
        }
    }

    pub(crate) fn schedule(&mut self, container: &mut TransactionPacketContainer) -> Result<usize, SchedulerError> {
        let num_threads = self.consume_work_senders.len();
        let available_threads = ThreadSet::any(num_threads);
        const MAX_TRANSACTIONS_PER_SCHEDULING_PASS: usize = 10_000;
        let ids = container
            .take_top_n(MAX_TRANSACTIONS_PER_SCHEDULING_PASS)
            .collect_vec();

        let mut graph = PrioGraph::new(
            container.transaction_lookup_map(),
            ids.iter().rev().copied(),
        );

        // Send batches of 1 transactions for all schedulable transactions at top-of-book.
        // If a transaction is not schedulable, it will be skipped until we receive signal
        // it was unblocked.
        let mut num_scheduled = 0;
        while !graph.is_empty() {
            graph.iterate(
                &mut |id: TransactionPriorityId, _node: &GraphNode<TransactionPriorityId>| {
                    let transaction = container.get_transaction(&id);
                    let account_locks = transaction.transaction.get_account_locks_unchecked();
                    if let Some(thread_id) = self.account_locks.try_lock_accounts(
                        account_locks.writable.into_iter(),
                        account_locks.readonly.into_iter(),
                        available_threads,
                        |thread_set| {
                            Self::select_thread(
                                self.in_flight_tracker.num_in_flight_per_thread(),
                                thread_set,
                            )
                        },
                    ) {
                        let batch_id = self.batch_id_generator.next();
                        let SanitizedTransactionTTL {
                            id,
                            transaction,
                            max_age_slot,
                        } = container.take_transaction(&id);

                        self.in_flight_tracker.track_batch(batch_id, 1, thread_id);
                        self.consume_work_senders[thread_id]
                            .send(ConsumeWork {
                                batch_id,
                                ids: vec![id],
                                transactions: vec![transaction],
                                max_age_slots: vec![max_age_slot],
                            }).unwrap();
                        num_scheduled += 1;

                        Selection {
                            selected: true,
                            continue_iterating: true,
                        }
                    } else {
                        Selection {
                            selected: false,
                            continue_iterating: true,
                        }
                    }
                },
            );

            // Receive signals for unblocking transactions during iteration.
            self.receive_and_process_finished_work(container);
        }
        Ok(num_scheduled)
    }

    pub(crate) fn receive_and_process_finished_work(
        &mut self,
        container: &mut TransactionPacketContainer,
    ) {
        // Receive signals for unblocking transactions
        while let Ok(FinishedConsumeWork {
            work:
                ConsumeWork {
                    batch_id,
                    ids,
                    transactions,
                    max_age_slots,
                },
            retryable_indexes,
        }) = self.finished_consume_work_receiver.try_recv()
        {
            let thread_id = self
                .in_flight_tracker
                .complete_batch(batch_id, transactions.len());

            let mut retryable_id_iter = retryable_indexes.into_iter().peekable();
            for (index, (id, transaction, max_age_slot)) in
                izip!(ids, transactions, max_age_slots).enumerate()
            {
                let account_locks = transaction.get_account_locks_unchecked();
                self.account_locks.unlock_accounts(
                    account_locks.writable.into_iter(),
                    account_locks.readonly.into_iter(),
                    thread_id,
                );

                match retryable_id_iter.peek() {
                    Some(retryable_index) if index == *retryable_index => {
                        container.retry_transaction(id, transaction, max_age_slot);
                        retryable_id_iter.next(); // advance the iterator
                    }
                    _ => {}
                }
            }
        }
    }

    fn select_thread(in_flight_per_thread: &[usize], thread_set: ThreadSet) -> ThreadId {
        thread_set
            .contained_threads_iter()
            .map(|thread_id| (thread_id, in_flight_per_thread[thread_id]))
            .min_by(|a, b| a.1.cmp(&b.1))
            .map(|(thread_id, _)| thread_id)
            .unwrap()
    }
}
