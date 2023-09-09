use std::collections::HashSet;

use {
    super::{
        batch_id_generator::BatchIdGenerator,
        central_scheduler_banking_stage::SchedulerError,
        in_flight_tracker::InFlightTracker,
        thread_aware_account_locks::{ThreadAwareAccountLocks, ThreadId},
        transaction_packet_container::TransactionPacketContainer,
    },
    crate::banking_stage::{
        read_write_account_set::ReadWriteAccountSet,
        scheduler_messages::{ConsumeWork, FinishedConsumeWork},
        transaction_scheduler::{
            thread_aware_account_locks::ThreadSet,
            transaction_packet_container::SanitizedTransactionTTL,
            transaction_priority_id::TransactionPriorityId,
        },
    },
    crossbeam_channel::{Receiver, Sender},
    itertools::{izip, Itertools},
    prio_graph::{GraphNode, PrioGraph, SelectKind, Selection},
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

    pub(crate) fn schedule(
        &mut self,
        container: &mut TransactionPacketContainer,
    ) -> Result<usize, SchedulerError> {
        let num_threads = self.consume_work_senders.len();
        let available_threads = ThreadSet::any(num_threads);
        const MAX_TRANSACTIONS_PER_SCHEDULING_PASS: usize = 10_000;
        let ids = container
            .take_top_n(MAX_TRANSACTIONS_PER_SCHEDULING_PASS)
            .collect_vec();

        // Priority modification for transactions at top-of-book.
        fn top_level_prioritization(
            priority_id: TransactionPriorityId,
            next_level_rewards: u64,
        ) -> u64 {
            priority_id.priority + (next_level_rewards / 4)
        }

        let mut graph = PrioGraph::new(
            container.transaction_lookup_map(),
            ids.iter().rev().copied(),
            top_level_prioritization,
        );

        // Instead of sending transactions in line, let's create batches at top-of-book.
        // This let's use gain additional efficiency by batching transactions:
        //  - fewer messages
        //  - fewer locks taken during execution
        const MAX_TRANSACTIONS_PER_BATCH: usize = 64;
        const MAX_BATCHES_IN_FLIGHT: usize = 100;
        let mut num_scheduled = 0;
        let mut batch_account_locks = ReadWriteAccountSet::default();
        let mut waiting_on_forked_nodes_count = 0;
        let mut join_set = HashSet::new();
        while !graph.is_empty() {
            // pre-allocate batches for each thread
            let mut available_threads = available_threads;

            // do not allow threads which have at least 4 batches in flight
            for (thread_id, sender) in self.consume_work_senders.iter().enumerate() {
                if sender.len() > MAX_BATCHES_IN_FLIGHT {
                    available_threads.remove(thread_id);
                }
            }

            if !available_threads.is_empty() {
                batch_account_locks.clear();
                let mut batches: Vec<_> = (0..num_threads)
                    .map(|_| {
                        let batch_id = self.batch_id_generator.next();
                        ConsumeWork {
                            batch_id,
                            ids: Vec::with_capacity(MAX_TRANSACTIONS_PER_BATCH),
                            transactions: Vec::with_capacity(MAX_TRANSACTIONS_PER_BATCH),
                            max_age_slots: Vec::with_capacity(MAX_TRANSACTIONS_PER_BATCH),
                        }
                    })
                    .collect();

                graph.iterate(
                    &mut |id: TransactionPriorityId, node: &GraphNode<TransactionPriorityId>| {
                        let transaction = container.get_transaction(&id);

                        // Check that there are no conflicts within current batch
                        if !batch_account_locks.check_sanitized_message_account_locks(
                            transaction.transaction.message(),
                        ) {
                            return Selection {
                                selected: SelectKind::Unselected,
                                continue_iterating: true,
                            };
                        }

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
                            // Add to batch locks
                            batch_account_locks.add_sanitized_message_account_locks(
                                transaction.transaction.message(),
                            );
                            let SanitizedTransactionTTL {
                                id,
                                transaction,
                                max_age_slot,
                            } = container.take_transaction(&id);

                            batches[thread_id].ids.push(id);
                            batches[thread_id].transactions.push(transaction);
                            batches[thread_id].max_age_slots.push(max_age_slot);
                            if batches[thread_id].ids.len() == MAX_TRANSACTIONS_PER_BATCH {
                                available_threads.remove(thread_id);
                            }

                            // If we previously detected a join and it is now schedule,
                            // we can remove it from the join set.
                            join_set.remove(&id);

                            Selection {
                                selected: if node.edges.len() > 1 {
                                    // TODO: we probably actually don't want to block entirely,
                                    // but allow the heaviest fork to continue, while
                                    // blocking other forks.
                                    waiting_on_forked_nodes_count += 1;

                                    SelectKind::SelectedBlock
                                } else {
                                    SelectKind::SelectedNoBlock
                                },
                                continue_iterating: !available_threads.is_empty(),
                            }
                        } else {
                            // This is a join on the graph or at least our threads, so we should try to
                            // receive finished work that would unblock this, during the scheduling loop.
                            join_set.insert(id);
                            Selection {
                                selected: SelectKind::Unselected,
                                continue_iterating: true,
                            }
                        }
                    },
                );
                for (thread_id, (batch, sender)) in batches
                    .into_iter()
                    .zip(self.consume_work_senders.iter())
                    .enumerate()
                    .filter(|(_, (batch, _))| !batch.ids.is_empty())
                {
                    self.in_flight_tracker
                        .track_batch(batch.batch_id, batch.ids.len(), thread_id);
                    num_scheduled += batch.ids.len();
                    sender.send(batch).unwrap();
                }

                if graph.is_empty() {
                    break;
                }
            }

            // Only receive during scheduling if we are blocking on something.
            if !join_set.is_empty() || waiting_on_forked_nodes_count > 0 {
                // Receive signals for unblocking transactions during iteration.
                self.receive_and_process_finished_work(container, |id| {
                    if graph.remove_transaction(&id) {
                        waiting_on_forked_nodes_count -= 1;
                    }
                });
            }
        }
        Ok(num_scheduled)
    }

    pub(crate) fn receive_and_process_finished_work(
        &mut self,
        container: &mut TransactionPacketContainer,
        mut on_finished_id: impl FnMut(TransactionPriorityId),
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
                on_finished_id(id);
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
