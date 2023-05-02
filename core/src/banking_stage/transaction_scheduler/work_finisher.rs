use {
    super::{
        in_flight_tracker::InFlightTracker, thread_aware_account_locks::ThreadAwareAccountLocks,
        transaction_packet_container::TransactionPacketContainer,
    },
    crate::{
        banking_stage::scheduler_messages::{
            ConsumeWork, FinishedConsumeWork, FinishedForwardWork, ForwardWork,
        },
        immutable_deserialized_packet::ImmutableDeserializedPacket,
    },
    crossbeam_channel::{select, Receiver},
    itertools::izip,
    solana_runtime::{bank::Bank, bank_forks::BankForks, blockhash_queue::BlockhashQueue},
    solana_sdk::{
        clock::{Slot, MAX_PROCESSING_AGE},
        transaction::SanitizedTransaction,
    },
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        time::Duration,
    },
};

pub(crate) struct WorkFinisher {
    exit: Arc<AtomicBool>,
    consumed_work_receiver: Receiver<FinishedConsumeWork>,
    forwarded_work_receiver: Receiver<FinishedForwardWork>,
    bank_forks: Arc<RwLock<BankForks>>,
    account_locks: Arc<ThreadAwareAccountLocks>,
    container: Arc<TransactionPacketContainer>,
    in_flight_tracker: Arc<InFlightTracker>,
}

impl WorkFinisher {
    pub(crate) fn new(
        exit: Arc<AtomicBool>,
        consumed_work_receiver: Receiver<FinishedConsumeWork>,
        forwarded_work_receiver: Receiver<FinishedForwardWork>,
        bank_forks: Arc<RwLock<BankForks>>,
        account_locks: Arc<ThreadAwareAccountLocks>,
        container: Arc<TransactionPacketContainer>,
        in_flight_tracker: Arc<InFlightTracker>,
    ) -> Self {
        Self {
            exit,
            consumed_work_receiver,
            forwarded_work_receiver,
            bank_forks,
            account_locks,
            container,
            in_flight_tracker,
        }
    }

    pub(crate) fn run(self) {
        const EXIT_CHECK_INTERVAL: Duration = Duration::from_millis(100);
        loop {
            select! {
                recv(self.consumed_work_receiver) -> consumed_work => {
                    if let Ok(consumed_work) = consumed_work {
                        self.finish_consume_work(consumed_work);
                    }
                    else {
                        break;
                    }
                }
                recv(self.forwarded_work_receiver) -> forwarded_work => {
                    if let Ok(forwarded_work) = forwarded_work {
                        self.finish_forward_work(forwarded_work);
                    }
                    else {break;}
                }
                default(EXIT_CHECK_INTERVAL) => {
                    if self.exit.load(Ordering::Relaxed) {
                        break;
                    }
                }
            }
        }
    }

    fn finish_consume_work(&self, consumed_work: FinishedConsumeWork) {
        let bank = self.bank_forks.read().unwrap().working_bank();
        let last_slot_in_epoch = bank.epoch_schedule().get_last_slot_in_epoch(bank.epoch());
        let r_blockhash_queue = bank.read_blockhash_queue().unwrap();

        for consumed_work in
            std::iter::once(consumed_work).chain(self.consumed_work_receiver.try_iter())
        {
            self.process_finished_consume_work(
                &bank,
                &r_blockhash_queue,
                last_slot_in_epoch,
                consumed_work,
            );
        }
    }

    fn process_finished_consume_work(
        &self,
        bank: &Bank,
        r_blockhash_queue: &BlockhashQueue,
        last_slot_in_epoch: Slot,
        FinishedConsumeWork {
            work:
                ConsumeWork {
                    batch_id,
                    ids,
                    transactions,
                    max_age_slots,
                },
            retryable_indexes,
        }: FinishedConsumeWork,
    ) {
        let thread_id = self.in_flight_tracker.complete_batch(batch_id, ids.len());
        let mut retryable_id_iter = retryable_indexes.into_iter().peekable();
        for (index, (id, transaction, max_age_slot)) in
            izip!(ids, transactions, max_age_slots).enumerate()
        {
            let locks = transaction.get_account_locks_unchecked();
            let accounts_to_write = self.account_locks.unlock_accounts(
                locks.writable.into_iter(),
                locks.readonly.into_iter(),
                thread_id,
            );

            for (slot, accounts) in accounts_to_write {
                let account_refs = accounts
                    .iter()
                    .map(|(pubkey, account)| (pubkey, account))
                    .collect::<Vec<_>>();

                bank.accounts().store_accounts_cached((
                    slot,
                    &account_refs[..],
                    // TODO: This could screw us over epoch boundaries. Best to just wait until the feature is activated.
                    bank.include_slot_in_hash(),
                ));
            }

            match retryable_id_iter.peek() {
                Some(retryable_index) if index == *retryable_index => {
                    self.container
                        .retry_transaction(id, transaction, max_age_slot);
                    retryable_id_iter.next(); // advance the iterator
                }
                _ => {
                    let packet_entry = self
                        .container
                        .get_packet_entry(id)
                        .expect("packet must exist");

                    if max_age_slot == 0 {
                        if let Some(resanitized_transaction) =
                            Self::should_retry_expired_transaction(
                                packet_entry.get().immutable_section(),
                                bank,
                                r_blockhash_queue,
                            )
                        {
                            // re-insert
                            drop(packet_entry);
                            self.container.retry_transaction(
                                id,
                                resanitized_transaction,
                                last_slot_in_epoch,
                            );
                            continue;
                        }
                    }

                    packet_entry.remove_entry();
                }
            }
        }
    }

    fn should_retry_expired_transaction(
        packet: &ImmutableDeserializedPacket,
        bank: &Bank,
        r_blockhash_queue: &BlockhashQueue,
    ) -> Option<SanitizedTransaction> {
        // Check age
        r_blockhash_queue
            .get_hash_age(
                packet
                    .transaction()
                    .get_message()
                    .message
                    .recent_blockhash(),
            )
            .filter(|age| *age <= MAX_PROCESSING_AGE as u64)?;

        packet.build_sanitized_transaction(&bank.feature_set, bank.vote_only_bank(), bank)
    }

    fn finish_forward_work(&self, forwarded_work: FinishedForwardWork) {
        for finished_forward_work in
            std::iter::once(forwarded_work).chain(self.forwarded_work_receiver.try_iter())
        {
            self.process_finished_forward_work(finished_forward_work);
        }
    }

    fn process_finished_forward_work(
        &self,
        FinishedForwardWork {
            work: ForwardWork { ids, packets: _ },
            successful,
        }: FinishedForwardWork,
    ) {
        if successful {
            for id in ids {
                if let Some(mut deserialized_packet) = self.container.get_packet_entry(id) {
                    deserialized_packet.get_mut().forwarded = true;
                } else {
                    // If a packet is not in the map, then it was forwarded *without* holding
                    // and this can return early without iterating over the remaining ids.
                    return;
                }
            }
        }
    }
}
