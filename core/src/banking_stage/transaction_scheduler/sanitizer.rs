use {
    super::{
        transaction_id_generator::TransactionIdGenerator,
        transaction_packet_container::{SanitizedTransactionTTL, TransactionPacketContainer},
    },
    crate::{
        banking_stage::scheduler_messages::TransactionId,
        banking_trace::{BankingPacketBatch, BankingPacketReceiver},
        immutable_deserialized_packet::ImmutableDeserializedPacket,
    },
    crossbeam_channel::select,
    solana_perf::packet::PacketBatch,
    solana_runtime::{bank::Bank, bank_forks::BankForks, blockhash_queue::BlockhashQueue},
    solana_sdk::{
        clock::{Slot, MAX_PROCESSING_AGE},
        transaction::SanitizedTransaction,
    },
    std::sync::{atomic::AtomicBool, Arc, RwLock},
};

/// Separate thread for sanitzing and inserting transactions into the scheduler
pub struct Sanitizer {
    exit: Arc<AtomicBool>,
    receiver: BankingPacketReceiver,
    transaction_id_generator: Arc<TransactionIdGenerator>,
    container: Arc<TransactionPacketContainer>,
    bank_forks: Arc<RwLock<BankForks>>,
}

impl Sanitizer {
    pub(crate) fn new(
        exit: Arc<AtomicBool>,
        receiver: BankingPacketReceiver,
        transaction_id_generator: Arc<TransactionIdGenerator>,
        container: Arc<TransactionPacketContainer>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        Self {
            exit,
            receiver,
            transaction_id_generator,
            container,
            bank_forks,
        }
    }

    pub(crate) fn run(self) {
        const SLEEP_DURATION: std::time::Duration = std::time::Duration::from_millis(100);

        loop {
            select! {
                recv(self.receiver) -> result => {
                    if let Ok(banking_packets) = result {
                        self.receive_loop(banking_packets);
                    }
                    else {
                        break;
                    }
                }
                default(SLEEP_DURATION) => {
                    if self.exit.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                }
            }
        }
    }

    fn receive_loop(&self, banking_packets: BankingPacketBatch) {
        let bank = self.bank_forks.read().unwrap().working_bank();
        let r_blockhash_queue = bank.read_blockhash_queue().unwrap();
        let last_slot_in_epoch = bank.epoch_schedule().get_last_slot_in_epoch(bank.epoch());
        let tx_account_lock_limit = bank.get_transaction_account_lock_limit();

        for banking_packets in std::iter::once(banking_packets).chain(self.receiver.try_iter()) {
            self.process_packet_batches(
                &banking_packets.0,
                &bank,
                &r_blockhash_queue,
                last_slot_in_epoch,
                tx_account_lock_limit,
            );

            // TODO: Accumulate tracer stats
        }
    }

    fn process_packet_batches(
        &self,
        packet_batches: &[PacketBatch],
        bank: &Bank,
        r_blockhash: &BlockhashQueue,
        last_slot_in_epoch: Slot,
        tx_account_lock_limit: usize,
    ) {
        for packet_batch in packet_batches {
            let packet_indexes = Self::generate_packet_indexes(packet_batch);
            let deserialized_packets = self.deserialize_packets(packet_batch, &packet_indexes);

            for (id, packet, transaction) in deserialized_packets
                .filter_map(|(id, packet)| {
                    packet
                        .build_sanitized_transaction(&bank.feature_set, bank.vote_only_bank(), bank)
                        .map(|tx| (id, packet, tx))
                })
                .filter(|(_, _, transaction)| {
                    SanitizedTransaction::validate_account_locks(
                        transaction.message(),
                        tx_account_lock_limit,
                    )
                    .is_ok()
                })
                .filter(|(_, _, transaction)| {
                    r_blockhash
                        .get_hash_age(transaction.message().recent_blockhash())
                        .map(|age| age <= MAX_PROCESSING_AGE as u64)
                        .unwrap_or(true)
                })
            {
                let transaction_ttl = SanitizedTransactionTTL {
                    transaction,
                    max_age_slot: last_slot_in_epoch,
                };

                self.container
                    .insert_new_transaction(id, packet, transaction_ttl);
            }
        }
    }

    fn generate_packet_indexes(packet_batch: &PacketBatch) -> Vec<usize> {
        packet_batch
            .iter()
            .enumerate()
            .filter(|(_, pkt)| !pkt.meta().discard())
            .map(|(index, _)| index)
            .collect()
    }

    fn deserialize_packets<'a>(
        &self,
        packet_batch: &'a PacketBatch,
        packet_indexes: &'a [usize],
    ) -> impl Iterator<Item = (TransactionId, ImmutableDeserializedPacket)> + 'a {
        let transaction_ids = self
            .transaction_id_generator
            .next_batch(packet_indexes.len() as u64);
        packet_indexes
            .iter()
            .filter_map(move |packet_index| {
                ImmutableDeserializedPacket::new(packet_batch[*packet_index].clone(), None).ok()
            })
            .zip(transaction_ids)
            .map(|t| (t.1, t.0))
    }
}
