use {
    super::transaction_state::SanitizedTransactionTTL,
    crate::{
        banking_stage::{
            immutable_deserialized_packet::ImmutableDeserializedPacket,
            packet_deserializer::{PacketDeserializer, PacketReceiverStats},
        },
        banking_trace::{BankingPacketBatch, BankingPacketReceiver},
    },
    crossbeam_channel::Sender,
    solana_runtime::bank_forks::BankForks,
    solana_sdk::transaction::SanitizedTransaction,
    std::sync::{Arc, RwLock},
};

/// A pass-through stage that receives packets after signature verification,
/// filters and deserializes them, and sends them to `BankingStage`.
pub struct PacketReceiveAndBuffer {
    /// Receiver for packet batches from sigverify stage
    packet_batch_receiver: BankingPacketReceiver,
    /// Provides working bank for deserializer to check feature activation
    bank_forks: Arc<RwLock<BankForks>>,
    /// Sender to BankingStage
    sender: Sender<Vec<(Arc<ImmutableDeserializedPacket>, SanitizedTransactionTTL)>>,
}

impl PacketReceiveAndBuffer {
    /// Create a new PacketReceiveAndBuffer
    pub fn new(
        packet_batch_receiver: BankingPacketReceiver,
        bank_forks: Arc<RwLock<BankForks>>,
        sender: Sender<Vec<(Arc<ImmutableDeserializedPacket>, SanitizedTransactionTTL)>>,
    ) -> Self {
        Self {
            packet_batch_receiver,
            bank_forks,
            sender,
        }
    }

    pub fn run(self) {
        // receive packets until the sender is dropped
        while let Ok(message) = self.packet_batch_receiver.recv() {
            self.process_packets(message);
        }
    }

    fn process_packets(&self, message: BankingPacketBatch) {
        let packet_batches = &message.0;
        let _stats = &message.1; // TODO: collect stats

        let bank = self.bank_forks.read().unwrap().working_bank();
        let last_slot_in_epoch = bank.epoch_schedule().get_last_slot_in_epoch(bank.epoch());
        let transaction_account_lock_limit = bank.get_transaction_account_lock_limit();
        let vote_only = bank.vote_only_bank();

        let round_compute_unit_price_enabled = false; // TODO get from working_bank.feature_set
        let mut packet_stats = PacketReceiverStats::default();

        for packet_batch in packet_batches {
            let packet_indexes = PacketDeserializer::generate_packet_indexes(packet_batch);
            let deserialized_packets_and_transactions: Vec<_> =
                PacketDeserializer::deserialize_packets(
                    packet_batch,
                    &packet_indexes,
                    round_compute_unit_price_enabled,
                    &mut packet_stats,
                    &|packet| {
                        packet.check_excessive_precompiles()?;
                        Ok(packet)
                    },
                )
                .map(Arc::new)
                .filter_map(|packet| {
                    packet
                        .build_sanitized_transaction(
                            vote_only,
                            bank.as_ref(),
                            bank.get_reserved_account_keys(),
                        )
                        .map(|tx| (packet, tx))
                })
                .filter(|(_packet, tx)| {
                    SanitizedTransaction::validate_account_locks(
                        tx.message(),
                        transaction_account_lock_limit,
                    )
                    .is_ok()
                })
                .map(|(packet, tx)| {
                    (
                        packet,
                        SanitizedTransactionTTL {
                            transaction: tx,
                            max_age_slot: last_slot_in_epoch,
                        },
                    )
                })
                .collect();

            // TODO: exit on error
            let _ = self.sender.send(deserialized_packets_and_transactions);
        }
    }
}
