use {
    super::multi_iterator_scheduler::{TransactionIdGenerator, TransactionPacketContainer},
    crate::{
        banking_trace::BankingPacketReceiver,
        immutable_deserialized_packet::ImmutableDeserializedPacket,
    },
    itertools::{izip, Itertools},
    solana_perf::packet::PacketBatch,
    solana_runtime::root_bank_cache::RootBankCache,
    std::sync::Arc,
};

pub struct DeserializerService {
    receiver: BankingPacketReceiver,
    container: Arc<TransactionPacketContainer>,
    transaction_id_generator: Arc<TransactionIdGenerator>,
    root_bank_cache: RootBankCache,
}

impl DeserializerService {
    pub fn new(
        receiver: BankingPacketReceiver,
        container: Arc<TransactionPacketContainer>,
        transaction_id_generator: Arc<TransactionIdGenerator>,
        root_bank_cache: RootBankCache,
    ) -> Self {
        Self {
            receiver,
            container,
            transaction_id_generator,
            root_bank_cache,
        }
    }

    pub fn run(mut self) {
        while let Ok(banking_packets) = self.receiver.recv() {
            let root_bank = self.root_bank_cache.root_bank();
            for packet_batch in banking_packets.0.iter() {
                let packet_indexes = Self::generate_packet_indexes(packet_batch);
                let deserialized_packets =
                    Self::deserialize_packets(packet_batch, &packet_indexes).collect_vec();

                let ids = self.transaction_id_generator.generate_n_ids(
                    deserialized_packets.len() as u64,
                    deserialized_packets.iter().map(|p| p.priority()),
                );

                let sanitized_transactions = deserialized_packets
                    .iter()
                    .filter_map(|p| {
                        p.build_sanitized_transaction(
                            &root_bank.feature_set,
                            root_bank.vote_only_bank(),
                            root_bank.as_ref(),
                        )
                    })
                    .collect_vec();

                for (id, packet, sanitized_transaction) in
                    izip!(ids, deserialized_packets, sanitized_transactions)
                {
                    self.container.push_priority_queue_with_map_inserts(
                        id,
                        packet,
                        sanitized_transaction,
                    );
                }
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
        packet_batch: &'a PacketBatch,
        packet_indexes: &'a [usize],
    ) -> impl Iterator<Item = ImmutableDeserializedPacket> + 'a {
        packet_indexes.iter().filter_map(move |packet_index| {
            ImmutableDeserializedPacket::new(packet_batch[*packet_index].clone(), None).ok()
        })
    }
}
