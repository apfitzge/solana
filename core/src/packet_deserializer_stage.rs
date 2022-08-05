//! Separate service to handle packet deserialization into transactions.

use {
    crate::{
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        sigverify::SigverifyTracerPacketStats, tracer_packet_stats::TracerPacketStats,
    },
    crossbeam_channel::{Receiver as CrossbeamReceiver, Sender as CrossbeamSender},
    solana_perf::packet::PacketBatch,
    std::{
        sync::{atomic::AtomicBool, Arc},
        thread::Builder,
        time::Duration,
    },
};

pub type BankingPacketBatch = (Vec<PacketBatch>, Option<SigverifyTracerPacketStats>);
pub type BankingPacketReceiver = CrossbeamReceiver<BankingPacketBatch>;
pub type BankingTransactionSender = CrossbeamSender<Box<ImmutableDeserializedPacket>>;

pub struct PacketDeserializerStage {
    deserializer_thread_hdl: std::thread::JoinHandle<()>,
}

impl PacketDeserializerStage {
    pub fn new(
        transaction_packet_receiver: BankingPacketReceiver,
        transaction_deserialized_packet_sender: BankingTransactionSender,
        tpu_vote_packet_receiver: BankingPacketReceiver,
        tpu_vote_deserialized_packet_sender: BankingTransactionSender,
        vote_packet_receiver: BankingPacketReceiver,
        vote_deserialized_packet_sender: BankingTransactionSender,
        exit_signal: Arc<AtomicBool>,
    ) -> Self {
        let transaction_deserializer = PacketDeserializer::new(
            transaction_packet_receiver,
            transaction_deserialized_packet_sender,
            0,
        );
        let tpu_vote_deserializer = PacketDeserializer::new(
            tpu_vote_packet_receiver,
            tpu_vote_deserialized_packet_sender,
            1,
        );
        let vote_deserializer =
            PacketDeserializer::new(vote_packet_receiver, vote_deserialized_packet_sender, 2);

        let mut packet_deserializer_service = PacketDeserializerService {
            transaction_deserializer,
            tpu_vote_deserializer,
            vote_deserializer,
        };

        let deserializer_thread_hdl = Builder::new()
            .name("solana-packet-deserializer".to_string())
            .spawn(move || loop {
                if exit_signal.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                packet_deserializer_service
                    .transaction_deserializer
                    .receive_packets();
                packet_deserializer_service
                    .tpu_vote_deserializer
                    .receive_packets();
                packet_deserializer_service
                    .vote_deserializer
                    .receive_packets();
            })
            .unwrap();

        Self {
            deserializer_thread_hdl,
        }
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.deserializer_thread_hdl.join()
    }
}

struct PacketDeserializerService {
    /// Packet deserializer for regular transactions
    transaction_deserializer: PacketDeserializer,
    /// Packet deserializer for TPU vote transactions
    tpu_vote_deserializer: PacketDeserializer,
    /// Packet deserializer for non-TPU vote transactions
    vote_deserializer: PacketDeserializer,
}

struct PacketDeserializer {
    /// Receiver for packets from sigverify stage
    packet_receiver: BankingPacketReceiver,
    /// Sender for deserialized packets to banking stage
    deserialized_packet_sender: BankingTransactionSender,

    /// Tracer packet stats from sigverify
    tracer_packet_stats: TracerPacketStats,
}

impl PacketDeserializer {
    fn new(
        packet_receiver: BankingPacketReceiver,
        deserialized_packet_sender: BankingTransactionSender,
        tracer_packet_stats_id: u32,
    ) -> Self {
        Self {
            packet_receiver,
            deserialized_packet_sender,
            tracer_packet_stats: TracerPacketStats::new(tracer_packet_stats_id),
        }
    }

    fn receive_packets(&mut self) {
        const RECEIVE_TIMEOUT: Duration = Duration::from_millis(10);

        if let Ok((packet_batches, new_sigverify_tracer_packet_stats_option)) =
            self.packet_receiver.recv_timeout(RECEIVE_TIMEOUT)
        {
            if let Some(new_sigverify_tracer_packet_stats) =
                &new_sigverify_tracer_packet_stats_option
            {
                self.tracer_packet_stats
                    .aggregate_sigverify_tracer_packet_stats(new_sigverify_tracer_packet_stats);
            }

            for packet_batch in packet_batches {
                let packet_indexes = Self::generate_packet_indexes(&packet_batch);

                // // Track all the packets incoming from sigverify, both valid and invalid
                // slot_metrics_tracker.increment_total_new_valid_packets(packet_indexes.len() as u64);
                // slot_metrics_tracker.increment_newly_failed_sigverify_count(
                //     packet_batch.len().saturating_sub(packet_indexes.len()) as u64,
                // );

                if !packet_indexes.is_empty() {
                    // TODO: track metrics

                    let deserialized_packets =
                        Self::deserialize_packets(&packet_batch, &packet_indexes);

                    deserialized_packets.for_each(|deserialized_packet| {
                        self.send_deserialized_packet(deserialized_packet)
                    });
                }
            }
        }
    }

    fn deserialize_packets<'a>(
        packet_batch: &'a PacketBatch,
        packet_indexes: &'a [usize],
    ) -> impl Iterator<Item = Box<ImmutableDeserializedPacket>> + 'a {
        packet_indexes
            .iter()
            .filter_map(move |packet_index| {
                ImmutableDeserializedPacket::new(packet_batch[*packet_index].clone(), None).ok()
            })
            .map(Box::new)
    }

    fn send_deserialized_packet(&self, packet: Box<ImmutableDeserializedPacket>) {
        let _ = self.deserialized_packet_sender.send(packet);
    }

    fn generate_packet_indexes(packet_batch: &PacketBatch) -> Vec<usize> {
        packet_batch
            .iter()
            .enumerate()
            .filter(|(_, pkt)| !pkt.meta.discard())
            .map(|(index, _)| index)
            .collect()
    }
}
