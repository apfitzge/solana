//! Separate service to handle packet deserialization into transactions.

use {
    crate::{
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        sigverify::SigverifyTracerPacketStats, tracer_packet_stats::TracerPacketStats,
    },
    crossbeam_channel::{
        Receiver as CrossbeamReceiver, RecvTimeoutError, Sender as CrossbeamSender,
    },
    solana_perf::packet::PacketBatch,
    std::{thread::Builder, time::Duration},
};

pub type BankingPacketBatch = (Vec<PacketBatch>, Option<SigverifyTracerPacketStats>);
pub type BankingPacketReceiver = CrossbeamReceiver<BankingPacketBatch>;
pub type BankingTransactionSender = CrossbeamSender<Box<ImmutableDeserializedPacket>>;

pub struct PacketDeserializerStage {
    deserializer_thread_hdl: std::thread::JoinHandle<()>,
}

impl PacketDeserializerStage {
    pub fn new() -> Self {
        let deserializer_thread_hdl = Builder::new()
            .name("solana-packet-deserializer".to_string())
            .spawn(move || todo!())
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

            let packet_count: usize = packet_batches.iter().map(|x| x.len()).sum();
            let mut dropped_packets_count = 0;
            let mut newly_buffered_packets_count = 0;
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
                ImmutableDeserializedPacket::from_packet(packet_batch[*packet_index].clone()).ok()
            })
            .map(Box::new)
    }

    fn send_deserialized_packet(&self, packet: Box<ImmutableDeserializedPacket>) {
        let _ = self.send_deserialized_packet(packet);
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
