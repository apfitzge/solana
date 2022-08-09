//! Separate service to handle packet deserialization into transactions.

use {
    crate::{
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        sigverify::SigverifyTracerPacketStats, tracer_packet_stats::TracerPacketStats,
    },
    crossbeam_channel::{Receiver as CrossbeamReceiver, Sender as CrossbeamSender},
    solana_measure::measure,
    solana_perf::packet::PacketBatch,
    std::{
        sync::{atomic::AtomicBool, Arc},
        thread::Builder,
        time::{Duration, Instant},
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
        let mut transaction_deserializer = PacketDeserializer::new(
            transaction_packet_receiver,
            transaction_deserialized_packet_sender,
            0,
        );
        let mut tpu_vote_deserializer = PacketDeserializer::new(
            tpu_vote_packet_receiver,
            tpu_vote_deserialized_packet_sender,
            1,
        );
        let mut vote_deserializer =
            PacketDeserializer::new(vote_packet_receiver, vote_deserialized_packet_sender, 2);

        let deserializer_thread_hdl = Builder::new()
            .name("solana-packet-deserializer".to_string())
            .spawn(move || loop {
                if exit_signal.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }

                transaction_deserializer.do_work();
                tpu_vote_deserializer.do_work();
                vote_deserializer.do_work();
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

struct PacketDeserializer {
    /// Receiver for packets from sigverify stage
    packet_receiver: BankingPacketReceiver,
    /// Sender for deserialized packets to banking stage
    deserialized_packet_sender: BankingTransactionSender,
    /// Tracer packet stats from sigverify
    tracer_packet_stats: TracerPacketStats,
    /// Metrics for deserializer
    deserializer_metrics: PacketDeserializerMetrics,
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
            deserializer_metrics: PacketDeserializerMetrics::new(),
        }
    }

    fn do_work(&mut self) {
        self.receive_packets();
        self.tracer_packet_stats.report(1000);
        self.deserializer_metrics.report();
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
                let (packet_indexes, generate_index_time) =
                    measure!(Self::generate_packet_indexes(&packet_batch));

                self.deserializer_metrics.generate_index_time_us += generate_index_time.as_us();
                self.deserializer_metrics.num_valid_packets += packet_indexes.len();
                self.deserializer_metrics.num_failed_sigverify +=
                    packet_batch.len().saturating_sub(packet_indexes.len());

                if !packet_indexes.is_empty() {
                    let (deserialized_packets, deserialize_packets_time) =
                        measure!(Self::deserialize_packets(&packet_batch, &packet_indexes));

                    let (_, send_packets_time) = measure!({
                        deserialized_packets.for_each(|deserialized_packet| {
                            self.send_deserialized_packet(deserialized_packet)
                        })
                    });

                    self.deserializer_metrics.deserialize_time_us +=
                        deserialize_packets_time.as_us();
                    self.deserializer_metrics.sending_time_us += send_packets_time.as_us();
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

struct PacketDeserializerMetrics {
    /// Last time reported
    last_report: Instant,
    /// Number of valid packets
    num_valid_packets: usize,
    /// Number of failed sigverify packets
    num_failed_sigverify: usize,
    /// Time spent generating packet indexes
    generate_index_time_us: u64,
    /// Time spent deserializing packets
    deserialize_time_us: u64,
    /// Time spent sending packets to banking stage
    sending_time_us: u64,
}

impl PacketDeserializerMetrics {
    fn new() -> Self {
        Self {
            last_report: Instant::now(),
            num_valid_packets: 0,
            num_failed_sigverify: 0,
            generate_index_time_us: 0,
            deserialize_time_us: 0,
            sending_time_us: 0,
        }
    }

    fn report(&mut self) {
        const REPORTING_INTERVAL: Duration = Duration::from_secs(1);
        if self.last_report.elapsed() > REPORTING_INTERVAL {
            datapoint_info!(
                "packet_deserializer",
                ("num_valid_packets", self.num_valid_packets, i64),
                ("num_failed_sigverify", self.num_failed_sigverify, i64),
                ("generate_index_time_us", self.generate_index_time_us, i64),
                ("deserialize_time_us", self.deserialize_time_us, i64),
                ("sending_time_us", self.sending_time_us, i64),
            );

            *self = Self::new();
        }
    }
}
