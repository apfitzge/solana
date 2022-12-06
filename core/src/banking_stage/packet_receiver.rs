use {
    crate::{
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        leader_slot_banking_stage_metrics::LeaderSlotMetricsTracker,
        packet_deserializer::{PacketDeserializer, ReceivePacketResults},
        sigverify::SigverifyTracerPacketStats,
        unprocessed_transaction_storage::UnprocessedTransactionStorage,
    },
    crossbeam_channel::RecvTimeoutError,
    histogram::Histogram,
    solana_measure::{measure, measure::Measure},
    solana_perf::packet::PACKETS_PER_BATCH,
    solana_sdk::{
        saturating_add_assign,
        timing::{duration_as_ms, timestamp, AtomicInterval},
    },
    std::{
        sync::atomic::{AtomicU64, AtomicUsize, Ordering},
        time::{Duration, Instant},
    },
};

#[derive(Default, Debug)]
pub struct PacketReceiverStats {
    last_report: AtomicInterval,
    id: u32,
    receive_and_buffer_packets_count: AtomicUsize,
    dropped_packets_count: AtomicUsize,
    dropped_duplicated_packets_count: AtomicUsize,
    newly_buffered_packets_count: AtomicUsize,
    current_buffered_packets_count: AtomicUsize,
    batch_packet_indexes_len: Histogram,
    receive_and_buffer_packets_elapsed: AtomicU64,
}

impl PacketReceiverStats {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            batch_packet_indexes_len: Histogram::configure()
                .max_value(PACKETS_PER_BATCH as u64)
                .build()
                .unwrap(),
            ..PacketReceiverStats::default()
        }
    }

    fn is_empty(&self) -> bool {
        0 == self
            .receive_and_buffer_packets_count
            .load(Ordering::Relaxed)
            + self.dropped_packets_count.load(Ordering::Relaxed)
            + self
                .dropped_duplicated_packets_count
                .load(Ordering::Relaxed)
            + self.newly_buffered_packets_count.load(Ordering::Relaxed)
            + self.current_buffered_packets_count.load(Ordering::Relaxed)
            + self.batch_packet_indexes_len.entries() as usize
    }

    fn report(&mut self, report_interval_ms: u64) {
        if self.is_empty() {
            return;
        }

        if self.last_report.should_update(report_interval_ms) {
            datapoint_info!(
                "banking_stage-packet_receiver_stats",
                ("id", self.id, i64),
                (
                    "receive_and_buffer_packet_counts",
                    self.receive_and_buffer_packets_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "dropped_packets_count",
                    self.dropped_packets_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "dropped_duplicated_packets_count",
                    self.dropped_duplicated_packets_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "newly_buffered_packets_count",
                    self.newly_buffered_packets_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "current_buffered_packets_count",
                    self.current_buffered_packets_count.load(Ordering::Relaxed),
                    i64
                ),
                (
                    "packet_batch_indices_len_min",
                    self.batch_packet_indexes_len.minimum().unwrap_or(0),
                    i64
                ),
                (
                    "packet_batch_indices_len_max",
                    self.batch_packet_indexes_len.maximum().unwrap_or(0),
                    i64
                ),
                (
                    "packet_batch_indices_len_mean",
                    self.batch_packet_indexes_len.mean().unwrap_or(0),
                    i64
                ),
                (
                    "packet_batch_indices_len_90pct",
                    self.batch_packet_indexes_len.percentile(90.0).unwrap_or(0),
                    i64
                ),
                (
                    "receive_and_buffer_packets_elapsed",
                    self.receive_and_buffer_packets_elapsed
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
            );
            self.batch_packet_indexes_len.clear();
        }
    }
}

#[derive(Default, Debug)]
pub struct TracerPacketReceiverStats {
    id: u32,
    last_report: AtomicInterval,
    sigverify_tracer_packet_stats: SigverifyTracerPacketStats,
    total_exceeded_banking_stage_buffer: AtomicUsize,
}

impl TracerPacketReceiverStats {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            ..TracerPacketReceiverStats::default()
        }
    }

    fn is_empty(&self) -> bool {
        // If sigverify didn't see any, then nothing to report
        0 == self
            .sigverify_tracer_packet_stats
            .total_tracer_packets_received_in_sigverify_stage
    }

    fn report(&mut self, report_interval_ms: u64) {
        if self.is_empty() {
            return;
        }

        if self.last_report.should_update(report_interval_ms) {
            datapoint_info!(
                "tracer_packet_receiver_stats",
                ("id", self.id, i64),
                (
                    "total_removed_before_sigverify",
                    self.sigverify_tracer_packet_stats
                        .total_removed_before_sigverify_stage,
                    i64
                ),
                (
                    "total_tracer_packets_received_in_sigverify",
                    self.sigverify_tracer_packet_stats
                        .total_tracer_packets_received_in_sigverify_stage,
                    i64
                ),
                (
                    "total_tracer_packets_deduped_in_sigverify",
                    self.sigverify_tracer_packet_stats
                        .total_tracer_packets_deduped,
                    i64
                ),
                (
                    "total_excess_tracer_packets_discarded_in_sigverify",
                    self.sigverify_tracer_packet_stats
                        .total_excess_tracer_packets,
                    i64
                ),
                (
                    "total_tracker_packets_passed_sigverify",
                    self.sigverify_tracer_packet_stats
                        .total_tracker_packets_passed_sigverify,
                    i64
                ),
                (
                    "total_exceeded_banking_stage_buffer",
                    self.total_exceeded_banking_stage_buffer
                        .swap(0, Ordering::Relaxed),
                    i64
                )
            );

            *self = Self::new(self.id);
        }
    }

    fn aggregate_sigverify_tracer_packet_stats(&mut self, new_stats: &SigverifyTracerPacketStats) {
        self.sigverify_tracer_packet_stats.aggregate(new_stats);
    }

    fn increment_total_exceeded_banking_stage_buffer(&mut self, count: usize) {
        self.total_exceeded_banking_stage_buffer
            .fetch_add(count, Ordering::Relaxed);
    }
}

pub struct PacketReceiver {
    id: u32,
    packet_deserializer: PacketDeserializer,
    last_receive_time: Instant,
    stats: PacketReceiverStats,
    tracer_stats: TracerPacketReceiverStats,
}

impl PacketReceiver {
    pub fn new(id: u32, packet_deserializer: PacketDeserializer) -> Self {
        Self {
            id,
            packet_deserializer,
            last_receive_time: Instant::now(),
            stats: PacketReceiverStats::new(id),
            tracer_stats: TracerPacketReceiverStats::new(id),
        }
    }

    pub fn do_packet_receiving_and_buffering(
        &mut self,
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) -> Result<(), RecvTimeoutError> {
        // Gossip thread will almost always not wait because the transaction storage will most likely not be empty
        let recv_timeout = if !unprocessed_transaction_storage.is_empty() {
            // If there are buffered packets, run the equivalent of try_recv to try reading more
            // packets. This prevents starving BankingStage::consume_buffered_packets due to
            // buffered_packet_batches containing transactions that exceed the cost model for
            // the current bank.
            Duration::from_millis(0)
        } else {
            // Default wait time
            Duration::from_millis(100)
        };

        let (res, receive_and_buffer_packets_time) = measure!(self.receive_and_buffer_packets(
            recv_timeout,
            unprocessed_transaction_storage,
            slot_metrics_tracker,
        ));
        slot_metrics_tracker
            .increment_receive_and_buffer_packets_us(receive_and_buffer_packets_time.as_us());

        // Report receiving stats on interval
        self.stats.report(1000);
        self.tracer_stats.report(1000);

        res
    }

    /// Receive incoming packets, push into unprocessed buffer with packet indexes
    fn receive_and_buffer_packets(
        &mut self,
        recv_timeout: Duration,
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) -> Result<(), RecvTimeoutError> {
        let mut recv_time = Measure::start("receive_and_buffer_packets_recv");
        let ReceivePacketResults {
            deserialized_packets,
            new_tracer_stats_option,
            passed_sigverify_count,
            failed_sigverify_count,
        } = self.packet_deserializer.handle_received_packets(
            recv_timeout,
            unprocessed_transaction_storage.max_receive_size(),
        )?;
        let packet_count = deserialized_packets.len();
        debug!(
            "@{:?} process start stalled for: {:?}ms txs: {} id: {}",
            timestamp(),
            duration_as_ms(&self.last_receive_time.elapsed()),
            packet_count,
            self.id,
        );

        if let Some(new_sigverify_stats) = &new_tracer_stats_option {
            self.tracer_stats
                .aggregate_sigverify_tracer_packet_stats(new_sigverify_stats);
        }

        // Track all the packets incoming from sigverify, both valid and invalid
        slot_metrics_tracker.increment_total_new_valid_packets(passed_sigverify_count);
        slot_metrics_tracker.increment_newly_failed_sigverify_count(failed_sigverify_count);

        let mut dropped_packets_count = 0;
        let mut newly_buffered_packets_count = 0;
        self.push_unprocessed(
            unprocessed_transaction_storage,
            deserialized_packets,
            &mut dropped_packets_count,
            &mut newly_buffered_packets_count,
            slot_metrics_tracker,
        );
        recv_time.stop();

        self.stats
            .receive_and_buffer_packets_elapsed
            .fetch_add(recv_time.as_us(), Ordering::Relaxed);
        self.stats
            .receive_and_buffer_packets_count
            .fetch_add(packet_count, Ordering::Relaxed);
        self.stats
            .dropped_packets_count
            .fetch_add(dropped_packets_count, Ordering::Relaxed);
        self.stats
            .newly_buffered_packets_count
            .fetch_add(newly_buffered_packets_count, Ordering::Relaxed);
        self.stats
            .current_buffered_packets_count
            .swap(unprocessed_transaction_storage.len(), Ordering::Relaxed);
        self.last_receive_time = Instant::now();
        Ok(())
    }

    fn push_unprocessed(
        &mut self,
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
        deserialized_packets: Vec<ImmutableDeserializedPacket>,
        dropped_packets_count: &mut usize,
        newly_buffered_packets_count: &mut usize,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) {
        if !deserialized_packets.is_empty() {
            let _ = self
                .stats
                .batch_packet_indexes_len
                .increment(deserialized_packets.len() as u64);

            *newly_buffered_packets_count += deserialized_packets.len();
            slot_metrics_tracker
                .increment_newly_buffered_packets_count(deserialized_packets.len() as u64);

            let insert_packet_batches_summary =
                unprocessed_transaction_storage.insert_batch(deserialized_packets);
            slot_metrics_tracker
                .accumulate_insert_packet_batches_summary(&insert_packet_batches_summary);
            saturating_add_assign!(
                *dropped_packets_count,
                insert_packet_batches_summary.total_dropped_packets()
            );
            self.tracer_stats
                .increment_total_exceeded_banking_stage_buffer(
                    insert_packet_batches_summary.dropped_tracer_packets(),
                );
        }
    }
}
