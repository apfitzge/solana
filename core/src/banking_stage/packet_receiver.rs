use {
    crate::{
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        packet_deserializer::{PacketDeserializer, ReceivePacketResults},
        sigverify::SigverifyTracerPacketStats,
        unprocessed_transaction_storage::{
            InsertPacketBatchSummary, UnprocessedTransactionStorage,
        },
    },
    crossbeam_channel::RecvTimeoutError,
    histogram::Histogram,
    solana_measure::{measure, measure::Measure, measure_us},
    solana_perf::packet::PACKETS_PER_BATCH,
    solana_poh::poh_recorder::Slot,
    solana_sdk::{
        saturating_add_assign,
        timing::{duration_as_ms, timestamp, AtomicInterval},
    },
    std::{
        sync::atomic::{AtomicU64, AtomicUsize, Ordering},
        time::{Duration, Instant},
    },
};

pub struct PacketReceiver {
    id: u32,
    packet_deserializer: PacketDeserializer,
    last_receive_time: Instant,
    stats: PacketReceiverStats,
    tracer_stats: TracerPacketReceiverStats,
    leader_stats: LeaderSlotReceiveMetricsTracker,
}

impl PacketReceiver {
    pub fn new(id: u32, packet_deserializer: PacketDeserializer) -> Self {
        Self {
            id,
            packet_deserializer,
            last_receive_time: Instant::now(),
            stats: PacketReceiverStats::new(id),
            tracer_stats: TracerPacketReceiverStats::new(id),
            leader_stats: LeaderSlotReceiveMetricsTracker::new(id),
        }
    }

    pub fn check_leader_slot_boundary(&mut self, leader_slot: Option<Slot>) {
        self.leader_stats.check_leader_slot_boundary(leader_slot);
    }

    pub fn do_packet_receiving_and_buffering(
        &mut self,
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
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

        let (res, receive_and_buffer_packets_us) = measure_us!(
            self.receive_and_buffer_packets(recv_timeout, unprocessed_transaction_storage,)
        );
        self.leader_stats
            .increment_receive_and_buffer_packets_us(receive_and_buffer_packets_us);

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
        self.leader_stats
            .increment_total_new_valid_packets(passed_sigverify_count);
        self.leader_stats
            .increment_newly_failed_sigverify_count(failed_sigverify_count);

        let mut dropped_packets_count = 0;
        let mut newly_buffered_packets_count = 0;
        self.push_unprocessed(
            unprocessed_transaction_storage,
            deserialized_packets,
            &mut dropped_packets_count,
            &mut newly_buffered_packets_count,
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
    ) {
        if !deserialized_packets.is_empty() {
            let _ = self
                .stats
                .batch_packet_indexes_len
                .increment(deserialized_packets.len() as u64);

            *newly_buffered_packets_count += deserialized_packets.len();
            self.leader_stats
                .increment_newly_buffered_packets_count(deserialized_packets.len() as u64);

            let insert_packet_batches_summary =
                unprocessed_transaction_storage.insert_batch(deserialized_packets);
            self.leader_stats
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

struct LeaderSlotReceiveMetricsTracker {
    id: u32,
    leader_slot_metrics: Option<LeaderSlotReceiveMetrics>,
}

struct LeaderSlotReceiveMetrics {
    /// Aggregate metrics per slot
    slot: Slot,

    // Counts:
    /// Total number of live packets TPU received from verified receiver for processing.
    total_new_valid_packets: u64,
    /// Total number of packets TPU received from sigverify that failed signature verification.
    newly_failed_sigverify_count: u64,
    /// Total number of dropped packet due to the thread's buffered packets capacity being reached.
    exceeded_buffer_limit_dropped_packets_count: u64,
    /// Total number of packets that got added to the pending buffer after arriving to BankingStage
    newly_buffered_packets_count: u64,
    /// How many votes ingested from gossip were dropped
    dropped_gossip_votes: u64,
    /// How many votes ingested from tpu were dropped
    dropped_tpu_votes: u64,

    // Timings:
    /// Time spent processing new incoming packets to the banking thread
    receive_and_buffer_packets_us: u64,
    /// The number of times the function to receive and buffer new packets
    /// was called
    receive_and_buffer_packets_invoked_count: u64,
}

impl LeaderSlotReceiveMetricsTracker {
    fn new(id: u32) -> Self {
        Self {
            id,
            leader_slot_metrics: None,
        }
    }

    fn check_leader_slot_boundary(&mut self, leader_slot: Option<Slot>) {
        match (&mut self.leader_slot_metrics, leader_slot) {
            (Some(_), None) => {
                self.report();
                self.leader_slot_metrics = None;
            }
            (Some(metrics), Some(leader_slot)) => {
                if metrics.slot != leader_slot {
                    self.report();
                    self.leader_slot_metrics = Some(LeaderSlotReceiveMetrics::new(leader_slot));
                }
            }
            (None, Some(leader_slot)) => {
                self.leader_slot_metrics = Some(LeaderSlotReceiveMetrics::new(leader_slot));
            }
            _ => (),
        }
    }

    fn report(&mut self) {
        if let Some(metrics) = &mut self.leader_slot_metrics {
            datapoint_info!(
                "leader_slot_receive_metrics",
                ("id", self.id, i64),
                ("slot", metrics.slot, i64),
                (
                    "total_new_valid_packets",
                    metrics.total_new_valid_packets,
                    i64
                ),
                (
                    "newly_failed_sigverify_count",
                    metrics.newly_failed_sigverify_count,
                    i64
                ),
                (
                    "exceeded_buffer_limit_dropped_packets_count",
                    metrics.exceeded_buffer_limit_dropped_packets_count,
                    i64
                ),
                (
                    "newly_buffered_packets_count",
                    metrics.newly_buffered_packets_count,
                    i64
                ),
                ("dropped_gossip_votes", metrics.dropped_gossip_votes, i64),
                ("dropped_tpu_votes", metrics.dropped_tpu_votes, i64),
                (
                    "receive_and_buffer_packets_us",
                    metrics.receive_and_buffer_packets_us,
                    i64
                ),
                (
                    "receive_and_buffer_packets_invoked_count",
                    metrics.receive_and_buffer_packets_invoked_count,
                    i64
                ),
            );
        }
    }

    fn increment_receive_and_buffer_packets_us(&mut self, us: u64) {
        if let Some(metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(metrics.receive_and_buffer_packets_us, us);
            saturating_add_assign!(metrics.receive_and_buffer_packets_invoked_count, 1);
        }
    }

    fn increment_total_new_valid_packets(&mut self, count: u64) {
        if let Some(metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(metrics.total_new_valid_packets, count);
        }
    }

    fn increment_newly_failed_sigverify_count(&mut self, count: u64) {
        if let Some(metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(metrics.newly_failed_sigverify_count, count);
        }
    }

    fn increment_exceeded_buffer_limit_dropped_packets_count(&mut self, count: u64) {
        if let Some(metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(metrics.exceeded_buffer_limit_dropped_packets_count, count);
        }
    }

    fn increment_newly_buffered_packets_count(&mut self, count: u64) {
        if let Some(metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(metrics.newly_buffered_packets_count, count);
        }
    }

    fn accumulate_insert_packet_batches_summary(
        &mut self,
        insert_packet_batches_summary: &InsertPacketBatchSummary,
    ) {
        self.increment_exceeded_buffer_limit_dropped_packets_count(
            insert_packet_batches_summary.total_dropped_packets() as u64,
        );
        self.increment_dropped_gossip_vote_count(
            insert_packet_batches_summary.dropped_gossip_packets() as u64,
        );
        self.increment_dropped_tpu_vote_count(
            insert_packet_batches_summary.dropped_tpu_packets() as u64
        );
    }

    fn increment_dropped_gossip_vote_count(&mut self, count: u64) {
        if let Some(metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(metrics.dropped_gossip_votes, count);
        }
    }

    fn increment_dropped_tpu_vote_count(&mut self, count: u64) {
        if let Some(metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(metrics.dropped_tpu_votes, count);
        }
    }
}

impl LeaderSlotReceiveMetrics {
    pub fn new(slot: Slot) -> Self {
        Self {
            slot,
            total_new_valid_packets: 0,
            newly_failed_sigverify_count: 0,
            exceeded_buffer_limit_dropped_packets_count: 0,
            newly_buffered_packets_count: 0,
            dropped_gossip_votes: 0,
            dropped_tpu_votes: 0,
            receive_and_buffer_packets_us: 0,
            receive_and_buffer_packets_invoked_count: 0,
        }
    }
}
