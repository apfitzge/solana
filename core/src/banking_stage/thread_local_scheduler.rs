use {
    super::{
        consume_executor::ConsumeExecutor,
        decision_maker::{BufferedPacketsDecision, DecisionMaker},
        forward_executor::ForwardExecutor,
        packet_receiver::PacketReceiver,
        scheduler_error::SchedulerError,
        BankingStageStats, SLOT_BOUNDARY_CHECK_PERIOD,
    },
    crate::{
        leader_slot_banking_stage_metrics::LeaderSlotMetricsTracker,
        tracer_packet_stats::TracerPacketStats,
        unprocessed_transaction_storage::UnprocessedTransactionStorage,
    },
    crossbeam_channel::RecvTimeoutError,
    solana_measure::{measure, measure_us},
    std::time::Instant,
};

pub(crate) struct ThreadLocalScheduler {
    decision_maker: DecisionMaker,
    unprocessed_transaction_storage: UnprocessedTransactionStorage,
    packet_receiver: PacketReceiver,
    last_metrics_update: Instant,
}

impl ThreadLocalScheduler {
    pub fn new(
        decision_maker: DecisionMaker,
        unprocessed_transaction_storage: UnprocessedTransactionStorage,
        packet_receiver: PacketReceiver,
    ) -> Self {
        Self {
            decision_maker,
            unprocessed_transaction_storage,
            packet_receiver,
            last_metrics_update: Instant::now(),
        }
    }

    pub fn tick(
        &mut self,
        banking_stage_stats: &mut BankingStageStats,
        tracer_packet_stats: &mut TracerPacketStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) -> Result<(), SchedulerError> {
        if matches!(
            self.packet_receiver.do_packet_receiving_and_buffering(
                &mut self.unprocessed_transaction_storage,
                banking_stage_stats,
                tracer_packet_stats,
                slot_metrics_tracker,
            ),
            Err(RecvTimeoutError::Disconnected)
        ) {
            Err(SchedulerError::PacketReceiverDisconnected)
        } else {
            Ok(())
        }
    }

    pub fn do_scheduled_work(
        &mut self,
        consume_executor: &ConsumeExecutor,
        forward_executor: &ForwardExecutor,
        banking_stage_stats: &mut BankingStageStats,
        tracer_packet_stats: &mut TracerPacketStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) {
        if !self.unprocessed_transaction_storage.is_empty()
            || self.last_metrics_update.elapsed() >= SLOT_BOUNDARY_CHECK_PERIOD
        {
            let (_, process_buffered_packets_us) = measure_us!(self.process_buffered_packets(
                consume_executor,
                forward_executor,
                banking_stage_stats,
                tracer_packet_stats,
                slot_metrics_tracker,
            ));
            slot_metrics_tracker.increment_process_buffered_packets_us(process_buffered_packets_us);
            self.last_metrics_update = Instant::now();
        }
    }

    fn process_buffered_packets(
        &mut self,
        consume_executor: &ConsumeExecutor,
        forward_executor: &ForwardExecutor,
        banking_stage_stats: &mut BankingStageStats,
        tracer_packet_stats: &mut TracerPacketStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) {
        if self.unprocessed_transaction_storage.should_not_process() {
            return;
        }

        let ((metrics_action, decision), make_decision_us) = measure_us!(self
            .decision_maker
            .make_consume_or_forward_decision(slot_metrics_tracker));
        slot_metrics_tracker.increment_make_decision_us(make_decision_us);

        match decision {
            BufferedPacketsDecision::Consume(bank_start) => {
                // Take metrics action before consume packets (potentially resetting the
                // slot metrics tracker to the next slot) so that we don't count the
                // packet processing metrics from the next slot towards the metrics
                // of the previous slot
                slot_metrics_tracker.apply_action(metrics_action);
                let (_, consume_buffered_packets_us) = measure_us!(consume_executor
                    .consume_buffered_packets(
                        &bank_start,
                        &mut self.unprocessed_transaction_storage,
                        None::<Box<dyn Fn()>>,
                        banking_stage_stats,
                        slot_metrics_tracker,
                    ));
                slot_metrics_tracker
                    .increment_consume_buffered_packets_us(consume_buffered_packets_us);
            }
            BufferedPacketsDecision::Forward => {
                let (_, forward_us) = measure_us!(forward_executor.handle_forwarding(
                    &mut self.unprocessed_transaction_storage,
                    false,
                    slot_metrics_tracker,
                    banking_stage_stats,
                    tracer_packet_stats,
                ));
                slot_metrics_tracker.increment_forward_us(forward_us);
                // Take metrics action after forwarding packets to include forwarded
                // metrics into current slot
                slot_metrics_tracker.apply_action(metrics_action);
            }
            BufferedPacketsDecision::ForwardAndHold => {
                let (_, forward_and_hold_us) = measure_us!(forward_executor.handle_forwarding(
                    &mut self.unprocessed_transaction_storage,
                    true,
                    slot_metrics_tracker,
                    banking_stage_stats,
                    tracer_packet_stats,
                ));
                slot_metrics_tracker.increment_forward_and_hold_us(forward_and_hold_us);
                // Take metrics action after forwarding packets
                slot_metrics_tracker.apply_action(metrics_action);
            }
            _ => (),
        }
    }
}
