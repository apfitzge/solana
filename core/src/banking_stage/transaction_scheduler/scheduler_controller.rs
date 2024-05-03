//! Control flow for BankingStage's transaction scheduler.
//!

use {
    super::{
        scheduler_error::SchedulerError,
        scheduler_metrics::{
            SchedulerCountMetrics, SchedulerLeaderDetectionMetrics, SchedulerTimingMetrics,
        },
        transaction_state::SanitizedTransactionTTL,
        transaction_state_container::TransactionStateContainer,
    },
    crate::{
        banking_stage::{
            consume_worker::ConsumeWorkerMetrics,
            consumer::Consumer,
            decision_maker::{BufferedPacketsDecision, DecisionMaker},
            forward_packet_batches_by_accounts::ForwardPacketBatchesByAccounts,
            forwarder::Forwarder,
            immutable_deserialized_packet::ImmutableDeserializedPacket,
            packet_deserializer::PacketDeserializer,
            ForwardOption, TOTAL_BUFFERED_PACKETS,
        },
        banking_trace::BankingPacketBatch,
    },
    crossbeam_channel::RecvTimeoutError,
    solana_cost_model::cost_model::CostModel,
    solana_measure::measure_us,
    solana_program_runtime::compute_budget_processor::process_compute_budget_instructions,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_sdk::{
        self,
        clock::{FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET, MAX_PROCESSING_AGE},
        fee::FeeBudgetLimits,
        saturating_add_assign,
        transaction::SanitizedTransaction,
    },
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    std::{
        num,
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    },
};

/// Controls packet and transaction flow into scheduler, and scheduling execution.
pub(crate) struct SchedulerController {
    /// Decision maker for determining what should be done with transactions.
    decision_maker: DecisionMaker,
    /// Packet/Transaction ingress.
    packet_receiver: PacketDeserializer,
    bank_forks: Arc<RwLock<BankForks>>,
    /// Container for transaction state.
    /// Shared resource between `packet_receiver` and `scheduler`.
    container: TransactionStateContainer,

    /// Metrics tracking time for leader bank detection.
    leader_detection_metrics: SchedulerLeaderDetectionMetrics,
    /// Metrics tracking counts on transactions in different states
    /// over an interval and during a leader slot.
    count_metrics: SchedulerCountMetrics,
    /// Metrics tracking time spent in difference code sections
    /// over an interval and during a leader slot.
    timing_metrics: SchedulerTimingMetrics,
    /// Metric report handles for the worker threads.
    worker_metrics: Vec<Arc<ConsumeWorkerMetrics>>,
    /// State for forwarding packets to the leader.
    forwarder: Forwarder,
}

impl SchedulerController {
    pub fn new(
        decision_maker: DecisionMaker,
        packet_deserializer: PacketDeserializer,
        bank_forks: Arc<RwLock<BankForks>>,
        worker_metrics: Vec<Arc<ConsumeWorkerMetrics>>,
        forwarder: Forwarder,
    ) -> Self {
        Self {
            decision_maker,
            packet_receiver: packet_deserializer,
            bank_forks,
            container: TransactionStateContainer::with_capacity(TOTAL_BUFFERED_PACKETS),
            leader_detection_metrics: SchedulerLeaderDetectionMetrics::default(),
            count_metrics: SchedulerCountMetrics::default(),
            timing_metrics: SchedulerTimingMetrics::default(),
            worker_metrics,
            forwarder,
        }
    }

    pub fn run(mut self) -> Result<(), SchedulerError> {
        loop {
            // BufferedPacketsDecision is shared with legacy BankingStage, which will forward
            // packets. Initially, not renaming these decision variants but the actions taken
            // are different, since new BankingStage will not forward packets.
            // For `Forward` and `ForwardAndHold`, we want to receive packets but will not
            // forward them to the next leader. In this case, `ForwardAndHold` is
            // indistiguishable from `Hold`.
            //
            // `Forward` will drop packets from the buffer instead of forwarding.
            // During receiving, since packets would be dropped from buffer anyway, we can
            // bypass sanitization and buffering and immediately drop the packets.
            let (decision, decision_time_us) =
                measure_us!(self.decision_maker.make_consume_or_forward_decision());
            self.timing_metrics.update(|timing_metrics| {
                saturating_add_assign!(timing_metrics.decision_time_us, decision_time_us);
            });
            let new_leader_slot = decision.bank_start().map(|b| b.working_bank.slot());
            self.leader_detection_metrics
                .update_and_maybe_report(decision.bank_start());
            self.count_metrics
                .maybe_report_and_reset_slot(new_leader_slot);
            self.timing_metrics
                .maybe_report_and_reset_slot(new_leader_slot);

            // self.process_transactions(&decision)?;
            if !self.receive_and_buffer_packets(&decision) {
                break;
            }
            // Report metrics only if there is data.
            // Reset intervals when appropriate, regardless of report.
            let should_report = self.count_metrics.interval_has_data();
            let priority_min_max = self.container.get_min_max_priority();
            self.count_metrics.update(|count_metrics| {
                count_metrics.update_priority_stats(priority_min_max);
            });
            self.count_metrics
                .maybe_report_and_reset_interval(should_report);
            self.timing_metrics
                .maybe_report_and_reset_interval(should_report);
            self.worker_metrics
                .iter()
                .for_each(|metrics| metrics.maybe_report_and_reset());
        }

        Ok(())
    }

    /// Returns whether the packet receiver is still connected.
    fn receive_and_buffer_packets(&mut self, decision: &BufferedPacketsDecision) -> bool {
        let remaining_queue_capacity = self.container.remaining_queue_capacity();

        const MAX_PACKET_RECEIVE_TIME: Duration = Duration::from_millis(100);
        let recv_timeout = match decision {
            BufferedPacketsDecision::Consume(_) => {
                if self.container.is_empty() {
                    MAX_PACKET_RECEIVE_TIME
                } else {
                    Duration::ZERO
                }
            }
            BufferedPacketsDecision::Forward
            | BufferedPacketsDecision::ForwardAndHold
            | BufferedPacketsDecision::Hold => MAX_PACKET_RECEIVE_TIME,
        };

        let (receive_result, receive_time_us) = measure_us!(self
            .packet_receiver
            .receive_until(recv_timeout, remaining_queue_capacity));

        self.timing_metrics.update(|timing_metrics| {
            saturating_add_assign!(timing_metrics.receive_time_us, receive_time_us);
        });

        match receive_result {
            Ok((count, messages)) => {
                self.count_metrics.update(|count_metrics| {
                    saturating_add_assign!(count_metrics.num_received, count);
                });

                let (_, buffer_time_us) = measure_us!(self.buffer_packets(messages));
                self.timing_metrics.update(|timing_metrics| {
                    saturating_add_assign!(timing_metrics.buffer_time_us, buffer_time_us);
                });
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return false,
        }

        true

        // let (received_packet_results, receive_time_us) = measure_us!(self
        //     .packet_receiver
        //     .receive_packets(recv_timeout, remaining_queue_capacity, |_| true));

        // self.timing_metrics.update(|timing_metrics| {
        //     saturating_add_assign!(timing_metrics.receive_time_us, receive_time_us);
        // });

        // match received_packet_results {
        //     Ok(receive_packet_results) => {
        //         let num_received_packets = receive_packet_results.deserialized_packets.len();

        //         self.count_metrics.update(|count_metrics| {
        //             saturating_add_assign!(count_metrics.num_received, num_received_packets);
        //         });

        //         let (_, buffer_time_us) =
        //             measure_us!(self.buffer_packets(receive_packet_results.deserialized_packets));
        //         self.timing_metrics.update(|timing_metrics| {
        //             saturating_add_assign!(timing_metrics.buffer_time_us, buffer_time_us);
        //         });
        //     }
        //     Err(RecvTimeoutError::Timeout) => {}
        //     Err(RecvTimeoutError::Disconnected) => return false,
        // }

        // true
    }

    fn buffer_packets(&mut self, messages: Vec<BankingPacketBatch>) {
        let mut num_buffered = 0;
        let mut num_dropped = 0;
        for message in messages {
            for packet_batch in &message.0 {
                for packet in packet_batch.into_iter() {
                    if self.container.insert_new_packet(packet) {
                        num_buffered += 1;
                    } else {
                        num_dropped += 1;
                    }
                }
            }
        }

        self.count_metrics.update(|count_metrics| {
            saturating_add_assign!(count_metrics.num_buffered, num_buffered);
            saturating_add_assign!(count_metrics.num_dropped_on_capacity, num_dropped);
        });

        // // Convert to Arcs
        // let packets: Vec<_> = packets.into_iter().map(Arc::new).collect();
        // // Sanitize packets, generate IDs, and insert into the container.
        // let bank = self.bank_forks.read().unwrap().working_bank();
        // let last_slot_in_epoch = bank.epoch_schedule().get_last_slot_in_epoch(bank.epoch());
        // let transaction_account_lock_limit = bank.get_transaction_account_lock_limit();
        // let feature_set = &bank.feature_set;
        // let vote_only = bank.vote_only_bank();

        // const CHUNK_SIZE: usize = 128;
        // let lock_results: [_; CHUNK_SIZE] = core::array::from_fn(|_| Ok(()));
        // let mut error_counts = TransactionErrorMetrics::default();
        // for chunk in packets.chunks(CHUNK_SIZE) {
        //     let mut post_sanitization_count: usize = 0;

        //     let mut arc_packets = Vec::with_capacity(chunk.len());
        //     let mut transactions = Vec::with_capacity(chunk.len());
        //     let mut fee_budget_limits_vec = Vec::with_capacity(chunk.len());

        //     chunk
        //         .iter()
        //         .filter_map(|packet| {
        //             packet
        //                 .build_sanitized_transaction(
        //                     feature_set,
        //                     vote_only,
        //                     bank.as_ref(),
        //                     bank.get_reserved_account_keys(),
        //                 )
        //                 .map(|tx| (packet.clone(), tx))
        //         })
        //         .inspect(|_| saturating_add_assign!(post_sanitization_count, 1))
        //         .filter(|(_packet, tx)| {
        //             SanitizedTransaction::validate_account_locks(
        //                 tx.message(),
        //                 transaction_account_lock_limit,
        //             )
        //             .is_ok()
        //         })
        //         .filter_map(|(packet, tx)| {
        //             process_compute_budget_instructions(tx.message().program_instructions_iter())
        //                 .map(|compute_budget| (packet, tx, compute_budget.into()))
        //                 .ok()
        //         })
        //         .for_each(|(packet, tx, fee_budget_limits)| {
        //             arc_packets.push(packet);
        //             transactions.push(tx);
        //             fee_budget_limits_vec.push(fee_budget_limits);
        //         });

        //     let check_results = bank.check_transactions(
        //         &transactions,
        //         &lock_results[..transactions.len()],
        //         MAX_PROCESSING_AGE,
        //         &mut error_counts,
        //     );
        //     let post_lock_validation_count = transactions.len();

        //     let mut post_transaction_check_count: usize = 0;
        //     let mut num_dropped_on_capacity: usize = 0;
        //     let mut num_buffered: usize = 0;
        //     for (((packet, transaction), fee_budget_limits), _) in arc_packets
        //         .into_iter()
        //         .zip(transactions)
        //         .zip(fee_budget_limits_vec)
        //         .zip(check_results)
        //         .filter(|(_, check_result)| check_result.0.is_ok())
        //     {
        //         saturating_add_assign!(post_transaction_check_count, 1);

        //         let (priority, cost) =
        //             Self::calculate_priority_and_cost(&transaction, &fee_budget_limits, &bank);
        //         let transaction_ttl = SanitizedTransactionTTL {
        //             transaction,
        //             max_age_slot: last_slot_in_epoch,
        //         };

        //         if self
        //             .container
        //             .insert_new_transaction(transaction_ttl, packet, priority, cost)
        //         {
        //             saturating_add_assign!(num_dropped_on_capacity, 1);
        //         }
        //         saturating_add_assign!(num_buffered, 1);
        //     }

        //     // Update metrics for transactions that were dropped.
        //     let num_dropped_on_sanitization = chunk.len().saturating_sub(post_sanitization_count);
        //     let num_dropped_on_lock_validation =
        //         post_sanitization_count.saturating_sub(post_lock_validation_count);
        //     let num_dropped_on_transaction_checks =
        //         post_lock_validation_count.saturating_sub(post_transaction_check_count);

        //     self.count_metrics.update(|count_metrics| {
        //         saturating_add_assign!(
        //             count_metrics.num_dropped_on_capacity,
        //             num_dropped_on_capacity
        //         );
        //         saturating_add_assign!(count_metrics.num_buffered, num_buffered);
        //         saturating_add_assign!(
        //             count_metrics.num_dropped_on_sanitization,
        //             num_dropped_on_sanitization
        //         );
        //         saturating_add_assign!(
        //             count_metrics.num_dropped_on_validate_locks,
        //             num_dropped_on_lock_validation
        //         );
        //         saturating_add_assign!(
        //             count_metrics.num_dropped_on_receive_transaction_checks,
        //             num_dropped_on_transaction_checks
        //         );
        //     });
        // }
    }

    /// Calculate priority and cost for a transaction:
    ///
    /// Cost is calculated through the `CostModel`,
    /// and priority is calculated through a formula here that attempts to sell
    /// blockspace to the highest bidder.
    ///
    /// The priority is calculated as:
    /// P = R / (1 + C)
    /// where P is the priority, R is the reward,
    /// and C is the cost towards block-limits.
    ///
    /// Current minimum costs are on the order of several hundred,
    /// so the denominator is effectively C, and the +1 is simply
    /// to avoid any division by zero due to a bug - these costs
    /// are calculated by the cost-model and are not direct
    /// from user input. They should never be zero.
    /// Any difference in the prioritization is negligible for
    /// the current transaction costs.
    fn calculate_priority_and_cost(
        transaction: &SanitizedTransaction,
        fee_budget_limits: &FeeBudgetLimits,
        bank: &Bank,
    ) -> (u64, u64) {
        let cost = CostModel::calculate_cost(transaction, &bank.feature_set).sum();
        let reward = bank.calculate_reward_for_transaction(transaction, fee_budget_limits);

        // We need a multiplier here to avoid rounding down too aggressively.
        // For many transactions, the cost will be greater than the fees in terms of raw lamports.
        // For the purposes of calculating prioritization, we multiply the fees by a large number so that
        // the cost is a small fraction.
        // An offset of 1 is used in the denominator to explicitly avoid division by zero.
        const MULTIPLIER: u64 = 1_000_000;
        (
            reward
                .saturating_mul(MULTIPLIER)
                .saturating_div(cost.saturating_add(1)),
            cost,
        )
    }
}
