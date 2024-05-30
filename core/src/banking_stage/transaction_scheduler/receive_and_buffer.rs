use {
    super::{
        scheduler_metrics::{SchedulerCountMetrics, SchedulerTimingMetrics},
        transaction_priority_id::TransactionPriorityId,
        transaction_state::TransactionState,
        transaction_state_container::{
            SanitizedTransactionStateContainer, TransactionViewStateContainer,
        },
    },
    crate::{
        banking_stage::{
            decision_maker::BufferedPacketsDecision,
            immutable_deserialized_packet::ImmutableDeserializedPacket,
            packet_deserializer::PacketDeserializer,
            scheduler_messages::TransactionId,
            transaction_scheduler::{
                transaction_state::SanitizedTransactionTTL,
                transaction_state_container::TransactionStateContainerInterface,
            },
        },
        banking_trace::{BankingPacketBatch, BankingPacketReceiver},
        transaction_view::TransactionView,
    },
    arrayvec::ArrayVec,
    core::time::Duration,
    crossbeam_channel::{RecvTimeoutError, TryRecvError},
    itertools::Itertools,
    solana_cost_model::{cost_model::CostModel, instruction_details::InstructionDetails},
    solana_fee::FeeBudgetLimits,
    solana_measure::{measure_ns, measure_us},
    solana_perf::packet::PACKETS_PER_BATCH,
    solana_program_runtime::compute_budget_processor::process_compute_budget_instructions,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_sdk::{
        clock::MAX_PROCESSING_AGE,
        packet::{Packet, PacketFlags},
        saturating_add_assign,
        transaction::{SanitizedTransaction, TransactionError},
    },
    solana_signed_message::{Message, SignedMessage},
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    std::{
        sync::{Arc, RwLock},
        time::Instant,
    },
};

pub trait ReceiveAndBufferPackets<T: SignedMessage, C: TransactionStateContainerInterface<T>> {
    // Return false if channel disconnected.
    fn receive_and_buffer_packets(
        &self,
        decision: &BufferedPacketsDecision,
        timing_metrics: &mut SchedulerTimingMetrics,
        count_metrics: &mut SchedulerCountMetrics,
        container: &mut C,
    ) -> bool;
}

pub struct SimpleReceiveAndBuffer {
    /// Packet/Transaction ingress.
    packet_receiver: PacketDeserializer,
    bank_forks: Arc<RwLock<BankForks>>,
}

impl ReceiveAndBufferPackets<SanitizedTransaction, SanitizedTransactionStateContainer>
    for SimpleReceiveAndBuffer
{
    /// Returns whether the packet receiver is still connected.
    fn receive_and_buffer_packets(
        &self,
        decision: &BufferedPacketsDecision,
        timing_metrics: &mut SchedulerTimingMetrics,
        count_metrics: &mut SchedulerCountMetrics,
        container: &mut SanitizedTransactionStateContainer,
    ) -> bool {
        let remaining_queue_capacity = container.remaining_queue_capacity();
        const MAX_PACKET_RECEIVE_TIME: Duration = Duration::from_millis(100);
        let recv_timeout = match decision {
            BufferedPacketsDecision::Consume(_) => {
                if container.is_empty() {
                    MAX_PACKET_RECEIVE_TIME
                } else {
                    Duration::ZERO
                }
            }
            BufferedPacketsDecision::Forward
            | BufferedPacketsDecision::ForwardAndHold
            | BufferedPacketsDecision::Hold => MAX_PACKET_RECEIVE_TIME,
        };

        let (received_packet_results, receive_time_us) = measure_us!(self
            .packet_receiver
            .receive_packets(recv_timeout, remaining_queue_capacity, |_| true));

        timing_metrics.update(|timing_metrics| {
            saturating_add_assign!(timing_metrics.receive_time_us, receive_time_us);
        });

        match received_packet_results {
            Ok(receive_packet_results) => {
                let num_received_packets = receive_packet_results.deserialized_packets.len();

                count_metrics.update(|count_metrics| {
                    saturating_add_assign!(count_metrics.num_received, num_received_packets);
                });

                let (_, buffer_time_us) = measure_us!(self.buffer_packets(
                    receive_packet_results.deserialized_packets,
                    timing_metrics,
                    count_metrics,
                    container
                ));
                timing_metrics.update(|timing_metrics| {
                    saturating_add_assign!(timing_metrics.buffer_time_us, buffer_time_us);
                });
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return false,
        }

        true
    }
}

#[allow(dead_code)]
impl SimpleReceiveAndBuffer {
    pub fn new(packet_receiver: PacketDeserializer, bank_forks: Arc<RwLock<BankForks>>) -> Self {
        Self {
            packet_receiver,
            bank_forks,
        }
    }

    fn buffer_packets(
        &self,
        packets: Vec<ImmutableDeserializedPacket>,
        _timing_metrics: &mut SchedulerTimingMetrics,
        count_metrics: &mut SchedulerCountMetrics,
        container: &mut SanitizedTransactionStateContainer,
    ) {
        // Convert to Arcs
        let packets: Vec<_> = packets.into_iter().map(Arc::new).collect();
        // Sanitize packets, generate IDs, and insert into the container.
        let bank = self.bank_forks.read().unwrap().working_bank();
        let last_slot_in_epoch = bank.epoch_schedule().get_last_slot_in_epoch(bank.epoch());
        let transaction_account_lock_limit = bank.get_transaction_account_lock_limit();
        let feature_set = &bank.feature_set;
        let vote_only = bank.vote_only_bank();

        const CHUNK_SIZE: usize = 128;
        let lock_results: [_; CHUNK_SIZE] = core::array::from_fn(|_| Ok(()));
        let mut error_counts = TransactionErrorMetrics::default();
        for chunk in packets.chunks(CHUNK_SIZE) {
            let mut post_sanitization_count: usize = 0;

            let mut arc_packets = Vec::with_capacity(chunk.len());
            let mut transactions = Vec::with_capacity(chunk.len());
            let mut instruction_details_vec = Vec::with_capacity(chunk.len());

            chunk
                .iter()
                .filter_map(|packet| {
                    packet
                        .build_sanitized_transaction(
                            feature_set,
                            vote_only,
                            bank.as_ref(),
                            bank.get_reserved_account_keys(),
                        )
                        .map(|tx| (packet.clone(), tx))
                })
                .inspect(|_| saturating_add_assign!(post_sanitization_count, 1))
                .filter(|(_packet, tx)| {
                    tx.validate_account_locks(transaction_account_lock_limit)
                        .is_ok()
                })
                .filter_map(|(packet, tx)| {
                    InstructionDetails::new(&tx)
                        .map(|instruction_details| (packet, tx, instruction_details))
                        .ok()
                })
                .for_each(|(packet, tx, instruction_details)| {
                    arc_packets.push(packet);
                    transactions.push(tx);
                    instruction_details_vec.push(instruction_details);
                });

            let check_results = bank.check_transactions(
                &transactions,
                &lock_results[..transactions.len()],
                MAX_PROCESSING_AGE,
                &mut error_counts,
            );
            let post_lock_validation_count = transactions.len();

            let mut post_transaction_check_count: usize = 0;
            let mut num_dropped_on_capacity: usize = 0;
            let mut num_buffered: usize = 0;
            for (((packet, transaction), instruction_details), _) in arc_packets
                .into_iter()
                .zip(transactions)
                .zip(instruction_details_vec)
                .zip(check_results)
                .filter(|(_, check_result)| check_result.0.is_ok())
            {
                saturating_add_assign!(post_transaction_check_count, 1);

                let (priority, cost) =
                    calculate_priority_and_cost(&transaction, &instruction_details, &bank);
                let transaction_ttl = SanitizedTransactionTTL {
                    transaction,
                    max_age_slot: last_slot_in_epoch,
                };

                if container.insert_new_transaction(
                    packet.original_packet().meta().flags,
                    transaction_ttl,
                    priority,
                    cost,
                ) {
                    saturating_add_assign!(num_dropped_on_capacity, 1);
                }
                saturating_add_assign!(num_buffered, 1);
            }

            // Update metrics for transactions that were dropped.
            let num_dropped_on_sanitization = chunk.len().saturating_sub(post_sanitization_count);
            let num_dropped_on_lock_validation =
                post_sanitization_count.saturating_sub(post_lock_validation_count);
            let num_dropped_on_transaction_checks =
                post_lock_validation_count.saturating_sub(post_transaction_check_count);

            count_metrics.update(|count_metrics| {
                saturating_add_assign!(
                    count_metrics.num_dropped_on_capacity,
                    num_dropped_on_capacity
                );
                saturating_add_assign!(count_metrics.num_buffered, num_buffered);
                saturating_add_assign!(
                    count_metrics.num_dropped_on_sanitization,
                    num_dropped_on_sanitization
                );
                saturating_add_assign!(
                    count_metrics.num_dropped_on_validate_locks,
                    num_dropped_on_lock_validation
                );
                saturating_add_assign!(
                    count_metrics.num_dropped_on_receive_transaction_checks,
                    num_dropped_on_transaction_checks
                );
            });
        }
    }
}

pub struct TransactionViewReceiveAndBuffer {
    receiver: BankingPacketReceiver,
    bank_forks: Arc<RwLock<BankForks>>,
}

#[allow(dead_code)]
impl TransactionViewReceiveAndBuffer {
    pub fn new(receiver: BankingPacketReceiver, bank_forks: Arc<RwLock<BankForks>>) -> Self {
        Self {
            receiver,
            bank_forks,
        }
    }

    fn handle_message(
        &self,
        message: BankingPacketBatch,
        _decision: &BufferedPacketsDecision,
        _timing_metrics: &mut SchedulerTimingMetrics,
        count_metrics: &mut SchedulerCountMetrics,
        container: &mut TransactionViewStateContainer,
    ) {
        let bank = self.bank_forks.read().unwrap().working_bank();
        let last_slot_in_epoch = bank.epoch_schedule().get_last_slot_in_epoch(bank.epoch());
        let transaction_account_lock_limit = bank.get_transaction_account_lock_limit();
        let feature_set = &bank.feature_set;

        let mut total_packet_count = 0;
        let mut num_dropped_on_capacity: usize = 0;
        let mut num_buffered: usize = 0;
        // let mut lock_results: [_; PACKETS_PER_BATCH] = core::array::from_fn(|_| Ok(()));
        let mut error_counts = TransactionErrorMetrics::default();

        for batch in &message.0 {
            total_packet_count += batch.len();
            for packet in batch {
                // Get free id
                let transaction_id = container.reserve_key();
                // Run sanitization and checks
                let maybe_priority_id = container
                    .with_mut_transaction_state(&transaction_id, |state| {
                        let transaction = &mut state.mut_transaction_ttl().transaction;
                        transaction.populate_from(packet)?;
                        transaction.sanitize().ok()?;
                        transaction
                            .validate_account_locks(transaction_account_lock_limit)
                            .ok()?;
                        transaction.resolve_addresses(&bank).ok()?;
                        transaction.verify_precompiles(feature_set).ok()?;

                        let instruction_details = InstructionDetails::new(transaction).ok()?;
                        let (priority, cost) =
                            calculate_priority_and_cost(transaction, &instruction_details, &bank);

                        state.set_priority(priority);
                        state.set_cost(cost);
                        // TODO: fix this, should come from packet flags
                        state.set_should_forward(false);
                        state.mut_transaction_ttl().max_age_slot = last_slot_in_epoch;

                        Some(TransactionPriorityId::new(priority, transaction_id))
                    })
                    .expect("transaction must exist");
                let Some(priority_id) = maybe_priority_id else {
                    container.remove_by_id(&transaction_id);
                    continue;
                };

                let a_tx_was_dropped = container.push_id_into_queue(priority_id);
                if a_tx_was_dropped {
                    saturating_add_assign!(num_dropped_on_capacity, 1);
                }
                saturating_add_assign!(num_buffered, 1);
            }
        }

        count_metrics.update(|count_metrics| {
            saturating_add_assign!(count_metrics.num_received, total_packet_count);
            saturating_add_assign!(
                count_metrics.num_dropped_on_capacity,
                num_dropped_on_capacity
            );
            saturating_add_assign!(count_metrics.num_buffered, num_buffered);
        });
    }
}

impl ReceiveAndBufferPackets<TransactionView, TransactionViewStateContainer>
    for TransactionViewReceiveAndBuffer
{
    /// Returns whether the packet receiver is still connected.
    fn receive_and_buffer_packets(
        &self,
        decision: &BufferedPacketsDecision,
        timing_metrics: &mut SchedulerTimingMetrics,
        count_metrics: &mut SchedulerCountMetrics,
        container: &mut TransactionViewStateContainer,
    ) -> bool {
        // If we are already the leader, do not do a blocking receive, but still
        // receive for up to 10ms.
        let now = Instant::now();

        // Perform initial receive with timeout if not leader
        let mut total_buffer_time_us = 0;
        let mut total_receive_time_us = 0;
        let mut connected = match decision {
            BufferedPacketsDecision::Consume(_) => true,
            BufferedPacketsDecision::Forward
            | BufferedPacketsDecision::ForwardAndHold
            | BufferedPacketsDecision::Hold => {
                // If not leader, block up to 100ms waiting for initial message
                let (maybe_message, receive_time_us) =
                    measure_us!(self.receiver.recv_timeout(Duration::from_millis(100)));
                total_receive_time_us += receive_time_us;
                match maybe_message {
                    Ok(message) => {
                        let (_, buffer_time_us) = measure_us!(self.handle_message(
                            message,
                            decision,
                            timing_metrics,
                            count_metrics,
                            container
                        ));
                        total_buffer_time_us += buffer_time_us;
                        true
                    }
                    Err(RecvTimeoutError::Timeout) => true,
                    Err(RecvTimeoutError::Disconnected) => false,
                }
            }
        };

        // After initial receive, do not spend more than 10ms receiving and buffering.
        const MAX_RECEIVE_AND_BUFFER_TIME: Duration = Duration::from_millis(10);
        while connected && now.elapsed() < MAX_RECEIVE_AND_BUFFER_TIME {
            let (maybe_message, receive_time_us) = measure_us!(self.receiver.try_recv());
            total_receive_time_us += receive_time_us;
            connected &= match maybe_message {
                Ok(message) => {
                    let (_, buffer_time_us) = measure_us!(self.handle_message(
                        message,
                        decision,
                        timing_metrics,
                        count_metrics,
                        container
                    ));
                    total_buffer_time_us += buffer_time_us;
                    true
                }
                Err(TryRecvError::Disconnected) => false,
                Err(TryRecvError::Empty) => break, // no more messages
            };
        }

        timing_metrics.update(|timing_metrics| {
            saturating_add_assign!(timing_metrics.receive_time_us, total_receive_time_us);
            saturating_add_assign!(timing_metrics.buffer_time_us, total_buffer_time_us);
        });

        connected
    }
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
    transaction: &impl SignedMessage,
    instruction_details: &InstructionDetails,
    bank: &Bank,
) -> (u64, u64) {
    let cost = CostModel::calculate_cost_sum(transaction, instruction_details, &bank.feature_set);
    let reward = bank
        .calculate_reward_for_transaction(transaction, &FeeBudgetLimits::from(instruction_details));

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
