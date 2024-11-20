use {
    super::{
        scheduler_metrics::{SchedulerCountMetrics, SchedulerTimingMetrics},
        transaction_id_generator::TransactionIdGenerator,
        transaction_state_container::{StateContainer, TransactionStateContainerWithBytes},
    },
    crate::{
        banking_stage::{
            decision_maker::BufferedPacketsDecision,
            immutable_deserialized_packet::ImmutableDeserializedPacket,
            packet_deserializer::PacketDeserializer, packet_filter::check_excessive_precompiles,
            scheduler_messages::MaxAge,
            transaction_scheduler::transaction_state::SanitizedTransactionTTL,
            TransactionStateContainer,
        },
        banking_trace::{BankingPacketBatch, BankingPacketReceiver},
    },
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView,
        transaction_view::SanitizedTransactionView,
    },
    arrayvec::ArrayVec,
    bytes::Bytes,
    core::time::Duration,
    crossbeam_channel::{RecvTimeoutError, TryRecvError},
    solana_accounts_db::account_locks::validate_account_locks,
    solana_cost_model::cost_model::CostModel,
    solana_measure::measure_us,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_runtime_transaction::{
        instructions_processor::process_compute_budget_instructions,
        runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
        transaction_with_meta::TransactionWithMeta,
    },
    solana_sdk::{
        address_lookup_table::state::estimate_last_valid_slot,
        clock::{Epoch, Slot, MAX_PROCESSING_AGE},
        fee::FeeBudgetLimits,
        saturating_add_assign,
        transaction::{MessageHash, SanitizedTransaction},
    },
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_svm_transaction::svm_message::SVMMessage,
    std::{
        sync::{Arc, RwLock},
        time::Instant,
    },
};

pub(crate) trait ReceiveAndBuffer {
    type Transaction: TransactionWithMeta + Send + Sync;
    type Container: StateContainer<Self::Transaction> + Send + Sync;

    /// Returns whether the packet receiver is still connected.
    fn receive_and_buffer_packets(
        &mut self,
        container: &mut Self::Container,
        timing_metrics: &mut SchedulerTimingMetrics,
        count_metrics: &mut SchedulerCountMetrics,
        decision: &BufferedPacketsDecision,
    ) -> bool;
}

pub struct SanitizedTransactionReceiveAndBuffer {
    /// Packet/Transaction ingress.
    packet_receiver: PacketDeserializer,
    bank_forks: Arc<RwLock<BankForks>>,
    /// Generates unique IDs for incoming transactions.
    transaction_id_generator: TransactionIdGenerator,

    forwarding_enabled: bool,
}

impl ReceiveAndBuffer for SanitizedTransactionReceiveAndBuffer {
    type Transaction = RuntimeTransaction<SanitizedTransaction>;
    type Container = TransactionStateContainer<Self::Transaction>;

    /// Returns whether the packet receiver is still connected.
    fn receive_and_buffer_packets(
        &mut self,
        container: &mut Self::Container,
        timing_metrics: &mut SchedulerTimingMetrics,
        count_metrics: &mut SchedulerCountMetrics,
        decision: &BufferedPacketsDecision,
    ) -> bool {
        let remaining_queue_capacity = container.remaining_queue_capacity();

        const MAX_PACKET_RECEIVE_TIME: Duration = Duration::from_millis(10);
        let (recv_timeout, should_buffer) = match decision {
            BufferedPacketsDecision::Consume(_) => (
                if container.is_empty() {
                    MAX_PACKET_RECEIVE_TIME
                } else {
                    Duration::ZERO
                },
                true,
            ),
            BufferedPacketsDecision::Forward => (MAX_PACKET_RECEIVE_TIME, self.forwarding_enabled),
            BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Hold => {
                (MAX_PACKET_RECEIVE_TIME, true)
            }
        };

        let (received_packet_results, receive_time_us) = measure_us!(self
            .packet_receiver
            .receive_packets(recv_timeout, remaining_queue_capacity, |packet| {
                packet.check_excessive_precompiles()?;
                Ok(packet)
            }));

        timing_metrics.update(|timing_metrics| {
            saturating_add_assign!(timing_metrics.receive_time_us, receive_time_us);
        });

        match received_packet_results {
            Ok(receive_packet_results) => {
                let num_received_packets = receive_packet_results.deserialized_packets.len();

                count_metrics.update(|count_metrics| {
                    saturating_add_assign!(count_metrics.num_received, num_received_packets);
                });

                if should_buffer {
                    let (_, buffer_time_us) = measure_us!(self.buffer_packets(
                        container,
                        timing_metrics,
                        count_metrics,
                        receive_packet_results.deserialized_packets
                    ));
                    timing_metrics.update(|timing_metrics| {
                        saturating_add_assign!(timing_metrics.buffer_time_us, buffer_time_us);
                    });
                } else {
                    count_metrics.update(|count_metrics| {
                        saturating_add_assign!(
                            count_metrics.num_dropped_on_receive,
                            num_received_packets
                        );
                    });
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return false,
        }

        true
    }
}

impl SanitizedTransactionReceiveAndBuffer {
    pub fn new(
        packet_receiver: PacketDeserializer,
        bank_forks: Arc<RwLock<BankForks>>,
        forwarding_enabled: bool,
    ) -> Self {
        Self {
            packet_receiver,
            bank_forks,
            transaction_id_generator: TransactionIdGenerator::default(),
            forwarding_enabled,
        }
    }

    fn buffer_packets(
        &mut self,
        container: &mut TransactionStateContainer<RuntimeTransaction<SanitizedTransaction>>,
        _timing_metrics: &mut SchedulerTimingMetrics,
        count_metrics: &mut SchedulerCountMetrics,
        packets: Vec<ImmutableDeserializedPacket>,
    ) {
        // Convert to Arcs
        let packets: Vec<_> = packets.into_iter().map(Arc::new).collect();
        // Sanitize packets, generate IDs, and insert into the container.
        let (root_bank, working_bank) = {
            let bank_forks = self.bank_forks.read().unwrap();
            let root_bank = bank_forks.root_bank();
            let working_bank = bank_forks.working_bank();
            (root_bank, working_bank)
        };
        let alt_resolved_slot = root_bank.slot();
        let sanitized_epoch = root_bank.epoch();
        let transaction_account_lock_limit = working_bank.get_transaction_account_lock_limit();
        let vote_only = working_bank.vote_only_bank();

        const CHUNK_SIZE: usize = 128;
        let lock_results: [_; CHUNK_SIZE] = core::array::from_fn(|_| Ok(()));

        let mut arc_packets = ArrayVec::<_, CHUNK_SIZE>::new();
        let mut transactions = ArrayVec::<_, CHUNK_SIZE>::new();
        let mut max_ages = ArrayVec::<_, CHUNK_SIZE>::new();
        let mut fee_budget_limits_vec = ArrayVec::<_, CHUNK_SIZE>::new();

        let mut error_counts = TransactionErrorMetrics::default();
        for chunk in packets.chunks(CHUNK_SIZE) {
            let mut post_sanitization_count: usize = 0;
            chunk
                .iter()
                .filter_map(|packet| {
                    packet
                        .build_sanitized_transaction(
                            vote_only,
                            root_bank.as_ref(),
                            root_bank.get_reserved_account_keys(),
                        )
                        .map(|(tx, deactivation_slot)| (packet.clone(), tx, deactivation_slot))
                })
                .inspect(|_| saturating_add_assign!(post_sanitization_count, 1))
                .filter(|(_packet, tx, _deactivation_slot)| {
                    validate_account_locks(
                        tx.message().account_keys(),
                        transaction_account_lock_limit,
                    )
                    .is_ok()
                })
                .filter_map(|(packet, tx, deactivation_slot)| {
                    process_compute_budget_instructions(SVMMessage::program_instructions_iter(&tx))
                        .map(|compute_budget| {
                            (packet, tx, deactivation_slot, compute_budget.into())
                        })
                        .ok()
                })
                .for_each(|(packet, tx, deactivation_slot, fee_budget_limits)| {
                    arc_packets.push(packet);
                    transactions.push(tx);
                    max_ages.push(calculate_max_age(
                        sanitized_epoch,
                        deactivation_slot,
                        alt_resolved_slot,
                    ));
                    fee_budget_limits_vec.push(fee_budget_limits);
                });

            let check_results = working_bank.check_transactions(
                &transactions,
                &lock_results[..transactions.len()],
                MAX_PROCESSING_AGE,
                &mut error_counts,
            );
            let post_lock_validation_count = transactions.len();

            let mut post_transaction_check_count: usize = 0;
            let mut num_dropped_on_capacity: usize = 0;
            let mut num_buffered: usize = 0;
            for ((((packet, transaction), max_age), fee_budget_limits), _check_result) in
                arc_packets
                    .drain(..)
                    .zip(transactions.drain(..))
                    .zip(max_ages.drain(..))
                    .zip(fee_budget_limits_vec.drain(..))
                    .zip(check_results)
                    .filter(|(_, check_result)| check_result.is_ok())
            {
                saturating_add_assign!(post_transaction_check_count, 1);
                let transaction_id = self.transaction_id_generator.next();

                let (priority, cost) =
                    calculate_priority_and_cost(&transaction, &fee_budget_limits, &working_bank);
                let transaction_ttl = SanitizedTransactionTTL {
                    transaction,
                    max_age,
                };

                if container.insert_new_transaction(
                    transaction_id,
                    transaction_ttl,
                    Some(packet),
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

pub(crate) struct TransactionViewReceiveAndBuffer {
    pub receiver: BankingPacketReceiver,
    pub bank_forks: Arc<RwLock<BankForks>>,
}

impl ReceiveAndBuffer for TransactionViewReceiveAndBuffer {
    type Transaction = RuntimeTransaction<ResolvedTransactionView<Bytes>>;
    type Container = TransactionStateContainerWithBytes;

    fn receive_and_buffer_packets(
        &mut self,
        container: &mut Self::Container,
        timing_metrics: &mut SchedulerTimingMetrics,
        count_metrics: &mut SchedulerCountMetrics,
        decision: &BufferedPacketsDecision,
    ) -> bool {
        // Receive packet batches.
        const TIMEOUT: Duration = Duration::from_millis(10);
        let start = Instant::now();

        match decision {
            BufferedPacketsDecision::Consume(_) => {
                // We have a bank we should be working on.
                // Do not do a blocking wait, under any circumstances.
                while start.elapsed() < TIMEOUT {
                    match self.receiver.try_recv() {
                        Ok(packet_batch_message) => {
                            self.handle_message(
                                container,
                                timing_metrics,
                                count_metrics,
                                decision,
                                packet_batch_message,
                            );
                        }
                        Err(TryRecvError::Empty) => return true,
                        Err(TryRecvError::Disconnected) => return false,
                    };
                }
            }
            BufferedPacketsDecision::Forward
            | BufferedPacketsDecision::ForwardAndHold
            | BufferedPacketsDecision::Hold => {
                while start.elapsed() < TIMEOUT {
                    match self.receiver.recv_timeout(TIMEOUT) {
                        Ok(packet_batch_message) => {
                            self.handle_message(
                                container,
                                timing_metrics,
                                count_metrics,
                                decision,
                                packet_batch_message,
                            );
                        }
                        Err(RecvTimeoutError::Timeout) => return true,
                        Err(RecvTimeoutError::Disconnected) => return false,
                    };
                }
            }
        }

        true
    }
}

impl TransactionViewReceiveAndBuffer {
    fn handle_message(
        &mut self,
        container: &mut TransactionStateContainerWithBytes,
        _timing_metrics: &mut SchedulerTimingMetrics,
        _count_metrics: &mut SchedulerCountMetrics,
        _decision: &BufferedPacketsDecision,
        packet_batch_message: BankingPacketBatch,
    ) {
        // Sanitize packets, generate IDs, and insert into the container.
        let (root_bank, working_bank) = {
            let bank_forks = self.bank_forks.read().unwrap();
            let root_bank = bank_forks.root_bank();
            let working_bank = bank_forks.working_bank();
            (root_bank, working_bank)
        };
        let alt_resolved_slot = root_bank.slot();
        let sanitized_epoch = root_bank.epoch();
        let transaction_account_lock_limit = working_bank.get_transaction_account_lock_limit();

        let mut error_metrics = TransactionErrorMetrics::default();
        const CHUNK_SIZE: usize = 128;
        let lock_results: [_; CHUNK_SIZE] = core::array::from_fn(|_| Ok(()));
        let mut transactions = ArrayVec::<_, CHUNK_SIZE>::new();
        let mut max_ages = ArrayVec::<_, CHUNK_SIZE>::new();
        let mut fee_budget_limits_vec = ArrayVec::<_, CHUNK_SIZE>::new();
        let mut index_bytes_vec = ArrayVec::<_, CHUNK_SIZE>::new();

        for packet_batch in packet_batch_message.0.iter() {
            for packet in packet_batch.iter() {
                let Some(packet_data) = packet.data(..) else {
                    continue;
                };

                // The container has extra capacity, so as long as we are not
                // leaking indexes this can never fail.
                let (index, mut bytes) = container.reserve_space().expect("reserve_space failed");
                bytes.copy_from_slice(packet_data);
                let bytes = bytes.freeze();

                let Ok(view) = SanitizedTransactionView::try_new_sanitized(bytes.clone()) else {
                    container.return_space(index, bytes.try_into_mut().expect("no leaks"));
                    continue;
                };

                if check_excessive_precompiles(view.program_instructions_iter()).is_err() {
                    drop(view);
                    container.return_space(index, bytes.try_into_mut().expect("no leaks"));
                    continue;
                }

                let Ok(transaction) = RuntimeTransaction::<SanitizedTransactionView<_>>::try_from(
                    view,
                    MessageHash::Compute,
                    None,
                ) else {
                    container.return_space(index, bytes.try_into_mut().expect("no leaks"));
                    continue;
                };

                // Load addresses for the transaction.
                let Ok((loaded_addresses, deactivation_slot)) =
                    root_bank.load_addresses_from_ref(transaction.address_table_lookup_iter())
                else {
                    drop(transaction);
                    container.return_space(index, bytes.try_into_mut().expect("no leaks"));
                    continue;
                };

                let Ok(transaction) = RuntimeTransaction::<ResolvedTransactionView<_>>::try_from(
                    transaction,
                    Some(loaded_addresses),
                    root_bank.get_reserved_account_keys(),
                ) else {
                    container.return_space(index, bytes.try_into_mut().expect("no leaks"));
                    continue;
                };

                if validate_account_locks(
                    transaction.account_keys(),
                    transaction_account_lock_limit,
                )
                .is_err()
                {
                    drop(transaction);
                    container.return_space(index, bytes.try_into_mut().expect("no leaks"));
                    continue;
                }

                let Ok(compute_budget_limits) =
                    transaction.compute_budget_limits(&working_bank.feature_set)
                else {
                    drop(transaction);
                    container.return_space(index, bytes.try_into_mut().expect("no leaks"));
                    continue;
                };

                // put transaction into the batch
                transactions.push(transaction);
                max_ages.push(calculate_max_age(
                    sanitized_epoch,
                    deactivation_slot,
                    alt_resolved_slot,
                ));
                fee_budget_limits_vec.push(FeeBudgetLimits::from(compute_budget_limits));
                index_bytes_vec.push((index, bytes));

                // TODO: make sure this isn't dumb as fuck.
                if transactions.len() == CHUNK_SIZE {
                    break;
                }
            }

            let check_results = working_bank.check_transactions(
                &transactions[..],
                &lock_results[..transactions.len()],
                MAX_PROCESSING_AGE,
                &mut error_metrics,
            );

            for ((((transaction, max_age), fee_budget_limits), (index, bytes)), check_result) in
                transactions
                    .drain(..)
                    .zip(max_ages.drain(..))
                    .zip(fee_budget_limits_vec.drain(..))
                    .zip(index_bytes_vec.drain(..))
                    .zip(check_results)
            {
                if check_result.is_err() {
                    drop(transaction);
                    container.return_space(index, bytes.try_into_mut().expect("no leaks"));
                    continue;
                }

                let (priority, cost) =
                    calculate_priority_and_cost(&transaction, &fee_budget_limits, &working_bank);

                // Freeze the packets in container.
                container.freeze(index, bytes);
                container.insert_new_transaction(
                    index,
                    SanitizedTransactionTTL {
                        transaction,
                        max_age,
                    },
                    None,
                    priority,
                    cost,
                );
            }
        }
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
    transaction: &impl TransactionWithMeta,
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

/// Given the epoch, the minimum deactivation slot, and the current slot,
/// return the `MaxAge` that should be used for the transaction. This is used
/// to determine the maximum slot that a transaction will be considered valid
/// for, without re-resolving addresses or resanitizing.
///
/// This function considers the deactivation period of Address Table
/// accounts. If the deactivation period runs past the end of the epoch,
/// then the transaction is considered valid until the end of the epoch.
/// Otherwise, the transaction is considered valid until the deactivation
/// period.
///
/// Since the deactivation period technically uses blocks rather than
/// slots, the value used here is the lower-bound on the deactivation
/// period, i.e. the transaction's address lookups are valid until
/// AT LEAST this slot.
fn calculate_max_age(
    sanitized_epoch: Epoch,
    deactivation_slot: Slot,
    current_slot: Slot,
) -> MaxAge {
    let alt_min_expire_slot = estimate_last_valid_slot(deactivation_slot.min(current_slot));
    MaxAge {
        sanitized_epoch,
        alt_invalidation_slot: alt_min_expire_slot,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_max_age() {
        let current_slot = 100;
        let sanitized_epoch = 10;

        // ALT deactivation slot is delayed
        assert_eq!(
            calculate_max_age(sanitized_epoch, current_slot - 1, current_slot),
            MaxAge {
                sanitized_epoch,
                alt_invalidation_slot: current_slot - 1
                    + solana_sdk::slot_hashes::get_entries() as u64,
            }
        );

        // no deactivation slot
        assert_eq!(
            calculate_max_age(sanitized_epoch, u64::MAX, current_slot),
            MaxAge {
                sanitized_epoch,
                alt_invalidation_slot: current_slot + solana_sdk::slot_hashes::get_entries() as u64,
            }
        );
    }
}
