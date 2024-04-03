use {
    super::transaction_state_container::TransactionStateContainer,
    crate::banking_stage::{
        decision_maker::{BufferedPacketsDecision, DecisionMaker},
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        packet_deserializer::PacketDeserializer,
        transaction_scheduler::transaction_state::SanitizedTransactionTTL,
    },
    crossbeam_channel::RecvTimeoutError,
    solana_cost_model::cost_model::CostModel,
    solana_program_runtime::compute_budget_processor::process_compute_budget_instructions,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_sdk::{
        clock::MAX_PROCESSING_AGE,
        feature_set::{
            include_loaded_accounts_data_size_in_fee_calculation,
            remove_rounding_in_fee_calculation,
        },
        fee::FeeBudgetLimits,
        transaction::SanitizedTransaction,
    },
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        time::Duration,
    },
};

/// A simple worker that receives, sanitizes, and buffers new transactions.
#[derive(Clone)]
pub struct SanitizingWorker {
    /// Exit signal for the scheduler.
    exit: Arc<AtomicBool>,
    /// Decision maker for determining what should be done with transactions.
    decision_maker: DecisionMaker,
    /// Packet/Transaction ingress.
    packet_receiver: PacketDeserializer,
    bank_forks: Arc<RwLock<BankForks>>,
    /// Container for transaction state.
    /// Shared resource between `packet_receiver` and `scheduler`.
    container: Arc<TransactionStateContainer>,
}

impl SanitizingWorker {
    /// Create a new `SanitizingWorker`.
    pub fn new(
        exit: Arc<AtomicBool>,
        decision_maker: DecisionMaker,
        packet_receiver: PacketDeserializer,
        bank_forks: Arc<RwLock<BankForks>>,
        container: Arc<TransactionStateContainer>,
    ) -> Self {
        Self {
            exit,
            decision_maker,
            packet_receiver,
            bank_forks,
            container,
        }
    }

    /// Run the worker.
    pub fn run(self) {
        loop {
            let decision = self.decision_maker.make_consume_or_forward_decision();
            if !self.receive_and_buffer_packets(&decision) {
                break;
            }
        }

        self.exit.store(true, Ordering::Relaxed);
    }

    /// Returns whether the packet receiver is still connected.
    pub fn receive_and_buffer_packets(&self, decision: &BufferedPacketsDecision) -> bool {
        let remaining_queue_capacity = self.container.remaining_queue_capacity();

        const MAX_PACKET_RECEIVE_TIME: Duration = Duration::from_millis(100);
        let (recv_timeout, should_buffer) = match decision {
            BufferedPacketsDecision::Consume(_) => (
                if self.container.is_empty() {
                    MAX_PACKET_RECEIVE_TIME
                } else {
                    Duration::ZERO
                },
                true,
            ),
            BufferedPacketsDecision::Forward => (MAX_PACKET_RECEIVE_TIME, false),
            BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Hold => {
                (MAX_PACKET_RECEIVE_TIME, true)
            }
        };

        let received_packet_results =
            self.packet_receiver
                .receive_packets(recv_timeout, remaining_queue_capacity, |_| true);

        match received_packet_results {
            Ok(receive_packet_results) => {
                if should_buffer {
                    self.buffer_packets(receive_packet_results.deserialized_packets);
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return false,
        }

        true
    }

    fn buffer_packets(&self, packets: Vec<ImmutableDeserializedPacket>) {
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
            let (transactions, fee_budget_limits_vec): (Vec<_>, Vec<_>) = chunk
                .iter()
                .filter_map(|packet| {
                    packet.build_sanitized_transaction(feature_set, vote_only, bank.as_ref())
                })
                .filter(|tx| {
                    SanitizedTransaction::validate_account_locks(
                        tx.message(),
                        transaction_account_lock_limit,
                    )
                    .is_ok()
                })
                .filter_map(|tx| {
                    process_compute_budget_instructions(tx.message().program_instructions_iter())
                        .map(|compute_budget| (tx, compute_budget.into()))
                        .ok()
                })
                .unzip();

            let check_results = bank.check_transactions(
                &transactions,
                &lock_results[..transactions.len()],
                MAX_PROCESSING_AGE,
                &mut error_counts,
            );

            for ((transaction, fee_budget_limits), _) in transactions
                .into_iter()
                .zip(fee_budget_limits_vec)
                .zip(check_results)
                .filter(|(_, check_result)| check_result.0.is_ok())
            {
                let (priority, cost) =
                    Self::calculate_priority_and_cost(&transaction, &fee_budget_limits, &bank);
                let transaction_ttl = SanitizedTransactionTTL {
                    transaction,
                    max_age_slot: last_slot_in_epoch,
                };

                self.container
                    .insert_new_transaction(transaction_ttl, priority, cost);
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
        transaction: &SanitizedTransaction,
        fee_budget_limits: &FeeBudgetLimits,
        bank: &Bank,
    ) -> (u64, u64) {
        let cost = CostModel::calculate_cost(transaction, &bank.feature_set).sum();
        let fee = bank.fee_structure.calculate_fee(
            transaction.message(),
            5_000, // this just needs to be non-zero
            fee_budget_limits,
            bank.feature_set
                .is_active(&include_loaded_accounts_data_size_in_fee_calculation::id()),
            bank.feature_set
                .is_active(&remove_rounding_in_fee_calculation::id()),
        );

        // We need a multiplier here to avoid rounding down too aggressively.
        // For many transactions, the cost will be greater than the fees in terms of raw lamports.
        // For the purposes of calculating prioritization, we multiply the fees by a large number so that
        // the cost is a small fraction.
        // An offset of 1 is used in the denominator to explicitly avoid division by zero.
        const MULTIPLIER: u64 = 1_000_000;
        (
            fee.saturating_mul(MULTIPLIER)
                .saturating_div(cost.saturating_add(1)),
            cost,
        )
    }
}
