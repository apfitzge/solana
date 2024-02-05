use {
    super::transaction_state_container::TransactionStateContainer,
    crate::{
        banking_stage::{
            consumer::Consumer,
            decision_maker::{BufferedPacketsDecision, DecisionMaker},
            immutable_deserialized_packet::ImmutableDeserializedPacket,
            transaction_scheduler::transaction_state::SanitizedTransactionTTL,
        },
        banking_trace::BankingPacketReceiver,
    },
    solana_accounts_db::transaction_error_metrics::TransactionErrorMetrics,
    solana_cost_model::cost_model::CostModel,
    solana_perf::packet::PacketBatch,
    solana_runtime::{
        bank::Bank, bank_forks::BankForks, transaction_priority_details::TransactionPriorityDetails,
    },
    solana_sdk::{
        clock::MAX_PROCESSING_AGE,
        slot_history::Slot,
        transaction::{SanitizedTransaction, TransactionError},
    },
    std::{
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    },
};

pub struct ReceiveWorker {
    /// Decision maker for determining what should be done with transactions.
    decision_maker: DecisionMaker,
    /// Packet/Transaction ingress.
    packet_batch_receiver: BankingPacketReceiver,
    bank_forks: Arc<RwLock<BankForks>>,
    /// Container for transaction state.
    /// Shared resource between `packet_receiver` and `scheduler`.
    container: Arc<TransactionStateContainer>,
}

impl ReceiveWorker {
    pub fn new(
        decision_maker: DecisionMaker,
        packet_batch_receiver: BankingPacketReceiver,
        bank_forks: Arc<RwLock<BankForks>>,
        container: Arc<TransactionStateContainer>,
    ) -> Self {
        Self {
            decision_maker,
            packet_batch_receiver,
            bank_forks,
            container,
        }
    }

    pub fn run(self) {
        let mut last_decision = (
            Instant::now(),
            self.decision_maker.make_consume_or_forward_decision(),
        );
        while let Ok(packet_batches) = self.packet_batch_receiver.recv() {
            if last_decision.0.elapsed() > Duration::from_millis(10) {
                last_decision.1 = self.decision_maker.make_consume_or_forward_decision();
                last_decision.0 = Instant::now();
            }

            let should_buffer = match &last_decision.1 {
                BufferedPacketsDecision::Consume(_)
                | BufferedPacketsDecision::ForwardAndHold
                | BufferedPacketsDecision::Hold => true,
                BufferedPacketsDecision::Forward => false,
            };

            for packet_batch in &packet_batches.0 {
                let packet_indexes = Self::generate_packet_indexes(packet_batch);
                if should_buffer {
                    self.buffer_packets(Self::deserialize_packets(packet_batch, &packet_indexes))
                }
            }
        }
    }

    fn generate_packet_indexes(packet_batch: &PacketBatch) -> Vec<usize> {
        packet_batch
            .iter()
            .enumerate()
            .filter(|(_, pkt)| !pkt.meta().discard())
            .map(|(index, _)| index)
            .collect()
    }

    fn deserialize_packets<'a>(
        packet_batch: &'a PacketBatch,
        packet_indexes: &'a [usize],
    ) -> impl Iterator<Item = ImmutableDeserializedPacket> + 'a {
        packet_indexes.iter().filter_map(move |packet_index| {
            ImmutableDeserializedPacket::new(packet_batch[*packet_index].clone()).ok()
        })
    }

    fn buffer_packets(&self, packets: impl Iterator<Item = ImmutableDeserializedPacket>) {
        // Sanitize packets, generate IDs, and insert into the container.
        let bank = self.bank_forks.read().unwrap().working_bank();
        let last_slot_in_epoch = bank.epoch_schedule().get_last_slot_in_epoch(bank.epoch());
        let transaction_account_lock_limit = bank.get_transaction_account_lock_limit();
        let feature_set = &bank.feature_set;
        let vote_only = bank.vote_only_bank();

        const CHUNK_SIZE: usize = 128;
        let lock_results: [_; CHUNK_SIZE] = core::array::from_fn(|_| Ok(()));
        let mut transactions = Vec::with_capacity(CHUNK_SIZE);
        let mut priority_details = Vec::with_capacity(CHUNK_SIZE);
        let mut error_counts = TransactionErrorMetrics::default();

        fn process_chunk(
            transactions: &mut Vec<SanitizedTransaction>,
            priority_details: &mut Vec<TransactionPriorityDetails>,
            container: &TransactionStateContainer,
            bank: &Bank,
            last_slot_in_epoch: Slot,
            lock_results: &[Result<(), TransactionError>; CHUNK_SIZE],
            error_counts: &mut TransactionErrorMetrics,
        ) {
            let check_results = bank.check_transactions(
                transactions,
                &lock_results[..transactions.len()],
                MAX_PROCESSING_AGE,
                error_counts,
            );

            for ((transaction, priority_details), _check_result) in transactions
                .drain(..)
                .zip(priority_details.drain(..))
                .zip(check_results)
                .filter(|(_, check_result)| check_result.0.is_ok())
                .filter(|((tx, _), _)| {
                    Consumer::check_fee_payer_unlocked(bank, tx.message(), error_counts).is_ok()
                })
            {
                let cost = CostModel::calculate_cost(&transaction, &bank.feature_set);
                let transaction_ttl = SanitizedTransactionTTL {
                    transaction,
                    max_age_slot: last_slot_in_epoch,
                };

                container.insert_new_transaction(transaction_ttl, priority_details, cost);
            }
        }

        for (tx, pd) in packets
            .filter_map(|packet| {
                packet
                    .build_sanitized_transaction(feature_set, vote_only, bank.as_ref())
                    .map(|tx| (tx, packet.priority_details()))
            })
            .filter(|(tx, _)| {
                SanitizedTransaction::validate_account_locks(
                    tx.message(),
                    transaction_account_lock_limit,
                )
                .is_ok()
            })
        {
            transactions.push(tx);
            priority_details.push(pd);

            if transactions.len() == CHUNK_SIZE {
                process_chunk(
                    &mut transactions,
                    &mut priority_details,
                    &self.container,
                    &bank,
                    last_slot_in_epoch,
                    &lock_results,
                    &mut error_counts,
                );
            }
        }

        if !transactions.is_empty() {
            process_chunk(
                &mut transactions,
                &mut priority_details,
                &self.container,
                &bank,
                last_slot_in_epoch,
                &lock_results,
                &mut error_counts,
            );
        }
    }
}
