use {
    super::{
        consume_executor::ConsumeExecutor, decision_maker::BufferedPacketsDecision,
        forward_executor::ForwardExecutor, scheduler_error::SchedulerError, BankingStageStats,
        ForwardOption,
    },
    crate::{
        leader_slot_banking_stage_metrics::LeaderSlotMetricsTracker,
        scheduler_stage::{
            ProcessedTransactions, ProcessedTransactionsSender, ScheduledTransactions,
            ScheduledTransactionsReceiver,
        },
    },
    crossbeam_channel::TryRecvError,
    solana_measure::{measure, measure_us},
};

/// Handle interface for interacting with an external scheduler
pub struct ExternalSchedulerHandle {
    scheduled_transactions_receiver: ScheduledTransactionsReceiver,
    processed_transactions_sender: ProcessedTransactionsSender,
    banking_stage_stats: BankingStageStats,
}

impl ExternalSchedulerHandle {
    pub fn new(
        id: u32,
        scheduled_transactions_receiver: ScheduledTransactionsReceiver,
        processed_transactions_sender: ProcessedTransactionsSender,
    ) -> Self {
        Self {
            scheduled_transactions_receiver,
            processed_transactions_sender,
            banking_stage_stats: BankingStageStats::new(id),
        }
    }

    pub fn tick(&mut self) -> Result<(), SchedulerError> {
        Ok(())
    }

    pub fn do_scheduled_work(
        &mut self,
        consume_executor: &ConsumeExecutor,
        forward_executor: &ForwardExecutor,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) -> Result<(), SchedulerError> {
        loop {
            match self.scheduled_transactions_receiver.try_recv() {
                Ok(scheduled_transactions) => {
                    let procssed_transactions = self.process_scheduled_transactions(
                        scheduled_transactions,
                        consume_executor,
                        forward_executor,
                        slot_metrics_tracker,
                    );

                    if self
                        .processed_transactions_sender
                        .send(procssed_transactions)
                        .is_err()
                    {
                        return Err(SchedulerError::ProcessedTransactionsSenderDisconnected);
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    return Err(SchedulerError::ScheduledWorkReceiverDisconnected)
                }
                _ => break, // No more work to do
            }
        }

        Ok(())
    }

    // TODO: fix forwarding leader metrics
    fn process_scheduled_transactions(
        &self,
        scheduled_transactions: ScheduledTransactions,
        consume_executor: &ConsumeExecutor,
        forward_executor: &ForwardExecutor,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) -> ProcessedTransactions {
        // Check if the scheduled transactions are still valid
        // TODO: metrics
        if !scheduled_transactions.is_valid() {
            let num_packets = scheduled_transactions.transactions.len();
            return ProcessedTransactions {
                thread_id: scheduled_transactions.thread_id,
                packets: scheduled_transactions.packets,
                transactions: scheduled_transactions.transactions,
                retryable: vec![true; num_packets],
            };
        }

        match scheduled_transactions.decision {
            BufferedPacketsDecision::Consume(ref bank_start) => {
                slot_metrics_tracker.apply_working_bank(Some(bank_start));
                let (process_transactions_summary, process_packets_transactions_us) =
                    measure_us!(consume_executor.process_packets_transactions(
                        bank_start,
                        &scheduled_transactions.transactions,
                        &self.banking_stage_stats,
                        slot_metrics_tracker,
                    ));
                if process_transactions_summary.reached_max_poh_height {
                    scheduled_transactions.mark_as_invalid();
                }

                slot_metrics_tracker
                    .increment_process_packets_transactions_us(process_packets_transactions_us);

                let retryable_transaction_indexes =
                    process_transactions_summary.retryable_transaction_indexes;
                slot_metrics_tracker
                    .increment_retryable_packets_count(retryable_transaction_indexes.len() as u64);

                let mut retryable = vec![false; scheduled_transactions.transactions.len()];
                for retryable_transaction_index in retryable_transaction_indexes {
                    retryable[retryable_transaction_index] = true;
                }
                ProcessedTransactions {
                    thread_id: scheduled_transactions.thread_id,
                    packets: scheduled_transactions.packets,
                    transactions: scheduled_transactions.transactions,
                    retryable,
                }
            }
            BufferedPacketsDecision::Forward => {
                slot_metrics_tracker.apply_working_bank(None);
                let (_forward_result, sucessful_forwarded_packets_count, _leader_pubkey) =
                    forward_executor.forward_buffered_packets(
                        &ForwardOption::ForwardTransaction, // Only support transactions for now
                        scheduled_transactions
                            .packets
                            .iter()
                            .map(|p| p.immutable_section().original_packet()),
                        &self.banking_stage_stats,
                    );
                slot_metrics_tracker.increment_successful_forwarded_packets_count(
                    sucessful_forwarded_packets_count as u64,
                );

                ProcessedTransactions::default() // No retryable packets
            }

            BufferedPacketsDecision::ForwardAndHold => {
                slot_metrics_tracker.apply_working_bank(None);
                let (_forward_result, sucessful_forwarded_packets_count, _leader_pubkey) =
                    forward_executor.forward_buffered_packets(
                        &ForwardOption::ForwardTransaction, // Only support transactions for now
                        scheduled_transactions
                            .packets
                            .iter()
                            .map(|p| p.immutable_section().original_packet()),
                        &self.banking_stage_stats,
                    );
                slot_metrics_tracker.increment_successful_forwarded_packets_count(
                    sucessful_forwarded_packets_count as u64,
                );

                let ScheduledTransactions {
                    thread_id,
                    mut packets,
                    transactions,
                    ..
                } = scheduled_transactions;

                // Mark all packets as forwarded
                for packet in &mut packets {
                    packet.forwarded = true;
                }
                let num_transactions = transactions.len();

                ProcessedTransactions {
                    thread_id,
                    packets,
                    transactions,
                    retryable: vec![true; num_transactions],
                }
            }

            _ => panic!("Unexpected decision"),
        }
    }
}
