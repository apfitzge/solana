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

    fn process_scheduled_transactions(
        &self,
        scheduled_transactions: ScheduledTransactions,
        consume_executor: &ConsumeExecutor,
        forward_executor: &ForwardExecutor,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) -> ProcessedTransactions {
        match scheduled_transactions.decision {
            BufferedPacketsDecision::Consume(bank_start) => {
                let (process_transactions_summary, process_packets_transactions_us) =
                    measure_us!(consume_executor.process_packets_transactions(
                        &bank_start,
                        &scheduled_transactions.transactions,
                        &self.banking_stage_stats,
                        slot_metrics_tracker,
                    ));
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
            BufferedPacketsDecision::Forward | BufferedPacketsDecision::ForwardAndHold => {
                let (_forward_result, sucessful_forwarded_packets_count, _leader_pubkey) =
                    forward_executor.forward_buffered_packets(
                        &ForwardOption::ForwardTransaction, // Only support transactions for now
                        scheduled_transactions
                            .packets
                            .iter()
                            .map(|p| p.original_packet()),
                        &self.banking_stage_stats,
                    );
                slot_metrics_tracker.increment_successful_forwarded_packets_count(
                    sucessful_forwarded_packets_count as u64,
                );

                ProcessedTransactions::default() // No retryable packets
            }

            _ => panic!("Unexpected decision"),
        }
    }
}
