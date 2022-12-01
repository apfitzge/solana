use {
    super::{CommitTransactionDetails, PreBalanceInfo},
    crate::leader_slot_banking_stage_timing_metrics::LeaderExecuteAndCommitTimings,
    solana_ledger::{
        blockstore_processor::TransactionStatusSender, token_balances::collect_token_balances,
    },
    solana_measure::measure,
    solana_runtime::{
        accounts::TransactionLoadResult,
        bank::{
            Bank, CommitTransactionCounts, TransactionBalancesSet, TransactionExecutionResult,
            TransactionResults,
        },
        bank_utils,
        transaction_batch::TransactionBatch,
        vote_sender_types::ReplayVoteSender,
    },
    solana_sdk::{saturating_add_assign, transaction::SanitizedTransaction},
    solana_transaction_status::token_balances::TransactionTokenBalancesSet,
    std::sync::Arc,
};

pub struct CommitExecutor;

impl CommitExecutor {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn commit_transactions(
        batch: &TransactionBatch,
        loaded_transactions: &mut [TransactionLoadResult],
        execution_results: Vec<TransactionExecutionResult>,
        sanitized_txs: &[SanitizedTransaction],
        starting_transaction_index: Option<usize>,
        bank: &Arc<Bank>,
        pre_balance_info: &mut PreBalanceInfo,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        signature_count: u64,
        executed_transactions_count: usize,
        executed_with_successful_result_count: usize,
    ) -> (u64, Vec<CommitTransactionDetails>) {
        inc_new_counter_info!(
            "banking_stage-record_transactions_num_to_commit",
            executed_transactions_count
        );

        let (last_blockhash, lamports_per_signature) =
            bank.last_blockhash_and_lamports_per_signature();

        let (tx_results, commit_time) = measure!(
            bank.commit_transactions(
                sanitized_txs,
                loaded_transactions,
                execution_results,
                last_blockhash,
                lamports_per_signature,
                CommitTransactionCounts {
                    committed_transactions_count: executed_transactions_count as u64,
                    committed_with_failure_result_count: executed_transactions_count
                        .saturating_sub(executed_with_successful_result_count)
                        as u64,
                    signature_count,
                },
                &mut execute_and_commit_timings.execute_timings,
            ),
            "commit",
        );
        let commit_time_us = commit_time.as_us();
        execute_and_commit_timings.commit_us = commit_time_us;

        let commit_transaction_statuses = tx_results
            .execution_results
            .iter()
            .map(|execution_result| match execution_result.details() {
                Some(details) => CommitTransactionDetails::Committed {
                    compute_units: details.executed_units,
                },
                None => CommitTransactionDetails::NotCommitted,
            })
            .collect();

        let (_, find_and_send_votes_time) = measure!(
            {
                bank_utils::find_and_send_votes(
                    sanitized_txs,
                    &tx_results,
                    Some(gossip_vote_sender),
                );
                Self::collect_balances_and_send_status_batch(
                    transaction_status_sender,
                    tx_results,
                    bank,
                    batch,
                    pre_balance_info,
                    starting_transaction_index,
                );
            },
            "find_and_send_votes",
        );
        execute_and_commit_timings.find_and_send_votes_us = find_and_send_votes_time.as_us();
        (commit_time_us, commit_transaction_statuses)
    }

    fn collect_balances_and_send_status_batch(
        transaction_status_sender: &Option<TransactionStatusSender>,
        tx_results: TransactionResults,
        bank: &Arc<Bank>,
        batch: &TransactionBatch,
        pre_balance_info: &mut PreBalanceInfo,
        starting_transaction_index: Option<usize>,
    ) {
        if let Some(transaction_status_sender) = transaction_status_sender {
            let txs = batch.sanitized_transactions().to_vec();
            let post_balances = bank.collect_balances(batch);
            let post_token_balances =
                collect_token_balances(bank, batch, &mut pre_balance_info.mint_decimals);
            let mut transaction_index = starting_transaction_index.unwrap_or_default();
            let batch_transaction_indexes: Vec<_> = tx_results
                .execution_results
                .iter()
                .map(|result| {
                    if result.was_executed() {
                        let this_transaction_index = transaction_index;
                        saturating_add_assign!(transaction_index, 1);
                        this_transaction_index
                    } else {
                        0
                    }
                })
                .collect();
            transaction_status_sender.send_transaction_status_batch(
                bank.clone(),
                txs,
                tx_results.execution_results,
                TransactionBalancesSet::new(
                    std::mem::take(&mut pre_balance_info.native),
                    post_balances,
                ),
                TransactionTokenBalancesSet::new(
                    std::mem::take(&mut pre_balance_info.token),
                    post_token_balances,
                ),
                tx_results.rent_debits,
                batch_transaction_indexes,
            );
        }
    }
}
