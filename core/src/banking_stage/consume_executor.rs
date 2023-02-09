use {
    super::BankingStageStats,
    crate::{
        banking_stage::{
            commit_executor::CommitExecutor,
            record_executor::{RecordExecutor, RecordTransactionsSummary},
            CommitTransactionDetails, ExecuteAndCommitTransactionsOutput, PreBalanceInfo,
            ProcessTransactionBatchOutput, MAX_NUM_TRANSACTIONS_PER_BATCH,
        },
        leader_slot_banking_stage_metrics::{LeaderSlotMetricsTracker, ProcessTransactionsSummary},
        leader_slot_banking_stage_timing_metrics::{
            LeaderExecuteAndCommitTimings, RecordTransactionsTimings,
        },
        qos_service::QosService,
    },
    itertools::Itertools,
    solana_ledger::token_balances::collect_token_balances,
    solana_measure::{measure, measure_us},
    solana_poh::poh_recorder::{BankStart, PohRecorderError},
    solana_program_runtime::timings::ExecuteTimings,
    solana_runtime::{
        bank::{Bank, LoadAndExecuteTransactionsOutput, TransactionCheckResult},
        bank_status::BankStatus,
        transaction_batch::TransactionBatch,
        transaction_error_metrics::TransactionErrorMetrics,
    },
    solana_sdk::{
        clock::{FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET, MAX_PROCESSING_AGE},
        transaction::{self, SanitizedTransaction, TransactionError},
    },
    std::sync::{atomic::Ordering, Arc},
};

pub struct ConsumeExecutor {
    record_executor: RecordExecutor,
    commit_executor: CommitExecutor,
    qos_service: QosService,
    log_messages_bytes_limit: Option<usize>,
}

impl ConsumeExecutor {
    pub fn new(
        record_executor: RecordExecutor,
        commit_executor: CommitExecutor,
        qos_service: QosService,
        log_messages_bytes_limit: Option<usize>,
    ) -> Self {
        Self {
            record_executor,
            commit_executor,
            qos_service,
            log_messages_bytes_limit,
        }
    }

    pub(crate) fn process_packets_transactions<'a>(
        &self,
        bank_start: &BankStart,
        sanitized_transactions: &[SanitizedTransaction],
        banking_stage_stats: &'a BankingStageStats,
        slot_metrics_tracker: &'a mut LeaderSlotMetricsTracker,
    ) -> ProcessTransactionsSummary {
        // Process transactions
        let (mut process_transactions_summary, process_transactions_us) =
            measure_us!(self.process_transactions(bank_start, sanitized_transactions,));
        slot_metrics_tracker.increment_process_transactions_us(process_transactions_us);
        banking_stage_stats
            .transaction_processing_elapsed
            .fetch_add(process_transactions_us, Ordering::Relaxed);

        let ProcessTransactionsSummary {
            ref retryable_transaction_indexes,
            ref error_counters,
            ..
        } = process_transactions_summary;

        slot_metrics_tracker.accumulate_process_transactions_summary(&process_transactions_summary);
        slot_metrics_tracker.accumulate_transaction_errors(error_counters);

        let retryable_tx_count = retryable_transaction_indexes.len();
        inc_new_counter_info!("banking_stage-unprocessed_transactions", retryable_tx_count);

        // Filter out the retryable transactions that are too old
        let (filtered_retryable_transaction_indexes, filter_retryable_packets_us) =
            measure_us!(Self::filter_pending_packets_from_pending_txs(
                &bank_start.working_bank,
                sanitized_transactions,
                retryable_transaction_indexes,
            ));
        slot_metrics_tracker.increment_filter_retryable_packets_us(filter_retryable_packets_us);
        banking_stage_stats
            .filter_pending_packets_elapsed
            .fetch_add(filter_retryable_packets_us, Ordering::Relaxed);

        let retryable_packets_filtered_count = retryable_transaction_indexes
            .len()
            .saturating_sub(filtered_retryable_transaction_indexes.len());
        slot_metrics_tracker
            .increment_retryable_packets_filtered_count(retryable_packets_filtered_count as u64);

        inc_new_counter_info!(
            "banking_stage-dropped_tx_before_forwarding",
            retryable_transaction_indexes
                .len()
                .saturating_sub(filtered_retryable_transaction_indexes.len())
        );

        process_transactions_summary.retryable_transaction_indexes =
            filtered_retryable_transaction_indexes;
        process_transactions_summary
    }

    /// Sends transactions to the bank.
    ///
    /// Returns the number of transactions successfully processed by the bank, which may be less
    /// than the total number if max PoH height was reached and the bank halted
    fn process_transactions(
        &self,
        bank_start: &BankStart,
        transactions: &[SanitizedTransaction],
    ) -> ProcessTransactionsSummary {
        let mut chunk_start = 0;
        let mut all_retryable_tx_indexes = vec![];
        // All the transactions that attempted execution. See description of
        // struct ProcessTransactionsSummary above for possible outcomes.
        let mut total_transactions_attempted_execution_count: usize = 0;
        // All transactions that were executed and committed
        let mut total_committed_transactions_count: usize = 0;
        // All transactions that were executed and committed with a successful result
        let mut total_committed_transactions_with_successful_result_count: usize = 0;
        // All transactions that were executed but then failed record because the
        // slot ended
        let mut total_failed_commit_count: usize = 0;
        let mut total_cost_model_throttled_transactions_count: usize = 0;
        let mut total_cost_model_us: u64 = 0;
        let mut total_execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
        let mut total_error_counters = TransactionErrorMetrics::default();
        let mut reached_max_poh_height = false;
        while chunk_start != transactions.len() {
            let chunk_end = std::cmp::min(
                transactions.len(),
                chunk_start + MAX_NUM_TRANSACTIONS_PER_BATCH,
            );
            let process_transaction_batch_output = self.process_and_record_transactions(
                &bank_start.working_bank,
                &transactions[chunk_start..chunk_end],
                chunk_start,
            );

            let ProcessTransactionBatchOutput {
                cost_model_throttled_transactions_count: new_cost_model_throttled_transactions_count,
                cost_model_us: new_cost_model_us,
                execute_and_commit_transactions_output,
            } = process_transaction_batch_output;
            total_cost_model_throttled_transactions_count =
                total_cost_model_throttled_transactions_count
                    .saturating_add(new_cost_model_throttled_transactions_count);
            total_cost_model_us = total_cost_model_us.saturating_add(new_cost_model_us);

            let ExecuteAndCommitTransactionsOutput {
                transactions_attempted_execution_count: new_transactions_attempted_execution_count,
                executed_transactions_count: new_executed_transactions_count,
                executed_with_successful_result_count: new_executed_with_successful_result_count,
                retryable_transaction_indexes: new_retryable_transaction_indexes,
                commit_transactions_result: new_commit_transactions_result,
                execute_and_commit_timings: new_execute_and_commit_timings,
                error_counters: new_error_counters,
                ..
            } = execute_and_commit_transactions_output;

            total_execute_and_commit_timings.accumulate(&new_execute_and_commit_timings);
            total_error_counters.accumulate(&new_error_counters);
            total_transactions_attempted_execution_count =
                total_transactions_attempted_execution_count
                    .saturating_add(new_transactions_attempted_execution_count);

            trace!(
                "process_transactions result: {:?}",
                new_commit_transactions_result
            );

            if new_commit_transactions_result.is_ok() {
                total_committed_transactions_count = total_committed_transactions_count
                    .saturating_add(new_executed_transactions_count);
                total_committed_transactions_with_successful_result_count =
                    total_committed_transactions_with_successful_result_count
                        .saturating_add(new_executed_with_successful_result_count);
            } else {
                total_failed_commit_count =
                    total_failed_commit_count.saturating_add(new_executed_transactions_count);
            }

            // Add the retryable txs (transactions that errored in a way that warrants a retry)
            // to the list of unprocessed txs.
            all_retryable_tx_indexes.extend_from_slice(&new_retryable_transaction_indexes);

            // If `bank_creation_time` is None, it's a test so ignore the option so
            // allow processing
            let should_bank_still_be_processing_txs =
                bank_start.should_working_bank_still_be_processing_txs();
            match (
                new_commit_transactions_result,
                should_bank_still_be_processing_txs,
            ) {
                (Err(PohRecorderError::MaxHeightReached), _) | (_, false) => {
                    info!(
                        "process transactions: max height reached slot: {} height: {}",
                        bank_start.working_bank.slot(),
                        bank_start.working_bank.tick_height()
                    );
                    // process_and_record_transactions has returned all retryable errors in
                    // transactions[chunk_start..chunk_end], so we just need to push the remaining
                    // transactions into the unprocessed queue.
                    all_retryable_tx_indexes.extend(chunk_end..transactions.len());
                    reached_max_poh_height = true;
                    break;
                }
                _ => (),
            }
            // Don't exit early on any other type of error, continue processing...
            chunk_start = chunk_end;
        }

        ProcessTransactionsSummary {
            reached_max_poh_height,
            transactions_attempted_execution_count: total_transactions_attempted_execution_count,
            committed_transactions_count: total_committed_transactions_count,
            committed_transactions_with_successful_result_count:
                total_committed_transactions_with_successful_result_count,
            failed_commit_count: total_failed_commit_count,
            retryable_transaction_indexes: all_retryable_tx_indexes,
            cost_model_throttled_transactions_count: total_cost_model_throttled_transactions_count,
            cost_model_us: total_cost_model_us,
            execute_and_commit_timings: total_execute_and_commit_timings,
            error_counters: total_error_counters,
        }
    }

    pub fn process_and_record_transactions(
        &self,
        bank: &Arc<Bank>,
        txs: &[SanitizedTransaction],
        chunk_offset: usize,
    ) -> ProcessTransactionBatchOutput {
        let (
            (transaction_costs, transactions_qos_results, cost_model_throttled_transactions_count),
            cost_model_us,
        ) = measure_us!(self
            .qos_service
            .select_and_accumulate_transaction_costs(bank, txs));

        // Only lock accounts for those transactions are selected for the block;
        // Once accounts are locked, other threads cannot encode transactions that will modify the
        // same account state
        let (batch, lock_us) = measure_us!(
            bank.prepare_sanitized_batch_with_results(txs, transactions_qos_results.iter())
        );

        // retryable_txs includes AccountInUse, WouldExceedMaxBlockCostLimit
        // WouldExceedMaxAccountCostLimit, WouldExceedMaxVoteCostLimit
        // and WouldExceedMaxAccountDataCostLimit
        let mut execute_and_commit_transactions_output =
            self.execute_and_commit_transactions_locked(bank, &batch);

        // Once the accounts are new transactions can enter the pipeline to process them
        let (_, unlock_us) = measure_us!(drop(batch));

        let ExecuteAndCommitTransactionsOutput {
            ref mut retryable_transaction_indexes,
            ref execute_and_commit_timings,
            ref commit_transactions_result,
            ..
        } = execute_and_commit_transactions_output;

        QosService::update_or_remove_transaction_costs(
            transaction_costs.iter(),
            transactions_qos_results.iter(),
            commit_transactions_result.as_ref().ok(),
            bank,
        );

        retryable_transaction_indexes
            .iter_mut()
            .for_each(|x| *x += chunk_offset);

        let (cu, us) =
            Self::accumulate_execute_units_and_time(&execute_and_commit_timings.execute_timings);
        self.qos_service.accumulate_actual_execute_cu(cu);
        self.qos_service.accumulate_actual_execute_time(us);

        // reports qos service stats for this batch
        self.qos_service.report_metrics(bank.clone());

        debug!(
            "bank: {} lock: {}us unlock: {}us txs_len: {}",
            bank.slot(),
            lock_us,
            unlock_us,
            txs.len(),
        );

        ProcessTransactionBatchOutput {
            cost_model_throttled_transactions_count,
            cost_model_us,
            execute_and_commit_transactions_output,
        }
    }

    /// Execute, record, and commit transactions in batch to bank. Returns vector of whether txs are retryable.
    pub fn simplified_execution_chain(
        &self,
        bank_status: &BankStatus,
        batch: &TransactionBatch,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) -> Vec<bool> {
        let mut retryable = vec![false; batch.sanitized_transactions().len()];

        loop {
            let Some(bank) = bank_status.wait_for_bank() else { continue; };
            let Some(bank) = bank.upgrade() else { continue; };

            let mut pre_balance_info = PreBalanceInfo::default();
            let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();

            let LoadAndExecuteTransactionsOutput {
                mut loaded_transactions,
                execution_results,
                mut retryable_transaction_indexes,
                executed_transactions_count,
                executed_with_successful_result_count,
                signature_count,
                error_counters,
                ..
            } = bank.load_and_execute_transactions(
                batch,
                MAX_PROCESSING_AGE,
                false,
                false,
                false,
                &mut execute_and_commit_timings.execute_timings,
                None, // account_overrides
                None,
            );

            let executed_transactions = execution_results
                .iter()
                .zip(batch.sanitized_transactions())
                .filter_map(|(execution_result, tx)| {
                    if execution_result.was_executed() {
                        Some(tx.to_versioned_transaction())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            if !executed_transactions.is_empty() {
                let freeze_lock = bank.freeze_lock();

                let RecordTransactionsSummary {
                    result: record_transactions_result,
                    record_transactions_timings,
                    starting_transaction_index,
                } = self
                    .record_executor
                    .record_transactions(bank.slot(), executed_transactions);

                if record_transactions_result.is_ok() {
                    self.commit_executor.commit_transactions(
                        batch,
                        &mut loaded_transactions,
                        execution_results,
                        batch.sanitized_transactions(),
                        starting_transaction_index,
                        &bank,
                        &mut pre_balance_info,
                        &mut execute_and_commit_timings,
                        signature_count,
                        executed_transactions_count,
                        executed_with_successful_result_count,
                    );
                    slot_metrics_tracker
                        .increment_comitted_transactions_count(executed_transactions_count as u64);
                } else {
                    continue; // try to execute and record these transactions again on the next bank.
                }

                drop(freeze_lock);
            } else {
                return retryable; // None were executable - don't retry any
            }

            for retryable_transaction_index in retryable_transaction_indexes {
                retryable[retryable_transaction_index] = true;
            }

            return retryable;
        }
    }

    fn execute_and_commit_transactions_locked(
        &self,
        bank: &Arc<Bank>,
        batch: &TransactionBatch,
    ) -> ExecuteAndCommitTransactionsOutput {
        let has_status_sender = self.commit_executor.has_status_sender();
        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();

        let mut pre_balance_info = PreBalanceInfo::default();
        let (_, collect_balances_us) = measure_us!({
            // If the extra meta-data services are enabled for RPC, collect the
            // pre-balances for native and token programs.
            if has_status_sender {
                pre_balance_info.native = bank.collect_balances(batch);
                pre_balance_info.token =
                    collect_token_balances(bank, batch, &mut pre_balance_info.mint_decimals)
            }
        });
        execute_and_commit_timings.collect_balances_us = collect_balances_us;

        let (load_and_execute_transactions_output, load_execute_us) = measure_us!(bank
            .load_and_execute_transactions(
                batch,
                MAX_PROCESSING_AGE,
                has_status_sender,
                has_status_sender,
                has_status_sender,
                &mut execute_and_commit_timings.execute_timings,
                None, // account_overrides
                self.log_messages_bytes_limit
            ));
        execute_and_commit_timings.load_execute_us = load_execute_us;

        let LoadAndExecuteTransactionsOutput {
            mut loaded_transactions,
            execution_results,
            mut retryable_transaction_indexes,
            executed_transactions_count,
            executed_with_successful_result_count,
            signature_count,
            error_counters,
            ..
        } = load_and_execute_transactions_output;

        let transactions_attempted_execution_count = execution_results.len();
        let (executed_transactions, execution_results_to_transactions_us): (Vec<_>, u64) =
            measure_us!(execution_results
                .iter()
                .zip(batch.sanitized_transactions())
                .filter_map(|(execution_result, tx)| {
                    if execution_result.was_executed() {
                        Some(tx.to_versioned_transaction())
                    } else {
                        None
                    }
                })
                .collect());

        let (freeze_lock, freeze_lock_us) = measure_us!(bank.freeze_lock());
        execute_and_commit_timings.freeze_lock_us = freeze_lock_us;

        let (record_transactions_summary, record_us) = measure_us!(self
            .record_executor
            .record_transactions(bank.slot(), executed_transactions));
        execute_and_commit_timings.record_us = record_us;

        let RecordTransactionsSummary {
            result: record_transactions_result,
            record_transactions_timings,
            starting_transaction_index,
        } = record_transactions_summary;
        execute_and_commit_timings.record_transactions_timings = RecordTransactionsTimings {
            execution_results_to_transactions_us,
            ..record_transactions_timings
        };

        if let Err(recorder_err) = record_transactions_result {
            inc_new_counter_info!(
                "banking_stage-record_transactions_retryable_record_txs",
                executed_transactions_count
            );

            retryable_transaction_indexes.extend(execution_results.iter().enumerate().filter_map(
                |(index, execution_result)| execution_result.was_executed().then_some(index),
            ));

            return ExecuteAndCommitTransactionsOutput {
                transactions_attempted_execution_count,
                executed_transactions_count,
                executed_with_successful_result_count,
                retryable_transaction_indexes,
                commit_transactions_result: Err(recorder_err),
                execute_and_commit_timings,
                error_counters,
            };
        }

        let sanitized_txs = batch.sanitized_transactions();
        let (commit_time_us, commit_transaction_statuses) = if executed_transactions_count != 0 {
            self.commit_executor.commit_transactions(
                batch,
                &mut loaded_transactions,
                execution_results,
                sanitized_txs,
                starting_transaction_index,
                bank,
                &mut pre_balance_info,
                &mut execute_and_commit_timings,
                signature_count,
                executed_transactions_count,
                executed_with_successful_result_count,
            )
        } else {
            (
                0,
                vec![CommitTransactionDetails::NotCommitted; execution_results.len()],
            )
        };

        drop(freeze_lock);

        debug!(
            "bank: {} process_and_record_locked: {}us record: {}us commit: {}us txs_len: {}",
            bank.slot(),
            load_execute_us,
            record_us,
            commit_time_us,
            sanitized_txs.len(),
        );

        debug!(
            "execute_and_commit_transactions_locked: {:?}",
            execute_and_commit_timings.execute_timings,
        );

        debug_assert_eq!(
            commit_transaction_statuses.len(),
            transactions_attempted_execution_count
        );

        ExecuteAndCommitTransactionsOutput {
            transactions_attempted_execution_count,
            executed_transactions_count,
            executed_with_successful_result_count,
            retryable_transaction_indexes,
            commit_transactions_result: Ok(commit_transaction_statuses),
            execute_and_commit_timings,
            error_counters,
        }
    }

    fn accumulate_execute_units_and_time(execute_timings: &ExecuteTimings) -> (u64, u64) {
        let (units, times): (Vec<_>, Vec<_>) = execute_timings
            .details
            .per_program_timings
            .values()
            .map(|program_timings| {
                (
                    program_timings.accumulated_units,
                    program_timings.accumulated_us,
                )
            })
            .unzip();
        (units.iter().sum(), times.iter().sum())
    }

    /// This function filters pending packets that are still valid
    /// # Arguments
    /// * `transactions` - a batch of transactions deserialized from packets
    /// * `pending_indexes` - identifies which indexes in the `transactions` list are still pending
    fn filter_pending_packets_from_pending_txs(
        bank: &Arc<Bank>,
        transactions: &[SanitizedTransaction],
        pending_indexes: &[usize],
    ) -> Vec<usize> {
        let filter =
            Self::prepare_filter_for_pending_transactions(transactions.len(), pending_indexes);

        let results = bank.check_transactions_with_forwarding_delay(
            transactions.iter(),
            &filter,
            FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET,
        );

        Self::filter_valid_transaction_indexes(&results)
    }

    /// This function creates a filter of transaction results with Ok() for every pending
    /// transaction. The non-pending transactions are marked with TransactionError
    fn prepare_filter_for_pending_transactions(
        transactions_len: usize,
        pending_tx_indexes: &[usize],
    ) -> Vec<transaction::Result<()>> {
        let mut mask = vec![Err(TransactionError::BlockhashNotFound); transactions_len];
        pending_tx_indexes.iter().for_each(|x| mask[*x] = Ok(()));
        mask
    }

    /// This function returns a vector containing index of all valid transactions. A valid
    /// transaction has result Ok() as the value
    fn filter_valid_transaction_indexes(valid_txs: &[TransactionCheckResult]) -> Vec<usize> {
        valid_txs
            .iter()
            .enumerate()
            .filter_map(|(index, (x, _h))| if x.is_ok() { Some(index) } else { None })
            .collect_vec()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::banking_stage::tests::{
            create_slow_genesis_config, generate_new_address_lookup_table, sanitize_transactions,
            simulate_poh, store_address_lookup_table,
        },
        crossbeam_channel::unbounded,
        solana_entry::entry::{next_entry, next_versioned_entry},
        solana_ledger::{
            blockstore::{entries_to_test_shreds, Blockstore},
            blockstore_processor::TransactionStatusSender,
            genesis_utils::GenesisConfigInfo,
            get_tmp_ledger_path_auto_delete,
            leader_schedule_cache::LeaderScheduleCache,
        },
        solana_poh::poh_recorder::PohRecorder,
        solana_program_runtime::timings::ProgramTiming,
        solana_rpc::transaction_status_service::TransactionStatusService,
        solana_sdk::{
            instruction::InstructionError,
            message::{v0, v0::MessageAddressTableLookup, MessageHeader, VersionedMessage},
            poh_config::PohConfig,
            pubkey::Pubkey,
            signature::Keypair,
            signer::Signer,
            system_transaction,
            transaction::{MessageHash, Transaction, VersionedTransaction},
        },
        solana_transaction_status::{TransactionStatusMeta, VersionedTransactionWithStatusMeta},
        std::{
            sync::{
                atomic::{AtomicBool, AtomicU64},
                RwLock,
            },
            time::Instant,
        },
    };

    #[test]
    fn test_bank_prepare_filter_for_pending_transaction() {
        assert_eq!(
            ConsumeExecutor::prepare_filter_for_pending_transactions(6, &[2, 4, 5]),
            vec![
                Err(TransactionError::BlockhashNotFound),
                Err(TransactionError::BlockhashNotFound),
                Ok(()),
                Err(TransactionError::BlockhashNotFound),
                Ok(()),
                Ok(())
            ]
        );

        assert_eq!(
            ConsumeExecutor::prepare_filter_for_pending_transactions(6, &[0, 2, 3]),
            vec![
                Ok(()),
                Err(TransactionError::BlockhashNotFound),
                Ok(()),
                Ok(()),
                Err(TransactionError::BlockhashNotFound),
                Err(TransactionError::BlockhashNotFound),
            ]
        );
    }

    #[test]
    fn test_bank_filter_valid_transaction_indexes() {
        assert_eq!(
            ConsumeExecutor::filter_valid_transaction_indexes(&[
                (Err(TransactionError::BlockhashNotFound), None),
                (Err(TransactionError::BlockhashNotFound), None),
                (Ok(()), None),
                (Err(TransactionError::BlockhashNotFound), None),
                (Ok(()), None),
                (Ok(()), None),
            ]),
            [2, 4, 5]
        );

        assert_eq!(
            ConsumeExecutor::filter_valid_transaction_indexes(&[
                (Ok(()), None),
                (Err(TransactionError::BlockhashNotFound), None),
                (Err(TransactionError::BlockhashNotFound), None),
                (Ok(()), None),
                (Ok(()), None),
                (Ok(()), None),
            ]),
            [0, 3, 4, 5]
        );
    }

    #[test]
    fn test_bank_process_and_record_transactions() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(10_000);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
        let pubkey = solana_sdk::pubkey::new_rand();

        let transactions = sanitize_transactions(vec![system_transaction::transfer(
            &mint_keypair,
            &pubkey,
            1,
            genesis_config.hash(),
        )]);

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger");
            let (poh_recorder, entry_receiver, record_receiver) = PohRecorder::new(
                bank.tick_height(),
                bank.last_blockhash(),
                bank.clone(),
                Some((4, 4)),
                bank.ticks_per_slot(),
                &pubkey,
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            let record_executor = RecordExecutor::new(poh_recorder.recorder());
            let poh_recorder = Arc::new(RwLock::new(poh_recorder));

            let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

            poh_recorder.write().unwrap().set_bank(&bank, false);
            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();
            let commit_executor = CommitExecutor::new(None, gossip_vote_sender);
            let consume_executor =
                ConsumeExecutor::new(record_executor, commit_executor, QosService::new(1), None);

            let process_transactions_batch_output =
                consume_executor.process_and_record_transactions(&bank, &transactions, 0);

            let ExecuteAndCommitTransactionsOutput {
                transactions_attempted_execution_count,
                executed_transactions_count,
                executed_with_successful_result_count,
                commit_transactions_result,
                ..
            } = process_transactions_batch_output.execute_and_commit_transactions_output;

            assert_eq!(transactions_attempted_execution_count, 1);
            assert_eq!(executed_transactions_count, 1);
            assert_eq!(executed_with_successful_result_count, 1);
            assert!(commit_transactions_result.is_ok());

            // Tick up to max tick height
            while poh_recorder.read().unwrap().tick_height() != bank.max_tick_height() {
                poh_recorder.write().unwrap().tick();
            }

            let mut done = false;
            // read entries until I find mine, might be ticks...
            while let Ok((_bank, (entry, _tick_height))) = entry_receiver.recv() {
                if !entry.is_tick() {
                    trace!("got entry");
                    assert_eq!(entry.transactions.len(), transactions.len());
                    assert_eq!(bank.get_balance(&pubkey), 1);
                    done = true;
                }
                if done {
                    break;
                }
            }
            trace!("done ticking");

            assert!(done);

            let transactions = sanitize_transactions(vec![system_transaction::transfer(
                &mint_keypair,
                &pubkey,
                2,
                genesis_config.hash(),
            )]);

            let process_transactions_batch_output =
                consume_executor.process_and_record_transactions(&bank, &transactions, 0);

            let ExecuteAndCommitTransactionsOutput {
                transactions_attempted_execution_count,
                executed_transactions_count,
                executed_with_successful_result_count,
                retryable_transaction_indexes,
                commit_transactions_result,
                ..
            } = process_transactions_batch_output.execute_and_commit_transactions_output;
            assert_eq!(transactions_attempted_execution_count, 1);
            // Transactions was still executed, just wasn't committed, so should be counted here.
            assert_eq!(executed_transactions_count, 1);
            assert_eq!(executed_with_successful_result_count, 1);
            assert_eq!(retryable_transaction_indexes, vec![0]);
            assert_matches!(
                commit_transactions_result,
                Err(PohRecorderError::MaxHeightReached)
            );

            poh_recorder
                .read()
                .unwrap()
                .is_exited
                .store(true, Ordering::Relaxed);
            let _ = poh_simulator.join();

            assert_eq!(bank.get_balance(&pubkey), 1);
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }

    #[test]
    fn test_bank_process_and_record_transactions_all_unexecuted() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(10_000);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
        let pubkey = solana_sdk::pubkey::new_rand();

        let transactions = {
            let mut tx =
                system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_config.hash());
            // Add duplicate account key
            tx.message.account_keys.push(pubkey);
            sanitize_transactions(vec![tx])
        };

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger");
            let (poh_recorder, _entry_receiver, record_receiver) = PohRecorder::new(
                bank.tick_height(),
                bank.last_blockhash(),
                bank.clone(),
                Some((4, 4)),
                bank.ticks_per_slot(),
                &pubkey,
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            let record_executor = RecordExecutor::new(poh_recorder.recorder());
            let poh_recorder = Arc::new(RwLock::new(poh_recorder));

            let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

            poh_recorder.write().unwrap().set_bank(&bank, false);
            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();
            let commit_executor = CommitExecutor::new(None, gossip_vote_sender);
            let consume_executor =
                ConsumeExecutor::new(record_executor, commit_executor, QosService::new(1), None);

            let process_transactions_batch_output =
                consume_executor.process_and_record_transactions(&bank, &transactions, 0);

            let ExecuteAndCommitTransactionsOutput {
                transactions_attempted_execution_count,
                executed_transactions_count,
                executed_with_successful_result_count,
                commit_transactions_result,
                retryable_transaction_indexes,
                ..
            } = process_transactions_batch_output.execute_and_commit_transactions_output;

            assert_eq!(transactions_attempted_execution_count, 1);
            assert_eq!(executed_transactions_count, 0);
            assert_eq!(executed_with_successful_result_count, 0);
            assert!(retryable_transaction_indexes.is_empty());
            assert_eq!(
                commit_transactions_result.ok(),
                Some(vec![CommitTransactionDetails::NotCommitted; 1])
            );

            poh_recorder
                .read()
                .unwrap()
                .is_exited
                .store(true, Ordering::Relaxed);
            let _ = poh_simulator.join();
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }

    #[test]
    fn test_bank_process_and_record_transactions_cost_tracker() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(10_000);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
        let pubkey = solana_sdk::pubkey::new_rand();

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger");
            let (poh_recorder, _entry_receiver, record_receiver) = PohRecorder::new(
                bank.tick_height(),
                bank.last_blockhash(),
                bank.clone(),
                Some((4, 4)),
                bank.ticks_per_slot(),
                &pubkey,
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            let record_executor = RecordExecutor::new(poh_recorder.recorder());
            let poh_recorder = Arc::new(RwLock::new(poh_recorder));

            let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

            poh_recorder.write().unwrap().set_bank(&bank, false);
            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();
            let commit_executor = CommitExecutor::new(None, gossip_vote_sender);
            let consume_executor =
                ConsumeExecutor::new(record_executor, commit_executor, QosService::new(1), None);

            let get_block_cost = || bank.read_cost_tracker().unwrap().block_cost();
            let get_tx_count = || bank.read_cost_tracker().unwrap().transaction_count();
            assert_eq!(get_block_cost(), 0);
            assert_eq!(get_tx_count(), 0);

            //
            // TEST: cost tracker's block cost increases when successfully processing a tx
            //

            let transactions = sanitize_transactions(vec![system_transaction::transfer(
                &mint_keypair,
                &pubkey,
                1,
                genesis_config.hash(),
            )]);

            let process_transactions_batch_output =
                consume_executor.process_and_record_transactions(&bank, &transactions, 0);

            let ExecuteAndCommitTransactionsOutput {
                executed_with_successful_result_count,
                commit_transactions_result,
                ..
            } = process_transactions_batch_output.execute_and_commit_transactions_output;
            assert_eq!(executed_with_successful_result_count, 1);
            assert!(commit_transactions_result.is_ok());

            let single_transfer_cost = get_block_cost();
            assert_ne!(single_transfer_cost, 0);
            assert_eq!(get_tx_count(), 1);

            //
            // TEST: When a tx in a batch can't be executed (here because of account
            // locks), then its cost does not affect the cost tracker.
            //

            let allocate_keypair = Keypair::new();
            let transactions = sanitize_transactions(vec![
                system_transaction::transfer(&mint_keypair, &pubkey, 2, genesis_config.hash()),
                // intentionally use a tx that has a different cost
                system_transaction::allocate(
                    &mint_keypair,
                    &allocate_keypair,
                    genesis_config.hash(),
                    1,
                ),
            ]);

            let process_transactions_batch_output =
                consume_executor.process_and_record_transactions(&bank, &transactions, 0);

            let ExecuteAndCommitTransactionsOutput {
                executed_with_successful_result_count,
                commit_transactions_result,
                retryable_transaction_indexes,
                ..
            } = process_transactions_batch_output.execute_and_commit_transactions_output;
            assert_eq!(executed_with_successful_result_count, 1);
            assert!(commit_transactions_result.is_ok());
            assert_eq!(retryable_transaction_indexes, vec![1]);

            assert_eq!(get_block_cost(), 2 * single_transfer_cost);
            assert_eq!(get_tx_count(), 2);

            poh_recorder
                .read()
                .unwrap()
                .is_exited
                .store(true, Ordering::Relaxed);
            let _ = poh_simulator.join();
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }

    #[test]
    fn test_bank_process_and_record_transactions_account_in_use() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(10_000);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
        let pubkey = solana_sdk::pubkey::new_rand();
        let pubkey1 = solana_sdk::pubkey::new_rand();

        let transactions = sanitize_transactions(vec![
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_config.hash()),
            system_transaction::transfer(&mint_keypair, &pubkey1, 1, genesis_config.hash()),
        ]);

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger");
            let (poh_recorder, _entry_receiver, record_receiver) = PohRecorder::new(
                bank.tick_height(),
                bank.last_blockhash(),
                bank.clone(),
                Some((4, 4)),
                bank.ticks_per_slot(),
                &pubkey,
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            let record_executor = RecordExecutor::new(poh_recorder.recorder());
            let poh_recorder = Arc::new(RwLock::new(poh_recorder));

            poh_recorder.write().unwrap().set_bank(&bank, false);

            let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();
            let commit_executor = CommitExecutor::new(None, gossip_vote_sender);
            let consume_executor =
                ConsumeExecutor::new(record_executor, commit_executor, QosService::new(1), None);

            let process_transactions_batch_output =
                consume_executor.process_and_record_transactions(&bank, &transactions, 0);

            poh_recorder
                .read()
                .unwrap()
                .is_exited
                .store(true, Ordering::Relaxed);
            let _ = poh_simulator.join();

            let ExecuteAndCommitTransactionsOutput {
                transactions_attempted_execution_count,
                executed_transactions_count,
                retryable_transaction_indexes,
                commit_transactions_result,
                ..
            } = process_transactions_batch_output.execute_and_commit_transactions_output;

            assert_eq!(transactions_attempted_execution_count, 2);
            assert_eq!(executed_transactions_count, 1);
            assert_eq!(retryable_transaction_indexes, vec![1],);
            assert!(commit_transactions_result.is_ok());
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }

    #[test]
    fn test_process_transactions_returns_unprocessed_txs() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(10_000);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let pubkey = solana_sdk::pubkey::new_rand();

        let transactions = sanitize_transactions(vec![system_transaction::transfer(
            &mint_keypair,
            &pubkey,
            1,
            genesis_config.hash(),
        )]);

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger");
            let (poh_recorder, _entry_receiver, record_receiver) = PohRecorder::new(
                bank.tick_height(),
                bank.last_blockhash(),
                bank.clone(),
                Some((4, 4)),
                bank.ticks_per_slot(),
                &solana_sdk::pubkey::new_rand(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );

            // Poh Recorder has no working bank, so should throw MaxHeightReached error on
            // record
            let record_executor = RecordExecutor::new(poh_recorder.recorder());
            let poh_recorder = Arc::new(RwLock::new(poh_recorder));

            let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();
            let commit_executor = CommitExecutor::new(None, gossip_vote_sender);
            let consume_executor =
                ConsumeExecutor::new(record_executor, commit_executor, QosService::new(1), None);

            let process_transactions_summary = consume_executor.process_transactions(
                &BankStart {
                    working_bank: bank,
                    bank_creation_time: Arc::new(Instant::now()),
                },
                &transactions,
            );

            let ProcessTransactionsSummary {
                reached_max_poh_height,
                transactions_attempted_execution_count,
                committed_transactions_count,
                committed_transactions_with_successful_result_count,
                failed_commit_count,
                mut retryable_transaction_indexes,
                ..
            } = process_transactions_summary;
            assert!(reached_max_poh_height);
            assert_eq!(transactions_attempted_execution_count, 1);
            assert_eq!(failed_commit_count, 1);
            // MaxHeightReached error does not commit, should be zero here
            assert_eq!(committed_transactions_count, 0);
            assert_eq!(committed_transactions_with_successful_result_count, 0);

            retryable_transaction_indexes.sort_unstable();
            let expected: Vec<usize> = (0..transactions.len()).collect();
            assert_eq!(retryable_transaction_indexes, expected);

            poh_recorder
                .read()
                .unwrap()
                .is_exited
                .store(true, Ordering::Relaxed);
            let _ = poh_simulator.join();
        }

        Blockstore::destroy(ledger_path.path()).unwrap();
    }

    fn execute_transactions_with_dummy_poh_service(
        bank: Arc<Bank>,
        transactions: Vec<Transaction>,
    ) -> ProcessTransactionsSummary {
        let transactions = sanitize_transactions(transactions);
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let (poh_recorder, _entry_receiver, record_receiver) = PohRecorder::new(
            bank.tick_height(),
            bank.last_blockhash(),
            bank.clone(),
            Some((4, 4)),
            bank.ticks_per_slot(),
            &Pubkey::new_unique(),
            &Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            &Arc::new(PohConfig::default()),
            Arc::new(AtomicBool::default()),
        );
        let record_executor = RecordExecutor::new(poh_recorder.recorder());
        let poh_recorder = Arc::new(RwLock::new(poh_recorder));

        poh_recorder.write().unwrap().set_bank(&bank, false);

        let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

        let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();
        let commit_executor = CommitExecutor::new(None, gossip_vote_sender);
        let consume_executor =
            ConsumeExecutor::new(record_executor, commit_executor, QosService::new(1), None);
        let process_transactions_summary = consume_executor.process_transactions(
            &BankStart {
                working_bank: bank,
                bank_creation_time: Arc::new(Instant::now()),
            },
            &transactions,
        );

        poh_recorder
            .read()
            .unwrap()
            .is_exited
            .store(true, Ordering::Relaxed);
        let _ = poh_simulator.join();

        process_transactions_summary
    }

    #[test]
    fn test_process_transactions_instruction_error() {
        solana_logger::setup();
        let lamports = 10_000;
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(lamports);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
        // set cost tracker limits to MAX so it will not filter out TXs
        bank.write_cost_tracker()
            .unwrap()
            .set_limits(std::u64::MAX, std::u64::MAX, std::u64::MAX);

        // Transfer more than the balance of the mint keypair, should cause a
        // InstructionError::InsufficientFunds that is then committed. Needs to be
        // MAX_NUM_TRANSACTIONS_PER_BATCH at least so it doesn't conflict on account locks
        // with the below transaction
        let mut transactions = vec![
            system_transaction::transfer(
                &mint_keypair,
                &Pubkey::new_unique(),
                lamports + 1,
                genesis_config.hash(),
            );
            MAX_NUM_TRANSACTIONS_PER_BATCH
        ];

        // Make one transaction that will succeed.
        transactions.push(system_transaction::transfer(
            &mint_keypair,
            &Pubkey::new_unique(),
            1,
            genesis_config.hash(),
        ));

        let transactions_count = transactions.len();
        let ProcessTransactionsSummary {
            reached_max_poh_height,
            transactions_attempted_execution_count,
            committed_transactions_count,
            committed_transactions_with_successful_result_count,
            failed_commit_count,
            retryable_transaction_indexes,
            ..
        } = execute_transactions_with_dummy_poh_service(bank, transactions);

        // All the transactions should have been replayed, but only 1 committed
        assert!(!reached_max_poh_height);
        assert_eq!(transactions_attempted_execution_count, transactions_count);
        // Both transactions should have been committed, even though one was an error,
        // because InstructionErrors are committed
        assert_eq!(committed_transactions_count, 2);
        assert_eq!(committed_transactions_with_successful_result_count, 1);
        assert_eq!(failed_commit_count, 0);
        assert_eq!(
            retryable_transaction_indexes,
            (1..transactions_count - 1).collect::<Vec<usize>>()
        );
    }
    #[test]
    fn test_process_transactions_account_in_use() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(10_000);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
        // set cost tracker limits to MAX so it will not filter out TXs
        bank.write_cost_tracker()
            .unwrap()
            .set_limits(std::u64::MAX, std::u64::MAX, std::u64::MAX);

        // Make all repetitive transactions that conflict on the `mint_keypair`, so only 1 should be executed
        let mut transactions = vec![
            system_transaction::transfer(
                &mint_keypair,
                &Pubkey::new_unique(),
                1,
                genesis_config.hash()
            );
            MAX_NUM_TRANSACTIONS_PER_BATCH
        ];

        // Make one more in separate batch that also conflicts, but because it's in a separate batch, it
        // should be executed
        transactions.push(system_transaction::transfer(
            &mint_keypair,
            &Pubkey::new_unique(),
            1,
            genesis_config.hash(),
        ));

        let transactions_count = transactions.len();
        let ProcessTransactionsSummary {
            reached_max_poh_height,
            transactions_attempted_execution_count,
            committed_transactions_count,
            committed_transactions_with_successful_result_count,
            failed_commit_count,
            retryable_transaction_indexes,
            ..
        } = execute_transactions_with_dummy_poh_service(bank, transactions);

        // All the transactions should have been replayed, but only 2 committed (first and last)
        assert!(!reached_max_poh_height);
        assert_eq!(transactions_attempted_execution_count, transactions_count);
        assert_eq!(committed_transactions_count, 2);
        assert_eq!(committed_transactions_with_successful_result_count, 2);
        assert_eq!(failed_commit_count, 0,);

        // Everything except first and last index of the transactions failed and are last retryable
        assert_eq!(
            retryable_transaction_indexes,
            (1..transactions_count - 1).collect::<Vec<usize>>()
        );
    }

    #[test]
    fn test_write_persist_transaction_status() {
        solana_logger::setup();
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(solana_sdk::native_token::sol_to_lamports(1000.0));
        genesis_config.rent.lamports_per_byte_year = 50;
        genesis_config.rent.exemption_threshold = 2.0;
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
        let pubkey = solana_sdk::pubkey::new_rand();
        let pubkey1 = solana_sdk::pubkey::new_rand();
        let keypair1 = Keypair::new();

        let rent_exempt_amount = bank.get_minimum_balance_for_rent_exemption(0);

        let success_tx = system_transaction::transfer(
            &mint_keypair,
            &pubkey,
            rent_exempt_amount,
            genesis_config.hash(),
        );
        let success_signature = success_tx.signatures[0];
        let entry_1 = next_entry(&genesis_config.hash(), 1, vec![success_tx.clone()]);
        let ix_error_tx = system_transaction::transfer(
            &keypair1,
            &pubkey1,
            2 * rent_exempt_amount,
            genesis_config.hash(),
        );
        let ix_error_signature = ix_error_tx.signatures[0];
        let entry_2 = next_entry(&entry_1.hash, 1, vec![ix_error_tx.clone()]);
        let entries = vec![entry_1, entry_2];

        let transactions = sanitize_transactions(vec![success_tx, ix_error_tx]);
        bank.transfer(rent_exempt_amount, &mint_keypair, &keypair1.pubkey())
            .unwrap();

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger");
            let blockstore = Arc::new(blockstore);
            let (poh_recorder, _entry_receiver, record_receiver) = PohRecorder::new(
                bank.tick_height(),
                bank.last_blockhash(),
                bank.clone(),
                Some((4, 4)),
                bank.ticks_per_slot(),
                &pubkey,
                &blockstore,
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            let record_executor = RecordExecutor::new(poh_recorder.recorder());
            let poh_recorder = Arc::new(RwLock::new(poh_recorder));

            let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

            poh_recorder.write().unwrap().set_bank(&bank, false);

            let shreds = entries_to_test_shreds(
                &entries,
                bank.slot(),
                0,    // parent_slot
                true, // is_full_slot
                0,    // version
                true, // merkle_variant
            );
            blockstore.insert_shreds(shreds, None, false).unwrap();
            blockstore.set_roots(std::iter::once(&bank.slot())).unwrap();

            let (transaction_status_sender, transaction_status_receiver) = unbounded();
            let transaction_status_service = TransactionStatusService::new(
                transaction_status_receiver,
                Arc::new(AtomicU64::default()),
                true,
                None,
                blockstore.clone(),
                false,
                &Arc::new(AtomicBool::new(false)),
            );

            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();

            {
                // scope so sender on commit_executor is dropped
                let commit_executor = CommitExecutor::new(
                    Some(TransactionStatusSender {
                        sender: transaction_status_sender,
                    }),
                    gossip_vote_sender,
                );
                let consume_executor = ConsumeExecutor::new(
                    record_executor,
                    commit_executor,
                    QosService::new(1),
                    None,
                );

                let _ = consume_executor.process_and_record_transactions(&bank, &transactions, 0);
            }

            transaction_status_service.join().unwrap();

            let confirmed_block = blockstore.get_rooted_block(bank.slot(), false).unwrap();
            let actual_tx_results: Vec<_> = confirmed_block
                .transactions
                .into_iter()
                .map(|VersionedTransactionWithStatusMeta { transaction, meta }| {
                    (transaction.signatures[0], meta.status)
                })
                .collect();
            let expected_tx_results = vec![
                (success_signature, Ok(())),
                (
                    ix_error_signature,
                    Err(TransactionError::InstructionError(
                        0,
                        InstructionError::Custom(1),
                    )),
                ),
            ];
            assert_eq!(actual_tx_results, expected_tx_results);

            poh_recorder
                .read()
                .unwrap()
                .is_exited
                .store(true, Ordering::Relaxed);
            let _ = poh_simulator.join();
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }

    #[test]
    fn test_write_persist_loaded_addresses() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(10_000);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
        let keypair = Keypair::new();

        let address_table_key = Pubkey::new_unique();
        let address_table_state = generate_new_address_lookup_table(None, 2);
        store_address_lookup_table(&bank, address_table_key, address_table_state);

        let bank = Arc::new(Bank::new_from_parent(&bank, &Pubkey::new_unique(), 1));
        let message = VersionedMessage::V0(v0::Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            recent_blockhash: genesis_config.hash(),
            account_keys: vec![keypair.pubkey()],
            address_table_lookups: vec![MessageAddressTableLookup {
                account_key: address_table_key,
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
            instructions: vec![],
        });

        let tx = VersionedTransaction::try_new(message, &[&keypair]).unwrap();
        let sanitized_tx = SanitizedTransaction::try_create(
            tx.clone(),
            MessageHash::Compute,
            Some(false),
            bank.as_ref(),
            true, // require_static_program_ids
        )
        .unwrap();

        let entry = next_versioned_entry(&genesis_config.hash(), 1, vec![tx]);
        let entries = vec![entry];

        bank.transfer(1, &mint_keypair, &keypair.pubkey()).unwrap();

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger");
            let blockstore = Arc::new(blockstore);
            let (poh_recorder, _entry_receiver, record_receiver) = PohRecorder::new(
                bank.tick_height(),
                bank.last_blockhash(),
                bank.clone(),
                Some((4, 4)),
                bank.ticks_per_slot(),
                &Pubkey::new_unique(),
                &blockstore,
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            let record_executor = RecordExecutor::new(poh_recorder.recorder());
            let poh_recorder = Arc::new(RwLock::new(poh_recorder));

            let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

            poh_recorder.write().unwrap().set_bank(&bank, false);

            let shreds = entries_to_test_shreds(
                &entries,
                bank.slot(),
                0,    // parent_slot
                true, // is_full_slot
                0,    // version
                true, // merkle_variant
            );
            blockstore.insert_shreds(shreds, None, false).unwrap();
            blockstore.set_roots(std::iter::once(&bank.slot())).unwrap();

            let (transaction_status_sender, transaction_status_receiver) = unbounded();
            let transaction_status_service = TransactionStatusService::new(
                transaction_status_receiver,
                Arc::new(AtomicU64::default()),
                true,
                None,
                blockstore.clone(),
                false,
                &Arc::new(AtomicBool::new(false)),
            );

            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();

            {
                // scope so sender on commit_executor is dropped
                let commit_executor = CommitExecutor::new(
                    Some(TransactionStatusSender {
                        sender: transaction_status_sender,
                    }),
                    gossip_vote_sender,
                );
                let consume_executor = ConsumeExecutor::new(
                    record_executor,
                    commit_executor,
                    QosService::new(1),
                    None,
                );
                let _ = consume_executor.process_and_record_transactions(
                    &bank,
                    &[sanitized_tx.clone()],
                    0,
                );
            }

            transaction_status_service.join().unwrap();

            let mut confirmed_block = blockstore.get_rooted_block(bank.slot(), false).unwrap();
            assert_eq!(confirmed_block.transactions.len(), 1);

            let recorded_meta = confirmed_block.transactions.pop().unwrap().meta;
            assert_eq!(
                recorded_meta,
                TransactionStatusMeta {
                    status: Ok(()),
                    pre_balances: vec![1, 0, 0],
                    post_balances: vec![1, 0, 0],
                    pre_token_balances: Some(vec![]),
                    post_token_balances: Some(vec![]),
                    rewards: Some(vec![]),
                    loaded_addresses: sanitized_tx.get_loaded_addresses(),
                    compute_units_consumed: Some(0),
                    ..TransactionStatusMeta::default()
                }
            );
            poh_recorder
                .read()
                .unwrap()
                .is_exited
                .store(true, Ordering::Relaxed);
            let _ = poh_simulator.join();
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }

    #[test]
    fn test_accumulate_execute_units_and_time() {
        let mut execute_timings = ExecuteTimings::default();
        let mut expected_units = 0;
        let mut expected_us = 0;

        for n in 0..10 {
            execute_timings.details.per_program_timings.insert(
                Pubkey::new_unique(),
                ProgramTiming {
                    accumulated_us: n * 100,
                    accumulated_units: n * 1000,
                    count: n as u32,
                    errored_txs_compute_consumed: vec![],
                    total_errored_units: 0,
                },
            );
            expected_us += n * 100;
            expected_units += n * 1000;
        }

        let (units, us) = ConsumeExecutor::accumulate_execute_units_and_time(&execute_timings);

        assert_eq!(expected_units, units);
        assert_eq!(expected_us, us);
    }
}
