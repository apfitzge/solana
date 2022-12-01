use {
    crate::leader_slot_banking_stage_timing_metrics::RecordTransactionsTimings,
    solana_entry::entry::hash_transactions,
    solana_measure::measure,
    solana_poh::poh_recorder::{PohRecorderError, Slot, TransactionRecorder},
    solana_sdk::transaction::VersionedTransaction,
};

pub struct RecordTransactionsSummary {
    // Metrics describing how time was spent recording transactions
    pub record_transactions_timings: RecordTransactionsTimings,
    // Result of trying to record the transactions into the PoH stream
    pub result: Result<(), PohRecorderError>,
    // Index in the slot of the first transaction recorded
    pub starting_transaction_index: Option<usize>,
}

pub struct RecordExecutor;

impl RecordExecutor {
    pub fn record_transactions(
        bank_slot: Slot,
        transactions: Vec<VersionedTransaction>,
        recorder: &TransactionRecorder,
    ) -> RecordTransactionsSummary {
        let mut record_transactions_timings = RecordTransactionsTimings::default();
        let mut starting_transaction_index = None;

        if !transactions.is_empty() {
            let num_to_record = transactions.len();
            inc_new_counter_info!("banking_stage-record_count", 1);
            inc_new_counter_info!("banking_stage-record_transactions", num_to_record);

            let (hash, hash_time) = measure!(hash_transactions(&transactions), "hash");
            record_transactions_timings.hash_us = hash_time.as_us();

            let (res, poh_record_time) =
                measure!(recorder.record(bank_slot, hash, transactions), "hash");
            record_transactions_timings.poh_record_us = poh_record_time.as_us();

            match res {
                Ok(starting_index) => {
                    starting_transaction_index = starting_index;
                }
                Err(PohRecorderError::MaxHeightReached) => {
                    inc_new_counter_info!("banking_stage-max_height_reached", 1);
                    inc_new_counter_info!(
                        "banking_stage-max_height_reached_num_to_commit",
                        num_to_record
                    );
                    return RecordTransactionsSummary {
                        record_transactions_timings,
                        result: Err(PohRecorderError::MaxHeightReached),
                        starting_transaction_index: None,
                    };
                }
                Err(e) => panic!("Poh recorder returned unexpected error: {:?}", e),
            }
        }

        RecordTransactionsSummary {
            record_transactions_timings,
            result: Ok(()),
            starting_transaction_index,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::banking_stage::tests::simulate_poh,
        solana_ledger::{
            blockstore::Blockstore,
            genesis_utils::{create_genesis_config, GenesisConfigInfo},
            get_tmp_ledger_path_auto_delete,
            leader_schedule_cache::LeaderScheduleCache,
        },
        solana_poh::poh_recorder::PohRecorder,
        solana_runtime::bank::Bank,
        solana_sdk::{
            poh_config::PohConfig, pubkey::Pubkey, signature::Keypair, system_transaction,
        },
        std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
    };

    #[test]
    fn test_bank_record_transactions() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger");
            let (poh_recorder, entry_receiver, record_receiver) = PohRecorder::new(
                // TODO use record_receiver
                bank.tick_height(),
                bank.last_blockhash(),
                bank.clone(),
                None,
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            let recorder = poh_recorder.recorder();
            let poh_recorder = Arc::new(RwLock::new(poh_recorder));

            let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

            poh_recorder.write().unwrap().set_bank(&bank, false);
            let pubkey = solana_sdk::pubkey::new_rand();
            let keypair2 = Keypair::new();
            let pubkey2 = solana_sdk::pubkey::new_rand();

            let txs = vec![
                system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_config.hash())
                    .into(),
                system_transaction::transfer(&keypair2, &pubkey2, 1, genesis_config.hash()).into(),
            ];

            let _ = RecordExecutor::record_transactions(bank.slot(), txs.clone(), &recorder);
            let (_bank, (entry, _tick_height)) = entry_receiver.recv().unwrap();
            assert_eq!(entry.transactions, txs);

            // Once bank is set to a new bank (setting bank.slot() + 1 in record_transactions),
            // record_transactions should throw MaxHeightReached
            let next_slot = bank.slot() + 1;
            let RecordTransactionsSummary { result, .. } =
                RecordExecutor::record_transactions(next_slot, txs, &recorder);
            assert_matches!(result, Err(PohRecorderError::MaxHeightReached));
            // Should receive nothing from PohRecorder b/c record failed
            assert!(entry_receiver.try_recv().is_err());

            poh_recorder
                .read()
                .unwrap()
                .is_exited
                .store(true, Ordering::Relaxed);
            let _ = poh_simulator.join();
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }
}
