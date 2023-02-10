use {
    super::{
        commit_executor::CommitExecutor,
        record_executor::{RecordExecutor, RecordTransactionsSummary},
        thread_aware_account_locks::ThreadId,
        CommitTransactionDetails,
    },
    crate::{
        scheduler_stage::{ProcessedTransactions, ScheduledTransactions},
        unprocessed_packet_batches::DeserializedPacket,
    },
    crossbeam_channel::{select, unbounded, Receiver, Sender},
    solana_poh::poh_recorder::Slot,
    solana_program_runtime::timings::ExecuteTimings,
    solana_runtime::{
        accounts::TransactionLoadResult,
        bank::{
            Bank, CommitTransactionCounts, LoadAndExecuteTransactionsOutput,
            TransactionExecutionResult,
        },
        bank_status::BankStatus,
        bank_utils,
        transaction_batch::TransactionBatch,
        vote_sender_types::ReplayVoteSender,
    },
    solana_sdk::{clock::MAX_PROCESSING_AGE, transaction::SanitizedTransaction},
    std::{
        borrow::Cow,
        collections::{hash_map::Entry, HashMap},
        sync::Arc,
        thread::JoinHandle,
    },
};

pub type LoadAndExecuteTransactionsSubstageInput = ScheduledTransactions;

pub struct LoadAndExecuteTransactionsSubstageOutput {
    pub thread_id: ThreadId,
    pub transactions: Vec<SanitizedTransaction>,
    pub packets: Vec<DeserializedPacket>,
    pub retryable: Vec<bool>,
    pub bank: Arc<Bank>,
    pub loaded_transactions: Vec<TransactionLoadResult>,
    pub execution_results: Vec<TransactionExecutionResult>,
    pub executed_transactions_count: usize,
    pub executed_with_successful_result_count: usize,
    pub signature_count: u64,
}

pub type RecordTransactionsSubstageInput = LoadAndExecuteTransactionsSubstageOutput;
pub struct RecordTransactionsSubstageOutput {
    pub thread_id: ThreadId,
    pub transactions: Vec<SanitizedTransaction>,
    pub packets: Vec<DeserializedPacket>,
    pub retryable: Vec<bool>,
    pub bank: Arc<Bank>,
    pub loaded_transactions: Vec<TransactionLoadResult>,
    pub execution_results: Vec<TransactionExecutionResult>,
    pub executed_transactions_count: usize,
    pub executed_with_successful_result_count: usize,
    pub signature_count: u64,
    pub record_successful: bool,
}

pub type CommitTransactionsSubstageInput = RecordTransactionsSubstageOutput;
pub type CommitTransactionsSubstageOutput = ProcessedTransactions;

pub type ExecutionPipelineInput = LoadAndExecuteTransactionsSubstageInput;
pub type ExecutionPipelineOutput = CommitTransactionsSubstageOutput;

pub struct ExecutionPipeline {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl ExecutionPipeline {
    pub fn run(
        id: u32,
        input_receiver: Receiver<ExecutionPipelineInput>,
        output_sender: Sender<ExecutionPipelineOutput>,
        bank_status: Arc<BankStatus>,
        record_executor: RecordExecutor,
        gossip_vote_sender: ReplayVoteSender,
    ) -> Self {
        let (bank_sender, bank_receiver) = unbounded();
        let (slot_sender, slot_receiver) = unbounded();
        let (load_output_sender, record_input_receiver) = unbounded();
        let (record_output_sender, commit_input_receiver) = unbounded();

        let holding_thread_hdl = Self::run_locking_holder(id, bank_receiver, slot_receiver);
        let load_and_execute_thread_hdl = Self::run_load_and_execute_thread(
            id,
            input_receiver,
            bank_sender,
            load_output_sender,
            bank_status,
        );
        let record_thread_hdl = Self::run_transaction_record_thread(
            id,
            record_input_receiver,
            record_output_sender,
            record_executor,
        );
        let commit_thread_hdl = Self::run_transaction_commit_thread(
            id,
            commit_input_receiver,
            slot_sender,
            output_sender,
            gossip_vote_sender,
        );

        Self {
            thread_hdls: vec![
                holding_thread_hdl,
                load_and_execute_thread_hdl,
                record_thread_hdl,
                commit_thread_hdl,
            ],
        }
    }

    pub fn take(self) -> Vec<JoinHandle<()>> {
        self.thread_hdls
    }

    /// Spawns thread that is responsible for holding freeze lock.
    fn run_locking_holder(
        id: u32,
        bank_receiver: Receiver<Arc<Bank>>,
        slot_receiver: Receiver<Slot>,
    ) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name(format!("solLH{id:02}"))
            .spawn(move || {
                while let Ok(bank) = bank_receiver.recv() {
                    let held_bank = bank;
                    let lock = held_bank.freeze_lock();
                    let mut num_held = 1;
                    loop {
                        select! {
                            recv(bank_receiver) -> bank => {
                                if let Ok(bank) = bank {
                                    if bank.slot() == held_bank.slot() {
                                        num_held += 1;
                                    } else {
                                        panic!("Bank mismatch");
                                    }
                                }
                            }
                            recv(slot_receiver) -> slot => {
                                if let Ok(slot) = slot {
                                    if slot == held_bank.slot() {
                                        num_held -= 1;
                                        if num_held == 0 {
                                            break;
                                        }
                                    } else {
                                        panic!("Slot mismatch");
                                    }
                                }
                            }
                        }
                    }
                    drop(lock);
                }
            })
            .unwrap()
    }

    /// Spawns thread to load and execute transactions
    fn run_load_and_execute_thread(
        id: u32,
        receiver: Receiver<LoadAndExecuteTransactionsSubstageInput>,
        bank_sender: Sender<Arc<Bank>>,
        sender: Sender<LoadAndExecuteTransactionsSubstageOutput>,
        bank_status: Arc<BankStatus>,
    ) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name(format!("solLAET{id:02}"))
            .spawn(move || {
                while let Ok(LoadAndExecuteTransactionsSubstageInput {
                    thread_id,
                    transactions,
                    packets,
                    decision: _,
                    validity_check: _,
                }) = receiver.recv()
                {
                    // Wait for a valid bank
                    let bank = loop {
                        let Some(bank) = bank_status.wait_for_bank() else { continue; };
                        let Some(bank) = bank.upgrade() else { continue; };
                        break bank;
                    };

                    // Send to the holding thread (grabs freeze lock)
                    bank_sender.send(bank.clone()).unwrap();

                    // Construct batch
                    let batch = TransactionBatch {
                        lock_results: vec![Ok(()); transactions.len()],
                        bank: &bank,
                        sanitized_txs: Cow::Borrowed(&transactions),
                        needs_unlock: false,
                    };

                    // Load and execute transactions
                    let mut timings = Default::default();
                    let LoadAndExecuteTransactionsOutput {
                        loaded_transactions,
                        execution_results,
                        retryable_transaction_indexes,
                        executed_transactions_count,
                        executed_with_successful_result_count,
                        signature_count,
                        error_counters,
                    } = bank.load_and_execute_transactions(
                        &batch,
                        MAX_PROCESSING_AGE,
                        false,
                        false,
                        false,
                        &mut timings,
                        None,
                        None,
                    );
                    drop(batch);

                    let mut retryable = vec![false; transactions.len()];
                    for index in retryable_transaction_indexes {
                        retryable[index] = true;
                    }

                    sender
                        .send(LoadAndExecuteTransactionsSubstageOutput {
                            thread_id,
                            transactions,
                            packets,
                            retryable,
                            bank,
                            loaded_transactions,
                            execution_results,
                            executed_transactions_count,
                            executed_with_successful_result_count,
                            signature_count,
                        })
                        .unwrap();
                }
            })
            .unwrap()
    }

    /// Spawns thread to record transactions
    fn run_transaction_record_thread(
        id: u32,
        receiver: Receiver<RecordTransactionsSubstageInput>,
        sender: Sender<RecordTransactionsSubstageOutput>,
        recorder: RecordExecutor,
    ) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name(format!("solTRT{id:02}"))
            .spawn(move || {
                while let Ok(RecordTransactionsSubstageInput {
                    thread_id,
                    transactions,
                    packets,
                    mut retryable,
                    bank,
                    loaded_transactions,
                    execution_results,
                    executed_transactions_count,
                    executed_with_successful_result_count,
                    signature_count,
                }) = receiver.recv()
                {
                    let mut record_successful = false;
                    let (executed_transactions_indexes, executed_transactions): (Vec<_>, Vec<_>) =
                        execution_results
                            .iter()
                            .zip(&transactions)
                            .enumerate()
                            .filter_map(|(idx, (execution_result, tx))| {
                                execution_result
                                    .was_executed()
                                    .then(|| (idx, tx.to_versioned_transaction()))
                            })
                            .unzip();

                    if !executed_transactions.is_empty() {
                        let RecordTransactionsSummary {
                            record_transactions_timings,
                            result,
                            starting_transaction_index,
                        } = recorder.record_transactions(bank.slot(), executed_transactions);

                        if result.is_err() {
                            executed_transactions_indexes.into_iter().for_each(|idx| {
                                retryable[idx] = true;
                            });
                        } else {
                            record_successful = true;
                        }
                    }

                    sender
                        .send(RecordTransactionsSubstageOutput {
                            thread_id,
                            transactions,
                            packets,
                            retryable,
                            bank,
                            loaded_transactions,
                            execution_results,
                            executed_transactions_count,
                            executed_with_successful_result_count,
                            signature_count,
                            record_successful,
                        })
                        .unwrap();
                }
            })
            .unwrap()
    }

    fn run_transaction_commit_thread(
        id: u32,
        receiver: Receiver<CommitTransactionsSubstageInput>,
        slot_sender: Sender<Slot>,
        sender: Sender<CommitTransactionsSubstageOutput>,
        gossip_vote_sender: ReplayVoteSender,
    ) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name(format!("solTCT{id:02}"))
            .spawn(move || {
                while let Ok(CommitTransactionsSubstageInput {
                    thread_id,
                    transactions,
                    packets,
                    retryable,
                    bank,
                    mut loaded_transactions,
                    execution_results,
                    executed_transactions_count,
                    executed_with_successful_result_count,
                    signature_count,
                    record_successful,
                }) = receiver.recv()
                {
                    if record_successful {
                        let (last_blockhash, lamports_per_signature) =
                            bank.last_blockhash_and_lamports_per_signature();

                        let mut execute_timings = ExecuteTimings::default();
                        let tx_results = bank.commit_transactions(
                            &transactions,
                            &mut loaded_transactions,
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
                            &mut execute_timings,
                        );

                        bank_utils::find_and_send_votes(
                            &transactions,
                            &tx_results,
                            Some(&gossip_vote_sender),
                        );

                        // TODO: Status sender
                    }

                    // Send to the holding thread (drops freeze lock if all are done)
                    slot_sender.send(bank.slot()).unwrap();

                    sender
                        .send(CommitTransactionsSubstageOutput {
                            thread_id,
                            transactions,
                            packets,
                            retryable,
                            invalidated: false,
                        })
                        .unwrap();
                }
            })
            .unwrap()
    }
}
