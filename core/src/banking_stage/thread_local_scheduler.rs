use {
    super::{
        consume_executor::ConsumeExecutor,
        decision_maker::{BufferedPacketsDecision, DecisionMaker},
        forward_executor::ForwardExecutor,
        packet_receiver::PacketReceiver,
        scheduler_error::SchedulerError,
        BankingStageStats, SLOT_BOUNDARY_CHECK_PERIOD,
    },
    crate::{
        leader_slot_banking_stage_metrics::LeaderSlotMetricsTracker,
        tracer_packet_stats::TracerPacketStats,
        unprocessed_transaction_storage::UnprocessedTransactionStorage,
    },
    crossbeam_channel::RecvTimeoutError,
    solana_measure::{measure, measure::Measure, measure_us},
    solana_poh::poh_recorder::BankStart,
    solana_sdk::timing::timestamp,
    std::{sync::atomic::Ordering, time::Instant},
};

/// Scheduler that lives in the same thread as executors. Handle is equivalent
/// to the scheduler itself.
pub struct ThreadLocalScheduler {
    decision_maker: DecisionMaker,
    unprocessed_transaction_storage: UnprocessedTransactionStorage,
    packet_receiver: PacketReceiver,
    last_metrics_update: Instant,
    banking_stage_stats: BankingStageStats,
}

impl ThreadLocalScheduler {
    pub fn new(
        id: u32,
        decision_maker: DecisionMaker,
        unprocessed_transaction_storage: UnprocessedTransactionStorage,
        packet_receiver: PacketReceiver,
    ) -> Self {
        Self {
            decision_maker,
            unprocessed_transaction_storage,
            packet_receiver,
            last_metrics_update: Instant::now(),
            banking_stage_stats: BankingStageStats::new(id),
        }
    }

    pub fn tick(&mut self) -> Result<(), SchedulerError> {
        let result = if matches!(
            self.packet_receiver
                .do_packet_receiving_and_buffering(&mut self.unprocessed_transaction_storage,),
            Err(RecvTimeoutError::Disconnected)
        ) {
            Err(SchedulerError::PacketReceiverDisconnected)
        } else {
            Ok(())
        };

        self.banking_stage_stats.report(1000);

        result
    }

    pub fn do_scheduled_work(
        &mut self,
        consume_executor: &ConsumeExecutor,
        forward_executor: &ForwardExecutor,
        tracer_packet_stats: &mut TracerPacketStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) {
        if !self.unprocessed_transaction_storage.is_empty()
            || self.last_metrics_update.elapsed() >= SLOT_BOUNDARY_CHECK_PERIOD
        {
            let (_, process_buffered_packets_us) = measure_us!(self.process_buffered_packets(
                consume_executor,
                forward_executor,
                tracer_packet_stats,
                slot_metrics_tracker,
            ));
            slot_metrics_tracker.increment_process_buffered_packets_us(process_buffered_packets_us);
            self.last_metrics_update = Instant::now();
        }
    }

    fn process_buffered_packets(
        &mut self,
        consume_executor: &ConsumeExecutor,
        forward_executor: &ForwardExecutor,
        tracer_packet_stats: &mut TracerPacketStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) {
        if self.unprocessed_transaction_storage.should_not_process() {
            return;
        }

        let (decision, make_decision_us) =
            measure_us!(self.decision_maker.make_consume_or_forward_decision());
        slot_metrics_tracker.increment_make_decision_us(make_decision_us);

        let leader_slot = match &decision {
            BufferedPacketsDecision::Consume(bank_start) => Some(bank_start.working_bank.slot()),
            _ => None,
        };
        self.packet_receiver.check_leader_slot_boundary(leader_slot);

        match decision {
            BufferedPacketsDecision::Consume(bank_start) => {
                // Take metrics action before consume packets (potentially resetting the
                // slot metrics tracker to the next slot) so that we don't count the
                // packet processing metrics from the next slot towards the metrics
                // of the previous slot
                slot_metrics_tracker.apply_working_bank(Some(&bank_start));
                let (_, consume_buffered_packets_us) = measure_us!(Self::consume_buffered_packets(
                    consume_executor,
                    &bank_start,
                    &mut self.unprocessed_transaction_storage,
                    None::<Box<dyn Fn()>>,
                    &self.banking_stage_stats,
                    slot_metrics_tracker
                ));
                slot_metrics_tracker
                    .increment_consume_buffered_packets_us(consume_buffered_packets_us);
            }
            BufferedPacketsDecision::Forward => {
                let (_, forward_us) = measure_us!(forward_executor.handle_forwarding(
                    &mut self.unprocessed_transaction_storage,
                    false,
                    slot_metrics_tracker,
                    &self.banking_stage_stats,
                    tracer_packet_stats,
                ));
                slot_metrics_tracker.increment_forward_us(forward_us);
                // Take metrics action after forwarding packets to include forwarded
                // metrics into current slot
                slot_metrics_tracker.apply_working_bank(None);
            }
            BufferedPacketsDecision::ForwardAndHold => {
                let (_, forward_and_hold_us) = measure_us!(forward_executor.handle_forwarding(
                    &mut self.unprocessed_transaction_storage,
                    true,
                    slot_metrics_tracker,
                    &self.banking_stage_stats,
                    tracer_packet_stats,
                ));
                slot_metrics_tracker.increment_forward_and_hold_us(forward_and_hold_us);
                // Take metrics action after forwarding packets
                slot_metrics_tracker.apply_working_bank(None);
            }
            _ => (),
        }
    }

    pub fn consume_buffered_packets(
        consume_executor: &ConsumeExecutor,
        bank_start: &BankStart,
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
        test_fn: Option<impl Fn()>,
        banking_stage_stats: &BankingStageStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) {
        let mut rebuffered_packet_count = 0;
        let mut consumed_buffered_packets_count = 0;
        let mut proc_start = Measure::start("consume_buffered_process");
        let num_packets_to_process = unprocessed_transaction_storage.len();

        let reached_end_of_slot = unprocessed_transaction_storage.process_packets(
            bank_start.working_bank.clone(),
            banking_stage_stats,
            slot_metrics_tracker,
            |packets_to_process, payload| {
                consume_executor.do_process_packets(
                    bank_start,
                    payload,
                    banking_stage_stats,
                    &mut consumed_buffered_packets_count,
                    &mut rebuffered_packet_count,
                    &test_fn,
                    packets_to_process,
                )
            },
        );

        if reached_end_of_slot {
            slot_metrics_tracker.set_end_of_slot_unprocessed_buffer_len(
                unprocessed_transaction_storage.len() as u64,
            );
        }

        proc_start.stop();
        debug!(
            "@{:?} done processing buffered batches: {} time: {:?}ms tx count: {} tx/s: {}",
            timestamp(),
            num_packets_to_process,
            proc_start.as_ms(),
            consumed_buffered_packets_count,
            (consumed_buffered_packets_count as f32) / (proc_start.as_s())
        );

        banking_stage_stats
            .consume_buffered_packets_elapsed
            .fetch_add(proc_start.as_us(), Ordering::Relaxed);
        banking_stage_stats
            .rebuffered_packets_count
            .fetch_add(rebuffered_packet_count, Ordering::Relaxed);
        banking_stage_stats
            .consumed_buffered_packets_count
            .fetch_add(consumed_buffered_packets_count, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            banking_stage::{
                commit_executor::CommitExecutor, record_executor::RecordExecutor,
                tests::setup_conflicting_transactions,
            },
            qos_service::QosService,
            unprocessed_packet_batches::{self, UnprocessedPacketBatches},
            unprocessed_transaction_storage::ThreadType,
        },
        crossbeam_channel::unbounded,
        solana_ledger::{blockstore::Blockstore, get_tmp_ledger_path_auto_delete},
        std::thread::Builder,
    };

    #[test]
    fn test_consume_buffered_packets() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let (transactions, bank, poh_recorder, _entry_receiver, poh_simulator) =
                setup_conflicting_transactions(ledger_path.path());
            let record_executor = RecordExecutor::new(poh_recorder.read().unwrap().recorder());
            let num_conflicting_transactions = transactions.len();
            let deserialized_packets =
                unprocessed_packet_batches::transactions_to_deserialized_packets(&transactions)
                    .unwrap();
            assert_eq!(deserialized_packets.len(), num_conflicting_transactions);
            let mut buffered_packet_batches =
                UnprocessedTransactionStorage::new_transaction_storage(
                    UnprocessedPacketBatches::from_iter(
                        deserialized_packets.into_iter(),
                        num_conflicting_transactions,
                    ),
                    ThreadType::Transactions,
                );

            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();
            let commit_executor = CommitExecutor::new(None, gossip_vote_sender);
            let consume_executor =
                ConsumeExecutor::new(record_executor, commit_executor, QosService::new(1), None);

            // When the working bank in poh_recorder is None, no packets should be processed (consume will not be called)
            assert!(!poh_recorder.read().unwrap().has_bank());
            assert_eq!(buffered_packet_batches.len(), num_conflicting_transactions);
            // When the working bank in poh_recorder is Some, all packets should be processed.
            // Multi-Iterator will process them 1-by-1 if all txs are conflicting.
            poh_recorder.write().unwrap().set_bank(&bank, false);
            let bank_start = poh_recorder.read().unwrap().bank_start().unwrap();
            ThreadLocalScheduler::consume_buffered_packets(
                &consume_executor,
                &bank_start,
                &mut buffered_packet_batches,
                None::<Box<dyn Fn()>>,
                &BankingStageStats::default(),
                &mut LeaderSlotMetricsTracker::new(0),
            );
            assert!(buffered_packet_batches.is_empty());
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
    fn test_consume_buffered_packets_sanitization_error() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let (mut transactions, bank, poh_recorder, _entry_receiver, poh_simulator) =
                setup_conflicting_transactions(ledger_path.path());
            let duplicate_account_key = transactions[0].message.account_keys[0];
            transactions[0]
                .message
                .account_keys
                .push(duplicate_account_key); // corrupt transaction
            let record_executor = RecordExecutor::new(poh_recorder.read().unwrap().recorder());
            let num_conflicting_transactions = transactions.len();
            let deserialized_packets =
                unprocessed_packet_batches::transactions_to_deserialized_packets(&transactions)
                    .unwrap();
            assert_eq!(deserialized_packets.len(), num_conflicting_transactions);
            let mut buffered_packet_batches =
                UnprocessedTransactionStorage::new_transaction_storage(
                    UnprocessedPacketBatches::from_iter(
                        deserialized_packets.into_iter(),
                        num_conflicting_transactions,
                    ),
                    ThreadType::Transactions,
                );

            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();
            let commit_executor = CommitExecutor::new(None, gossip_vote_sender);
            let consume_executor =
                ConsumeExecutor::new(record_executor, commit_executor, QosService::new(1), None);

            // When the working bank in poh_recorder is None, no packets should be processed
            assert!(!poh_recorder.read().unwrap().has_bank());
            assert_eq!(buffered_packet_batches.len(), num_conflicting_transactions);
            // When the working bank in poh_recorder is Some, all packets should be processed.
            // Multi-Iterator will process them 1-by-1 if all txs are conflicting.
            poh_recorder.write().unwrap().set_bank(&bank, false);
            let bank_start = poh_recorder.read().unwrap().bank_start().unwrap();
            ThreadLocalScheduler::consume_buffered_packets(
                &consume_executor,
                &bank_start,
                &mut buffered_packet_batches,
                None::<Box<dyn Fn()>>,
                &BankingStageStats::default(),
                &mut LeaderSlotMetricsTracker::new(0),
            );
            assert!(buffered_packet_batches.is_empty());
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
    fn test_consume_buffered_packets_interrupted() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let (continue_sender, continue_receiver) = unbounded();
            let (finished_packet_sender, finished_packet_receiver) = unbounded();
            let (transactions, bank, poh_recorder, _entry_receiver, poh_simulator) =
                setup_conflicting_transactions(ledger_path.path());

            let test_fn = Some(move || {
                finished_packet_sender.send(()).unwrap();
                continue_receiver.recv().unwrap();
            });
            // When the poh recorder has a bank, it should process all buffered packets.
            let num_conflicting_transactions = transactions.len();
            poh_recorder.write().unwrap().set_bank(&bank, false);
            let record_executor = RecordExecutor::new(poh_recorder.read().unwrap().recorder());
            let bank_start = poh_recorder.read().unwrap().bank_start().unwrap();
            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();
            let commit_executor = CommitExecutor::new(None, gossip_vote_sender);
            let consume_executor =
                ConsumeExecutor::new(record_executor, commit_executor, QosService::new(1), None);

            // Start up thread to process the banks
            let t_consume = Builder::new()
                .name("consume-buffered-packets".to_string())
                .spawn(move || {
                    let num_conflicting_transactions = transactions.len();
                    let deserialized_packets =
                        unprocessed_packet_batches::transactions_to_deserialized_packets(
                            &transactions,
                        )
                        .unwrap();
                    assert_eq!(deserialized_packets.len(), num_conflicting_transactions);
                    let mut buffered_packet_batches =
                        UnprocessedTransactionStorage::new_transaction_storage(
                            UnprocessedPacketBatches::from_iter(
                                deserialized_packets.into_iter(),
                                num_conflicting_transactions,
                            ),
                            ThreadType::Transactions,
                        );
                    ThreadLocalScheduler::consume_buffered_packets(
                        &consume_executor,
                        &bank_start,
                        &mut buffered_packet_batches,
                        test_fn,
                        &BankingStageStats::default(),
                        &mut LeaderSlotMetricsTracker::new(0),
                    );

                    // Check everything is correct. All valid packets should be processed.
                    assert!(buffered_packet_batches.is_empty());
                })
                .unwrap();

            // Should be calling `test_fn` for each non-conflicting batch.
            // In this case each batch is of size 1.
            for i in 0..num_conflicting_transactions {
                finished_packet_receiver.recv().unwrap();
                if i + 1 == num_conflicting_transactions {
                    poh_recorder
                        .read()
                        .unwrap()
                        .is_exited
                        .store(true, Ordering::Relaxed);
                }
                continue_sender.send(()).unwrap();
            }
            t_consume.join().unwrap();
            let _ = poh_simulator.join();
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }
}
