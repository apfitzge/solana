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
        forward_packet_batches_by_accounts::ForwardPacketBatchesByAccounts,
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        leader_slot_banking_stage_metrics::{LeaderSlotMetricsTracker, ProcessTransactionsSummary},
        tracer_packet_stats::TracerPacketStats,
        unprocessed_transaction_storage::{ConsumeScannerPayload, UnprocessedTransactionStorage},
    },
    crossbeam_channel::RecvTimeoutError,
    solana_measure::{measure, measure::Measure, measure_us},
    solana_poh::poh_recorder::BankStart,
    solana_runtime::bank_forks::BankForks,
    solana_sdk::timing::timestamp,
    std::{
        sync::{atomic::Ordering, Arc, RwLock},
        time::Instant,
    },
};

/// Scheduler that lives in the same thread as executors. Handle is equivalent
/// to the scheduler itself.
pub struct ThreadLocalScheduler {
    decision_maker: DecisionMaker,
    unprocessed_transaction_storage: UnprocessedTransactionStorage,
    bank_forks: Arc<RwLock<BankForks>>,
    packet_receiver: PacketReceiver,
    last_metrics_update: Instant,
    banking_stage_stats: BankingStageStats,
}

impl ThreadLocalScheduler {
    pub fn new(
        id: u32,
        decision_maker: DecisionMaker,
        unprocessed_transaction_storage: UnprocessedTransactionStorage,
        bank_forks: Arc<RwLock<BankForks>>,
        packet_receiver: PacketReceiver,
    ) -> Self {
        Self {
            decision_maker,
            unprocessed_transaction_storage,
            bank_forks,
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
                let (_, forward_us) = measure_us!(Self::handle_forwarding(
                    forward_executor,
                    &mut self.unprocessed_transaction_storage,
                    &self.bank_forks,
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
                let (_, forward_and_hold_us) = measure_us!(Self::handle_forwarding(
                    forward_executor,
                    &mut self.unprocessed_transaction_storage,
                    &self.bank_forks,
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
                Self::do_process_packets(
                    consume_executor,
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

    fn do_process_packets(
        consume_executor: &ConsumeExecutor,
        bank_start: &BankStart,
        payload: &mut ConsumeScannerPayload,
        banking_stage_stats: &BankingStageStats,
        consumed_buffered_packets_count: &mut usize,
        rebuffered_packet_count: &mut usize,
        test_fn: &Option<impl Fn()>,
        packets_to_process: &Vec<Arc<ImmutableDeserializedPacket>>,
    ) -> Option<Vec<usize>> {
        if payload.reached_end_of_slot {
            return None;
        }

        let packets_to_process_len = packets_to_process.len();
        let (process_transactions_summary, process_packets_transactions_us) =
            measure_us!(consume_executor.process_packets_transactions(
                bank_start,
                &payload.sanitized_transactions,
                banking_stage_stats,
                payload.slot_metrics_tracker,
            ));
        payload
            .slot_metrics_tracker
            .increment_process_packets_transactions_us(process_packets_transactions_us);

        // Clear payload for next iteration
        payload.sanitized_transactions.clear();
        payload.account_locks.clear();

        let ProcessTransactionsSummary {
            reached_max_poh_height,
            retryable_transaction_indexes,
            ..
        } = process_transactions_summary;

        if reached_max_poh_height || !bank_start.should_working_bank_still_be_processing_txs() {
            payload.reached_end_of_slot = true;
        }

        // The difference between all transactions passed to execution and the ones that
        // are retryable were the ones that were either:
        // 1) Committed into the block
        // 2) Dropped without being committed because they had some fatal error (too old,
        // duplicate signature, etc.)
        //
        // Note: This assumes that every packet deserializes into one transaction!
        *consumed_buffered_packets_count +=
            packets_to_process_len.saturating_sub(retryable_transaction_indexes.len());

        // Out of the buffered packets just retried, collect any still unprocessed
        // transactions in this batch for forwarding
        *rebuffered_packet_count += retryable_transaction_indexes.len();
        if let Some(test_fn) = test_fn {
            test_fn();
        }

        payload
            .slot_metrics_tracker
            .increment_retryable_packets_count(retryable_transaction_indexes.len() as u64);

        Some(retryable_transaction_indexes)
    }

    pub fn handle_forwarding(
        forward_executor: &ForwardExecutor,
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
        bank_forks: &RwLock<BankForks>,
        hold: bool,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
        banking_stage_stats: &BankingStageStats,
        tracer_packet_stats: &mut TracerPacketStats,
    ) {
        let forward_option = unprocessed_transaction_storage.forward_option();

        // get current root bank from bank_forks, use it to sanitize transaction and
        // load all accounts from address loader;
        let current_bank = bank_forks.read().unwrap().root_bank();

        let mut forward_packet_batches_by_accounts =
            ForwardPacketBatchesByAccounts::new_with_default_batch_limits();

        // sanitize and filter packets that are no longer valid (could be too old, a duplicate of something
        // already processed), then add to forwarding buffer.
        let filter_forwarding_result = unprocessed_transaction_storage
            .filter_forwardable_packets_and_add_batches(
                current_bank,
                &mut forward_packet_batches_by_accounts,
            );
        slot_metrics_tracker.increment_transactions_from_packets_us(
            filter_forwarding_result.total_packet_conversion_us,
        );
        banking_stage_stats.packet_conversion_elapsed.fetch_add(
            filter_forwarding_result.total_packet_conversion_us,
            Ordering::Relaxed,
        );
        banking_stage_stats
            .filter_pending_packets_elapsed
            .fetch_add(
                filter_forwarding_result.total_filter_packets_us,
                Ordering::Relaxed,
            );

        forward_packet_batches_by_accounts
            .iter_batches()
            .filter(|&batch| !batch.is_empty())
            .for_each(|forward_batch| {
                slot_metrics_tracker.increment_forwardable_batches_count(1);

                let batched_forwardable_packets_count = forward_batch.len();
                let (_forward_result, sucessful_forwarded_packets_count, leader_pubkey) =
                    forward_executor.forward_buffered_packets(
                        &forward_option,
                        forward_batch.get_forwardable_packets(),
                        banking_stage_stats,
                    );

                if let Some(leader_pubkey) = leader_pubkey {
                    tracer_packet_stats.increment_total_forwardable_tracer_packets(
                        filter_forwarding_result.total_forwardable_tracer_packets,
                        leader_pubkey,
                    );
                }
                let failed_forwarded_packets_count = batched_forwardable_packets_count
                    .saturating_sub(sucessful_forwarded_packets_count);

                if failed_forwarded_packets_count > 0 {
                    slot_metrics_tracker.increment_failed_forwarded_packets_count(
                        failed_forwarded_packets_count as u64,
                    );
                    slot_metrics_tracker.increment_packet_batch_forward_failure_count(1);
                }

                if sucessful_forwarded_packets_count > 0 {
                    slot_metrics_tracker.increment_successful_forwarded_packets_count(
                        sucessful_forwarded_packets_count as u64,
                    );
                }
            });

        if !hold {
            slot_metrics_tracker.increment_cleared_from_buffer_after_forward_count(
                filter_forwarding_result.total_forwardable_packets as u64,
            );
            tracer_packet_stats.increment_total_cleared_from_buffer_after_forward(
                filter_forwarding_result.total_tracer_packets_in_buffer,
            );
            unprocessed_transaction_storage.clear_forwarded_packets();
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            banking_stage::{
                commit_executor::CommitExecutor,
                record_executor::RecordExecutor,
                tests::{
                    create_slow_genesis_config, new_test_cluster_info,
                    setup_conflicting_transactions,
                },
            },
            qos_service::QosService,
            unprocessed_packet_batches::{self, DeserializedPacket, UnprocessedPacketBatches},
            unprocessed_transaction_storage::ThreadType,
        },
        crossbeam_channel::unbounded,
        solana_client::connection_cache::ConnectionCache,
        solana_gossip::cluster_info::Node,
        solana_ledger::{
            blockstore::Blockstore, genesis_utils::GenesisConfigInfo,
            get_tmp_ledger_path_auto_delete,
        },
        solana_perf::{
            data_budget::DataBudget,
            packet::{Packet, PacketFlags},
        },
        solana_poh::poh_recorder::create_test_recorder,
        solana_runtime::bank::Bank,
        solana_sdk::{
            hash::Hash, poh_config::PohConfig, signature::Keypair, system_transaction,
            transaction::VersionedTransaction,
        },
        solana_streamer::recvmmsg::recv_mmsg,
        std::{net::UdpSocket, thread::Builder},
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

    #[test]
    #[ignore]
    fn test_forwarder_budget() {
        solana_logger::setup();
        // Create `PacketBatch` with 1 unprocessed packet
        let tx = system_transaction::transfer(
            &Keypair::new(),
            &solana_sdk::pubkey::new_rand(),
            1,
            Hash::new_unique(),
        );
        let packet = Packet::from_data(None, tx).unwrap();
        let deserialized_packet = DeserializedPacket::new(packet).unwrap();

        let genesis_config_info = create_slow_genesis_config(10_000);
        let GenesisConfigInfo {
            genesis_config,
            validator_pubkey,
            ..
        } = &genesis_config_info;

        let bank = Bank::new_no_wallclock_throttle_for_tests(genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let bank = Arc::new(bank_forks.read().unwrap().get(0).unwrap());
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Arc::new(
                Blockstore::open(ledger_path.path())
                    .expect("Expected to be able to open database ledger"),
            );
            let poh_config = PohConfig {
                // limit tick count to avoid clearing working_bank at
                // PohRecord then PohRecorderError(MaxHeightReached) at BankingStage
                target_tick_count: Some(bank.max_tick_height() - 1),
                ..PohConfig::default()
            };

            let (exit, poh_recorder, poh_service, _entry_receiver) =
                create_test_recorder(&bank, &blockstore, Some(poh_config), None);

            let local_node = Node::new_localhost_with_pubkey(validator_pubkey);
            let cluster_info = Arc::new(new_test_cluster_info(local_node.info));
            let recv_socket = &local_node.sockets.tpu_forwards[0];

            let test_cases = vec![
                ("budget-restricted", DataBudget::restricted(), 0),
                ("budget-available", DataBudget::default(), 1),
            ];
            for (name, data_budget, expected_num_forwarded) in test_cases {
                let forward_executor = ForwardExecutor::new(
                    poh_recorder.clone(),
                    UdpSocket::bind("0.0.0.0:0").unwrap(),
                    cluster_info.clone(),
                    Arc::new(ConnectionCache::default()),
                    Arc::new(data_budget),
                );
                let unprocessed_packet_batches: UnprocessedPacketBatches =
                    UnprocessedPacketBatches::from_iter(
                        vec![deserialized_packet.clone()].into_iter(),
                        1,
                    );
                let stats = BankingStageStats::default();
                ThreadLocalScheduler::handle_forwarding(
                    &forward_executor,
                    &mut UnprocessedTransactionStorage::new_transaction_storage(
                        unprocessed_packet_batches,
                        ThreadType::Transactions,
                    ),
                    &bank_forks,
                    true,
                    &mut LeaderSlotMetricsTracker::new(0),
                    &stats,
                    &mut TracerPacketStats::new(0),
                );

                recv_socket
                    .set_nonblocking(expected_num_forwarded == 0)
                    .unwrap();

                let mut packets = vec![Packet::default(); 2];
                let num_received = recv_mmsg(recv_socket, &mut packets[..]).unwrap_or_default();
                assert_eq!(num_received, expected_num_forwarded, "{}", name);
            }

            exit.store(true, Ordering::Relaxed);
            poh_service.join().unwrap();
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }

    #[test]
    #[ignore]
    fn test_handle_forwarding() {
        solana_logger::setup();
        // packets are deserialized upon receiving, failed packets will not be
        // forwarded; Therefore need to create real packets here.
        let keypair = Keypair::new();
        let pubkey = solana_sdk::pubkey::new_rand();

        let fwd_block_hash = Hash::new_unique();
        let forwarded_packet = {
            let transaction = system_transaction::transfer(&keypair, &pubkey, 1, fwd_block_hash);
            let mut packet = Packet::from_data(None, transaction).unwrap();
            packet.meta.flags |= PacketFlags::FORWARDED;
            DeserializedPacket::new(packet).unwrap()
        };

        let normal_block_hash = Hash::new_unique();
        let normal_packet = {
            let transaction = system_transaction::transfer(&keypair, &pubkey, 1, normal_block_hash);
            let packet = Packet::from_data(None, transaction).unwrap();
            DeserializedPacket::new(packet).unwrap()
        };

        let mut unprocessed_packet_batches = UnprocessedTransactionStorage::new_transaction_storage(
            UnprocessedPacketBatches::from_iter(
                vec![forwarded_packet, normal_packet].into_iter(),
                2,
            ),
            ThreadType::Transactions,
        );

        let genesis_config_info = create_slow_genesis_config(10_000);
        let GenesisConfigInfo {
            genesis_config,
            validator_pubkey,
            ..
        } = &genesis_config_info;
        let bank = Bank::new_no_wallclock_throttle_for_tests(genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let bank = Arc::new(bank_forks.read().unwrap().get(0).unwrap());
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Arc::new(
                Blockstore::open(ledger_path.path())
                    .expect("Expected to be able to open database ledger"),
            );
            let poh_config = PohConfig {
                // limit tick count to avoid clearing working_bank at
                // PohRecord then PohRecorderError(MaxHeightReached) at BankingStage
                target_tick_count: Some(bank.max_tick_height() - 1),
                ..PohConfig::default()
            };

            let (exit, poh_recorder, poh_service, _entry_receiver) =
                create_test_recorder(&bank, &blockstore, Some(poh_config), None);

            let local_node = Node::new_localhost_with_pubkey(validator_pubkey);
            let cluster_info = new_test_cluster_info(local_node.info);
            let recv_socket = &local_node.sockets.tpu_forwards[0];
            let connection_cache = ConnectionCache::default();

            let test_cases = vec![
                ("fwd-normal", true, vec![normal_block_hash], 2),
                ("fwd-no-op", true, vec![], 2),
                ("fwd-no-hold", false, vec![], 0),
            ];

            let forward_executor = ForwardExecutor::new(
                poh_recorder,
                UdpSocket::bind("0.0.0.0:0").unwrap(),
                Arc::new(cluster_info),
                Arc::new(connection_cache),
                Arc::new(DataBudget::default()),
            );
            for (name, hold, expected_ids, expected_num_unprocessed) in test_cases {
                let stats = BankingStageStats::default();
                ThreadLocalScheduler::handle_forwarding(
                    &forward_executor,
                    &mut unprocessed_packet_batches,
                    &bank_forks,
                    hold,
                    &mut LeaderSlotMetricsTracker::new(0),
                    &stats,
                    &mut TracerPacketStats::new(0),
                );

                recv_socket
                    .set_nonblocking(expected_ids.is_empty())
                    .unwrap();

                let mut packets = vec![Packet::default(); 2];
                let num_received = recv_mmsg(recv_socket, &mut packets[..]).unwrap_or_default();
                assert_eq!(num_received, expected_ids.len(), "{}", name);
                for (i, expected_id) in expected_ids.iter().enumerate() {
                    assert_eq!(packets[i].meta.size, 215);
                    let recv_transaction: VersionedTransaction =
                        packets[i].deserialize_slice(..).unwrap();
                    assert_eq!(
                        recv_transaction.message.recent_blockhash(),
                        expected_id,
                        "{}",
                        name
                    );
                }

                let num_unprocessed_packets: usize = unprocessed_packet_batches.len();
                assert_eq!(
                    num_unprocessed_packets, expected_num_unprocessed,
                    "{}",
                    name
                );
            }

            exit.store(true, Ordering::Relaxed);
            poh_service.join().unwrap();
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }
}
