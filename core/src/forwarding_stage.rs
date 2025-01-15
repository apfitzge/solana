use {
    agave_banking_stage_ingress_types::BankingPacketBatch,
    agave_transaction_view::transaction_view::SanitizedTransactionView,
    crossbeam_channel::{Receiver, RecvTimeoutError},
    min_max_heap::MinMaxHeap,
    slab::Slab,
    solana_client::connection_cache::ConnectionCache,
    solana_cost_model::cost_model::CostModel,
    solana_net_utils::bind_to_unspecified,
    solana_perf::{data_budget::DataBudget, packet::Packet},
    solana_runtime::{bank::Bank, root_bank_cache::RootBankCache},
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
    },
    solana_sdk::fee::FeeBudgetLimits,
    std::{
        net::UdpSocket,
        sync::Arc,
        thread::{Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

pub struct ForwardingStage {
    receiver: Receiver<(BankingPacketBatch, bool)>,
    packet_container: PacketContainer,

    root_bank_cache: RootBankCache,
    connection_cache: Arc<ConnectionCache>,
    data_budget: DataBudget,
    udp_socket: UdpSocket,
}

impl ForwardingStage {
    pub fn spawn(
        receiver: Receiver<(BankingPacketBatch, bool)>,
        connection_cache: Arc<ConnectionCache>,
        root_bank_cache: RootBankCache,
    ) -> JoinHandle<()> {
        let forwarding_stage = Self::new(receiver, connection_cache, root_bank_cache);
        Builder::new()
            .name("solFwdStage".to_string())
            .spawn(move || forwarding_stage.run())
            .unwrap()
    }

    fn new(
        receiver: Receiver<(BankingPacketBatch, bool)>,
        connection_cache: Arc<ConnectionCache>,
        root_bank_cache: RootBankCache,
    ) -> Self {
        Self {
            receiver,
            packet_container: PacketContainer::with_capacity(4 * 4096),
            root_bank_cache,
            connection_cache,
            data_budget: DataBudget::default(),
            udp_socket: bind_to_unspecified().unwrap(),
        }
    }

    fn run(mut self) {
        loop {
            let root_bank = self.root_bank_cache.root_bank();
            if !self.receive_and_buffer(&root_bank) {
                break;
            }
            self.forward_buffered_packets();
        }
    }

    fn receive_and_buffer(&mut self, bank: &Bank) -> bool {
        let now = Instant::now();
        const TIMEOUT: Duration = Duration::from_millis(10);
        match self.receiver.recv_timeout(TIMEOUT) {
            Ok((packet_batches, _tpu_vote_batch)) => {
                self.buffer_packet_batches(packet_batches, bank);

                // Drain the channel up to timeout
                let timed_out = loop {
                    if now.elapsed() >= TIMEOUT {
                        break true;
                    }
                    match self.receiver.try_recv() {
                        Ok((packet_batches, _tpu_vote_batch)) => {
                            self.buffer_packet_batches(packet_batches, bank)
                        }
                        Err(_) => break false,
                    }
                };

                // If timeout waas reached, prevent backup by draining all
                // packets in the channel.
                if timed_out {
                    warn!("ForwardingStage is backed up, dropping packets");
                    while self.receiver.try_recv().is_ok() {}
                }

                true
            }
            Err(RecvTimeoutError::Timeout) => true,
            Err(RecvTimeoutError::Disconnected) => false,
        }
    }

    fn buffer_packet_batches(&mut self, packet_batches: BankingPacketBatch, bank: &Bank) {
        for batch in packet_batches.iter() {
            for packet in batch.iter().filter(|p| Self::initial_packet_meta_filter(p)) {
                let Some(packet_data) = packet.data(..) else {
                    // should never occur since we've already checked the
                    // packet is not marked for discard.
                    continue;
                };

                // Parse the transaction, make sure it passes basic sanitization checks.
                let Ok(transaction) = SanitizedTransactionView::try_new_sanitized(packet_data)
                else {
                    continue;
                };

                // Calculate static metadata for the transaction so that we
                // are able to calculate fees for prioritization.
                let Ok(transaction) = RuntimeTransaction::<SanitizedTransactionView<_>>::try_from(
                    transaction,
                    solana_sdk::transaction::MessageHash::Compute,
                    Some(packet.meta().is_simple_vote_tx()),
                ) else {
                    continue;
                };

                // Calculate priority if we can, if this fails we drop.
                let Some(priority) = calculate_priority(&transaction, bank) else {
                    continue;
                };

                // If at capacity, check lowest priority item.
                if self.packet_container.priority_queue.len()
                    == self.packet_container.priority_queue.capacity()
                {
                    let min_priority = self
                        .packet_container
                        .priority_queue
                        .peek_min()
                        .expect("not empty")
                        .priority;
                    // If priority of current packet is not higher than the min
                    // drop the current packet.
                    if min_priority >= priority {
                        continue;
                    }

                    let dropped_index = self
                        .packet_container
                        .priority_queue
                        .pop_min()
                        .expect("not empty")
                        .index;
                    self.packet_container.packets.remove(dropped_index);
                }

                let entry = self.packet_container.packets.vacant_entry();
                let index = entry.key();
                entry.insert(packet.clone());
                let priority_index = PriorityIndex { priority, index };
                self.packet_container.priority_queue.push(priority_index);
            }
        }
    }

    fn forward_buffered_packets(&mut self) {
        todo!()
    }

    fn initial_packet_meta_filter(packet: &Packet) -> bool {
        let meta = packet.meta();
        !meta.discard() && !meta.forwarded() && meta.is_from_staked_node()
    }
}

struct PacketContainer {
    priority_queue: MinMaxHeap<PriorityIndex>,
    packets: Slab<Packet>,
}

impl PacketContainer {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            priority_queue: MinMaxHeap::with_capacity(capacity),
            packets: Slab::with_capacity(capacity),
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
struct PriorityIndex {
    priority: u64,
    index: usize,
}

/// Calculate priority for a transaction:
///
/// The priority is calculated as:
/// P = R / (1 + C)
/// where P is the priority, R is the reward,
/// and C is the cost towards block-limits.
///
/// Current minimum costs are on the order of several hundred,
/// so the denominator is effectively C, and the +1 is simply
/// to avoid any division by zero due to a bug - these costs
/// are estimate by the cost-model and are not direct
/// from user input. They should never be zero.
/// Any difference in the prioritization is negligible for
/// the current transaction costs.
fn calculate_priority(
    transaction: &RuntimeTransaction<SanitizedTransactionView<&[u8]>>,
    bank: &Bank,
) -> Option<u64> {
    let compute_budget_limits = transaction
        .compute_budget_instruction_details()
        .sanitize_and_convert_to_compute_budget_limits(&bank.feature_set)
        .ok()?;
    let fee_budget_limits = FeeBudgetLimits::from(compute_budget_limits);

    // Manually estimate fee here since currently interface doesn't allow a on SVM type.
    // Doesn't need to be 100% accurate so long as close and consistent.
    let prioritization_fee = fee_budget_limits.prioritization_fee;
    let signature_details = transaction.signature_details();
    let signature_fee = signature_details
        .total_signatures()
        .saturating_mul(bank.fee_structure().lamports_per_signature);
    let fee = signature_fee.saturating_add(prioritization_fee);

    let cost = CostModel::estimate_cost(
        transaction,
        transaction.program_instructions_iter(),
        transaction.num_requested_write_locks(),
        &bank.feature_set,
    );

    // We need a multiplier here to avoid rounding down too aggressively.
    // For many transactions, the cost will be greater than the fees in terms of raw lamports.
    // For the purposes of calculating prioritization, we multiply the fees by a large number so that
    // the cost is a small fraction.
    // An offset of 1 is used in the denominator to explicitly avoid division by zero.
    const MULTIPLIER: u64 = 1_000_000;
    Some(
        MULTIPLIER
            .saturating_mul(fee)
            .wrapping_div(cost.sum().saturating_add(1)),
    )
}
