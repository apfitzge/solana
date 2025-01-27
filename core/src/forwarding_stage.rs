use {
    crate::{
        banking_stage::LikeClusterInfo,
        next_leader::{next_leader, next_leader_tpu_vote},
    },
    agave_banking_stage_ingress_types::BankingPacketBatch,
    agave_transaction_view::transaction_view::SanitizedTransactionView,
    crossbeam_channel::{Receiver, RecvTimeoutError},
    min_max_heap::MinMaxHeap,
    slab::Slab,
    solana_client::connection_cache::ConnectionCache,
    solana_connection_cache::client_connection::ClientConnection,
    solana_cost_model::cost_model::CostModel,
    solana_gossip::contact_info::Protocol,
    solana_net_utils::bind_to,
    solana_perf::{data_budget::DataBudget, packet::Packet},
    solana_poh::poh_recorder::PohRecorder,
    solana_runtime::{bank::Bank, root_bank_cache::RootBankCache},
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
    },
    solana_sdk::{fee::FeeBudgetLimits, packet, transaction::MessageHash},
    solana_streamer::sendmmsg::batch_send,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        sync::{Arc, RwLock},
        thread::{Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

const FORWARD_BATCH_SIZE: usize = 128;

pub trait ForwardAddressGetter: Clone + Send + Sync + 'static {
    fn get_non_vote_forwarding_addresses(&self, protocol: Protocol) -> Option<SocketAddr>;
    fn get_vote_forwarding_addresses(&self) -> Option<SocketAddr>;
}

impl<T: LikeClusterInfo> ForwardAddressGetter for (T, Arc<RwLock<PohRecorder>>) {
    fn get_non_vote_forwarding_addresses(&self, protocol: Protocol) -> Option<SocketAddr> {
        next_leader(&self.0, &self.1, |node| node.tpu_forwards(protocol)).map(|(_, s)| s)
    }
    fn get_vote_forwarding_addresses(&self) -> Option<SocketAddr> {
        next_leader_tpu_vote(&self.0, &self.1).map(|(_, s)| s)
    }
}

struct VoteClient<F: ForwardAddressGetter> {
    udp_socket: UdpSocket,
    forward_address_getter: F,
    current_address: Option<SocketAddr>,
}

impl<F: ForwardAddressGetter> VoteClient<F> {
    fn new(forward_address_getter: F) -> Self {
        Self {
            udp_socket: bind_to(IpAddr::V4(Ipv4Addr::LOCALHOST), 0, false).unwrap(),
            forward_address_getter,
            current_address: None,
        }
    }

    fn update_address(&mut self) -> bool {
        self.current_address = self.forward_address_getter.get_vote_forwarding_addresses();
        self.current_address.is_some()
    }

    //TODO(klykov): This function should be modified after adding batch_send
    //version with one socket address.
    fn send_batch(&self, input_batch: &mut Vec<Vec<u8>>) {
        assert!(
            self.current_address.is_some(),
            "current_address should be updated before send_batch call."
        );
        let mut batch: Vec<(Vec<u8>, SocketAddr)> = Vec::with_capacity(input_batch.len());
        for packet in input_batch.drain(..) {
            batch.push((packet, self.current_address.unwrap()));
        }
        let _res = batch_send(&self.udp_socket, &batch);
    }
}

struct ConnectionCacheClient<F: ForwardAddressGetter> {
    connection_cache: Arc<ConnectionCache>,
    forward_address_getter: F,
    current_address: Option<SocketAddr>,
}

impl<F: ForwardAddressGetter> ConnectionCacheClient<F> {
    fn new(connection_cache: Arc<ConnectionCache>, forward_address_getter: F) -> Self {
        Self {
            connection_cache,
            forward_address_getter,
            current_address: None,
        }
    }

    fn update_address(&mut self) -> bool {
        self.current_address = self
            .forward_address_getter
            .get_non_vote_forwarding_addresses(self.connection_cache.protocol());
        self.current_address.is_some()
    }

    fn send_batch(&self, input_batch: &mut Vec<Vec<u8>>) {
        assert!(
            self.current_address.is_some(),
            "current_address should be updated before send_batch call."
        );
        let conn = self
            .connection_cache
            .get_connection(&self.current_address.unwrap());
        let mut batch = Vec::with_capacity(FORWARD_BATCH_SIZE);
        core::mem::swap(&mut batch, input_batch);
        let _res = conn.send_data_batch_async(batch);
    }
}

pub struct ForwardingStage<F: ForwardAddressGetter> {
    receiver: Receiver<(BankingPacketBatch, bool)>,
    packet_container: PacketContainer,

    root_bank_cache: RootBankCache,
    transaction_client: ConnectionCacheClient<F>,
    vote_client: VoteClient<F>,
    data_budget: DataBudget,

    metrics: ForwardingStageMetrics,
}

impl<F: ForwardAddressGetter> ForwardingStage<F> {
    pub fn spawn(
        receiver: Receiver<(BankingPacketBatch, bool)>,
        connection_cache: Arc<ConnectionCache>,
        root_bank_cache: RootBankCache,
        forward_address_getter: F,
    ) -> JoinHandle<()> {
        let transaction_client =
            ConnectionCacheClient::new(connection_cache, forward_address_getter.clone());
        let vote_client = VoteClient::new(forward_address_getter);
        let forwarding_stage =
            Self::new(receiver, transaction_client, vote_client, root_bank_cache);
        Builder::new()
            .name("solFwdStage".to_string())
            .spawn(move || forwarding_stage.run())
            .unwrap()
    }

    fn new(
        receiver: Receiver<(BankingPacketBatch, bool)>,
        transaction_client: ConnectionCacheClient<F>,
        vote_client: VoteClient<F>,
        root_bank_cache: RootBankCache,
    ) -> Self {
        Self {
            receiver,
            packet_container: PacketContainer::with_capacity(4 * 4096),
            root_bank_cache,
            transaction_client,
            vote_client,
            data_budget: DataBudget::default(),
            metrics: ForwardingStageMetrics::default(),
        }
    }

    fn run(mut self) {
        loop {
            let root_bank = self.root_bank_cache.root_bank();
            if !self.receive_and_buffer(&root_bank) {
                break;
            }
            self.forward_buffered_packets();
            self.metrics.maybe_report();
        }
    }

    fn receive_and_buffer(&mut self, bank: &Bank) -> bool {
        let now = Instant::now();
        const TIMEOUT: Duration = Duration::from_millis(10);
        match self.receiver.recv_timeout(TIMEOUT) {
            Ok((packet_batches, tpu_vote_batch)) => {
                self.metrics.did_something = true;
                self.buffer_packet_batches(packet_batches, tpu_vote_batch, bank);

                // Drain the channel up to timeout
                let timed_out = loop {
                    if now.elapsed() >= TIMEOUT {
                        break true;
                    }
                    match self.receiver.try_recv() {
                        Ok((packet_batches, tpu_vote_batch)) => {
                            self.buffer_packet_batches(packet_batches, tpu_vote_batch, bank)
                        }
                        Err(_) => break false,
                    }
                };

                // If timeout waas reached, prevent backup by draining all
                // packets in the channel.
                if timed_out {
                    warn!("ForwardingStage is backed up, dropping packets");
                    while let Ok((packet_batch, _)) = self.receiver.try_recv() {
                        self.metrics.dropped_on_timeout +=
                            packet_batch.iter().map(|b| b.len()).sum::<usize>();
                    }
                }

                true
            }
            Err(RecvTimeoutError::Timeout) => true,
            Err(RecvTimeoutError::Disconnected) => false,
        }
    }

    fn buffer_packet_batches(
        &mut self,
        packet_batches: BankingPacketBatch,
        is_tpu_vote_batch: bool,
        bank: &Bank,
    ) {
        for batch in packet_batches.iter() {
            for packet in batch
                .iter()
                .filter(|p| initial_packet_meta_filter(p.meta()))
            {
                let Some(packet_data) = packet.data(..) else {
                    // should never occur since we've already checked the
                    // packet is not marked for discard.
                    continue;
                };

                let vote_count = usize::from(is_tpu_vote_batch);
                let non_vote_count = usize::from(!is_tpu_vote_batch);

                self.metrics.votes_received += vote_count;
                self.metrics.non_votes_received += non_vote_count;

                // Perform basic sanitization checks and calculate priority.
                // If any steps fail, drop the packet.
                let Some(priority) = SanitizedTransactionView::try_new_sanitized(packet_data)
                    .map_err(|_| ())
                    .and_then(|transaction| {
                        RuntimeTransaction::<SanitizedTransactionView<_>>::try_from(
                            transaction,
                            MessageHash::Compute,
                            Some(packet.meta().is_simple_vote_tx()),
                        )
                        .map_err(|_| ())
                    })
                    .ok()
                    .and_then(|transaction| calculate_priority(&transaction, bank))
                else {
                    self.metrics.votes_dropped_on_receive += vote_count;
                    self.metrics.non_votes_dropped_on_receive += non_vote_count;
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
                        self.metrics.votes_dropped_on_capacity += vote_count;
                        self.metrics.non_votes_dropped_on_capacity += non_vote_count;
                        continue;
                    }

                    let dropped_index = self
                        .packet_container
                        .priority_queue
                        .pop_min()
                        .expect("not empty")
                        .index;
                    let dropped_packet = self.packet_container.packets.remove(dropped_index);
                    self.metrics.votes_dropped_on_capacity +=
                        usize::from(dropped_packet.meta().is_simple_vote_tx());
                    self.metrics.non_votes_dropped_on_capacity +=
                        usize::from(!dropped_packet.meta().is_simple_vote_tx());
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
        self.metrics.did_something |= !self.packet_container.priority_queue.is_empty();
        self.refresh_data_budget();

        // Get forwarding addresses otherwise return now.
        if !self.vote_client.update_address() || !self.transaction_client.update_address() {
            return;
        }

        let mut non_vote_batch = Vec::with_capacity(FORWARD_BATCH_SIZE);
        let mut vote_batch = Vec::with_capacity(FORWARD_BATCH_SIZE);

        // Loop through packets creating batches of packets to forward.
        while let Some(priority_index) = self.packet_container.priority_queue.pop_max() {
            let packet = self
                .packet_container
                .packets
                .get(priority_index.index)
                .expect("packet exists");

            // If it exceeds our data-budget, drop.
            if !self.data_budget.take(packet.meta().size) {
                self.metrics.votes_dropped_on_data_budget +=
                    usize::from(packet.meta().is_simple_vote_tx());
                self.metrics.non_votes_dropped_on_data_budget +=
                    usize::from(!packet.meta().is_simple_vote_tx());
                self.packet_container.packets.remove(priority_index.index);
                continue;
            }

            let packet_data_vec = packet.data(..).expect("packet has data").to_vec();

            if packet.meta().is_simple_vote_tx() {
                vote_batch.push(packet_data_vec);
                if vote_batch.len() == vote_batch.capacity() {
                    self.vote_client.send_batch(&mut vote_batch);
                }
            } else {
                non_vote_batch.push(packet_data_vec);
                if non_vote_batch.len() == non_vote_batch.capacity() {
                    self.transaction_client.send_batch(&mut non_vote_batch);
                }
            }
        }

        // Send out remaining packets
        if !vote_batch.is_empty() {
            self.metrics.votes_forwarded += vote_batch.len();
            self.vote_client.send_batch(&mut vote_batch);
        }
        if !non_vote_batch.is_empty() {
            self.metrics.non_votes_forwarded += non_vote_batch.len();
            self.transaction_client.send_batch(&mut non_vote_batch);
        }
    }

    /// Re-fill the data budget if enough time has passed
    fn refresh_data_budget(&self) {
        const INTERVAL_MS: u64 = 100;
        // 12 MB outbound limit per second
        const MAX_BYTES_PER_SECOND: usize = 12_000_000;
        const MAX_BYTES_PER_INTERVAL: usize = MAX_BYTES_PER_SECOND * INTERVAL_MS as usize / 1000;
        const MAX_BYTES_BUDGET: usize = MAX_BYTES_PER_INTERVAL * 5;
        self.data_budget.update(INTERVAL_MS, |bytes| {
            std::cmp::min(
                bytes.saturating_add(MAX_BYTES_PER_INTERVAL),
                MAX_BYTES_BUDGET,
            )
        });
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

struct ForwardingStageMetrics {
    last_reported: Instant,
    did_something: bool,

    votes_received: usize,
    votes_dropped_on_receive: usize,
    votes_dropped_on_capacity: usize,
    votes_dropped_on_data_budget: usize,
    votes_forwarded: usize,

    non_votes_received: usize,
    non_votes_dropped_on_receive: usize,
    non_votes_dropped_on_capacity: usize,
    non_votes_dropped_on_data_budget: usize,
    non_votes_forwarded: usize,

    dropped_on_timeout: usize,
}

impl ForwardingStageMetrics {
    fn maybe_report(&mut self) {
        const REPORTING_INTERVAL: Duration = Duration::from_secs(1);

        if self.last_reported.elapsed() > REPORTING_INTERVAL {
            // Reset time and all counts.
            let metrics = core::mem::take(self);

            // Only report if something happened.
            if !metrics.did_something {
                return;
            }

            datapoint_info!(
                "forwarding_stage",
                ("votes_received", metrics.votes_received, i64),
                (
                    "votes_dropped_on_receive",
                    metrics.votes_dropped_on_receive,
                    i64
                ),
                (
                    "votes_dropped_on_data_budget",
                    metrics.votes_dropped_on_data_budget,
                    i64
                ),
                ("votes_forwarded", metrics.votes_forwarded, i64),
                ("non_votes_received", metrics.non_votes_received, i64),
                (
                    "votes_dropped_on_receive",
                    metrics.votes_dropped_on_receive,
                    i64
                ),
                (
                    "votes_dropped_on_data_budget",
                    metrics.votes_dropped_on_data_budget,
                    i64
                ),
                ("votes_forwarded", metrics.votes_forwarded, i64),
            );
        }
    }
}

impl Default for ForwardingStageMetrics {
    fn default() -> Self {
        Self {
            last_reported: Instant::now(),
            did_something: false,
            votes_received: 0,
            votes_dropped_on_receive: 0,
            votes_dropped_on_capacity: 0,
            votes_dropped_on_data_budget: 0,
            votes_forwarded: 0,
            non_votes_received: 0,
            non_votes_dropped_on_receive: 0,
            non_votes_dropped_on_capacity: 0,
            non_votes_dropped_on_data_budget: 0,
            non_votes_forwarded: 0,
            dropped_on_timeout: 0,
        }
    }
}

fn initial_packet_meta_filter(meta: &packet::Meta) -> bool {
    !meta.discard() && !meta.forwarded() && meta.is_from_staked_node()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crossbeam_channel::unbounded,
        packet::PacketFlags,
        solana_perf::packet::PacketBatch,
        solana_pubkey::Pubkey,
        solana_runtime::genesis_utils::create_genesis_config,
        solana_sdk::{hash::Hash, signature::Keypair, system_transaction},
    };

    impl ForwardAddressGetter for ForwardingAddresses {
        fn get_forwarding_addresses(&self, _protocol: Protocol) -> ForwardingAddresses {
            self.clone()
        }
    }

    fn meta_with_flags(packet_flags: PacketFlags) -> packet::Meta {
        packet::Meta {
            flags: packet_flags,
            ..packet::Meta::default()
        }
    }

    fn simple_transfer_with_flags(packet_flags: PacketFlags) -> Packet {
        let transaction = system_transaction::transfer(
            &Keypair::new(),
            &Pubkey::new_unique(),
            1,
            Hash::default(),
        );
        let mut packet = Packet::from_data(None, &transaction).unwrap();
        packet.meta_mut().flags = packet_flags;
        packet
    }

    #[test]
    fn test_initial_packet_meta_filter() {
        assert!(!initial_packet_meta_filter(&meta_with_flags(
            PacketFlags::empty()
        )));
        assert!(initial_packet_meta_filter(&meta_with_flags(
            PacketFlags::FROM_STAKED_NODE
        )));
        assert!(!initial_packet_meta_filter(&meta_with_flags(
            PacketFlags::DISCARD
        )));
        assert!(!initial_packet_meta_filter(&meta_with_flags(
            PacketFlags::FORWARDED
        )));
        assert!(!initial_packet_meta_filter(&meta_with_flags(
            PacketFlags::FROM_STAKED_NODE | PacketFlags::DISCARD
        )));
    }

    #[test]
    fn test_forwarding() {
        let vote_socket = bind_to_unspecified().unwrap();
        vote_socket
            .set_read_timeout(Some(Duration::from_millis(10)))
            .unwrap();
        let non_vote_socket = bind_to_unspecified().unwrap();
        non_vote_socket
            .set_read_timeout(Some(Duration::from_millis(10)))
            .unwrap();

        let forwarding_addresses = ForwardingAddresses {
            tpu_vote: Some(vote_socket.local_addr().unwrap()),
            tpu: Some(non_vote_socket.local_addr().unwrap()),
        };

        let (packet_batch_sender, packet_batch_receiver) = unbounded();
        let connection_cache = Arc::new(ConnectionCache::with_udp("connection_cache_test", 1));
        let (_bank, bank_forks) =
            Bank::new_with_bank_forks_for_tests(&create_genesis_config(1).genesis_config);
        let root_bank_cache = RootBankCache::new(bank_forks);
        let mut forwarding_stage = ForwardingStage::new(
            packet_batch_receiver,
            connection_cache,
            root_bank_cache,
            forwarding_addresses,
        );

        // Send packet batches.
        let non_vote_packets = BankingPacketBatch::new(vec![PacketBatch::new(vec![
            simple_transfer_with_flags(PacketFlags::FROM_STAKED_NODE),
            simple_transfer_with_flags(PacketFlags::FROM_STAKED_NODE | PacketFlags::DISCARD),
            simple_transfer_with_flags(PacketFlags::FROM_STAKED_NODE | PacketFlags::FORWARDED),
        ])]);
        let vote_packets = BankingPacketBatch::new(vec![PacketBatch::new(vec![
            simple_transfer_with_flags(PacketFlags::SIMPLE_VOTE_TX | PacketFlags::FROM_STAKED_NODE),
            simple_transfer_with_flags(
                PacketFlags::SIMPLE_VOTE_TX | PacketFlags::FROM_STAKED_NODE | PacketFlags::DISCARD,
            ),
            simple_transfer_with_flags(
                PacketFlags::SIMPLE_VOTE_TX
                    | PacketFlags::FROM_STAKED_NODE
                    | PacketFlags::FORWARDED,
            ),
        ])]);

        packet_batch_sender
            .send((non_vote_packets.clone(), false))
            .unwrap();
        packet_batch_sender
            .send((vote_packets.clone(), true))
            .unwrap();

        let bank = forwarding_stage.root_bank_cache.root_bank();
        forwarding_stage.receive_and_buffer(&bank);
        if !packet_batch_sender.is_empty() {
            forwarding_stage.receive_and_buffer(&bank);
        }
        assert_eq!(forwarding_stage.packet_container.priority_queue.len(), 2); // only 2 valid packets
        forwarding_stage.forward_buffered_packets();

        let recv_buffer = &mut [0; 1024];
        let (vote_packet_bytes, _) = vote_socket.recv_from(recv_buffer).unwrap();
        assert_eq!(
            &recv_buffer[..vote_packet_bytes],
            vote_packets[0][0].data(..).unwrap()
        );
        assert!(vote_socket.recv_from(recv_buffer).is_err());

        let (non_vote_packet_bytes, _) = non_vote_socket.recv_from(recv_buffer).unwrap();
        assert_eq!(
            &recv_buffer[..non_vote_packet_bytes],
            non_vote_packets[0][0].data(..).unwrap()
        );
        assert!(non_vote_socket.recv_from(recv_buffer).is_err());
    }
}
