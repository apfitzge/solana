use {
    agave_banking_stage_ingress_types::BankingPacketBatch,
    crossbeam_channel::Receiver,
    min_max_heap::MinMaxHeap,
    slab::Slab,
    solana_client::connection_cache::ConnectionCache,
    solana_net_utils::bind_to_unspecified,
    solana_perf::{data_budget::DataBudget, packet::Packet},
    std::{
        net::UdpSocket,
        sync::Arc,
        thread::{Builder, JoinHandle},
    },
};

pub struct ForwardingStage {
    receiver: Receiver<(BankingPacketBatch, bool)>,
    vote_packet_container: PacketContainer,
    non_vote_packet_container: PacketContainer,

    connection_cache: Arc<ConnectionCache>,
    data_budget: DataBudget,
    udp_socket: UdpSocket,
}

impl ForwardingStage {
    pub fn spawn(
        receiver: Receiver<(BankingPacketBatch, bool)>,
        connection_cache: Arc<ConnectionCache>,
    ) -> JoinHandle<()> {
        let forwarding_stage = Self::new(receiver, connection_cache);
        Builder::new()
            .name("solFwdStage".to_string())
            .spawn(move || forwarding_stage.run())
            .unwrap()
    }

    fn new(
        receiver: Receiver<(BankingPacketBatch, bool)>,
        connection_cache: Arc<ConnectionCache>,
    ) -> Self {
        Self {
            receiver,
            vote_packet_container: PacketContainer::with_capacity(4096),
            non_vote_packet_container: PacketContainer::with_capacity(4 * 4096),
            connection_cache,
            data_budget: DataBudget::default(),
            udp_socket: bind_to_unspecified().unwrap(),
        }
    }

    fn run(mut self) {
        loop {
            if !self.receive_and_buffer() {
                break;
            }
            self.forward_buffered_packets();
        }
    }

    fn receive_and_buffer(&mut self) -> bool {
        todo!()
    }

    fn forward_buffered_packets(&mut self) {
        todo!()
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

type Index = u16;

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
struct PriorityIndex {
    priority: u64,
    index: u16,
}
