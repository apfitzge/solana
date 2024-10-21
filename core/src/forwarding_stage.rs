use {
    crate::{
        banking_stage::LikeClusterInfo,
        banking_trace::BankingPacketBatch,
        next_leader::{next_leader, next_leader_tpu_vote},
    },
    crossbeam_channel::Receiver,
    solana_client::connection_cache::ConnectionCache,
    solana_connection_cache::client_connection::ClientConnection,
    solana_gossip::contact_info::Protocol,
    solana_perf::data_budget::DataBudget,
    solana_poh::poh_recorder::PohRecorder,
    solana_streamer::sendmmsg::batch_send,
    std::{
        iter::repeat,
        net::{SocketAddr, UdpSocket},
        sync::{Arc, RwLock},
        thread::{Builder, JoinHandle},
    },
};

pub trait ForwardAddressGetter: Send + Sync + 'static {
    fn get_forwarding_address(&self, tpu_vote: bool, protocol: Protocol) -> Option<SocketAddr>;
}

impl<T: LikeClusterInfo> ForwardAddressGetter for (T, Arc<RwLock<PohRecorder>>) {
    fn get_forwarding_address(&self, tpu_vote: bool, protocol: Protocol) -> Option<SocketAddr> {
        if tpu_vote {
            next_leader_tpu_vote(&self.0, &self.1)
        } else {
            next_leader(&self.0, &self.1, |node| node.tpu_forwards(protocol))
        }
        .map(|(_, addr)| addr)
    }
}

pub struct ForwardingStage<T: ForwardAddressGetter> {
    receiver: Receiver<(BankingPacketBatch, bool)>,
    forward_address_getter: T,
    connection_cache: Arc<ConnectionCache>,
    data_budget: DataBudget,
    udp_socket: UdpSocket,
}

impl<T: ForwardAddressGetter> ForwardingStage<T> {
    pub fn spawn(
        receiver: Receiver<(BankingPacketBatch, bool)>,
        forward_address_getter: T,
        connection_cache: Arc<ConnectionCache>,
    ) -> JoinHandle<()> {
        let forwarding_stage = Self::new(receiver, forward_address_getter, connection_cache);
        Builder::new()
            .name("solFwdStage".to_string())
            .spawn(move || forwarding_stage.run())
            .unwrap()
    }

    fn new(
        receiver: Receiver<(BankingPacketBatch, bool)>,
        forward_address_getter: T,
        connection_cache: Arc<ConnectionCache>,
    ) -> Self {
        Self {
            receiver,
            forward_address_getter,
            connection_cache,
            data_budget: DataBudget::default(),
            udp_socket: UdpSocket::bind("0.0.0.0:0").unwrap(),
        }
    }

    fn run(self) {
        while let Ok((packet_batches, tpu_vote_batch)) = self.receiver.recv() {
            // Get the address to forward the packets to.
            let Some(addr) = self
                .forward_address_getter
                .get_forwarding_address(tpu_vote_batch, self.connection_cache.protocol())
            else {
                // If unknown, move to next packet batch.
                continue;
            };

            self.update_data_budget();
            self.forward_batch(packet_batches, tpu_vote_batch, addr);
        }
    }

    fn forward_batch(
        &self,
        packet_batches: BankingPacketBatch,
        tpu_vote_batch: bool,
        addr: SocketAddr,
    ) {
        let filtered_packets = packet_batches
            .0
            .iter()
            .flat_map(|batch| batch.iter())
            .filter(|p| !p.meta().forwarded())
            .filter(|p| p.meta().is_from_staked_node())
            .filter(|p| self.data_budget.take(p.meta().size))
            .filter_map(|p| p.data(..).map(|data| data.to_vec()));

        if tpu_vote_batch {
            let pkts: Vec<_> = filtered_packets.into_iter().zip(repeat(addr)).collect();
            let _ = batch_send(&self.udp_socket, &pkts);
        } else {
            let conn = self.connection_cache.get_connection(&addr);
            let _ = conn.send_data_batch_async(filtered_packets.collect::<Vec<_>>());
        }
    }

    /// Re-fill the data budget if enough time has passed
    fn update_data_budget(&self) {
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

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_perf::packet::PacketBatch,
        solana_sdk::packet::{Packet, PacketFlags},
        std::time::Duration,
    };

    struct DummyForwardAddressGetter(Option<SocketAddr>);
    impl ForwardAddressGetter for DummyForwardAddressGetter {
        fn get_forwarding_address(
            &self,
            _tpu_vote: bool,
            _protocol: Protocol,
        ) -> Option<SocketAddr> {
            self.0
        }
    }

    #[test]
    fn test_tpu_vote_forwarding() {
        let forward_socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        let forward_addr = forward_socket.local_addr().unwrap();

        let (_sender, receiver) = crossbeam_channel::unbounded();
        let dummy_forward_address_getter = DummyForwardAddressGetter(Some(forward_addr));
        let connection_cache = ConnectionCache::new("connection_cache_test");

        let forwarding_stage = ForwardingStage::new(
            receiver,
            dummy_forward_address_getter,
            Arc::new(connection_cache),
        );

        const NUM_BYTES: usize = 8;
        let mut packet = Packet::default();
        packet.populate_packet(None, &[0u8; NUM_BYTES]).unwrap(); // we don't need actual content here
        packet.meta_mut().set_simple_vote(true);
        packet.meta_mut().set_from_staked_node(true);

        forward_socket
            .set_read_timeout(Some(Duration::from_millis(10)))
            .unwrap();
        let recv_buffer = &mut [0; 1024];

        // Data Budget is 0, so no packets should be sent
        forwarding_stage.forward_batch(
            BankingPacketBatch::new((vec![PacketBatch::new(vec![packet.clone()])], None)),
            true,
            forward_addr,
        );
        assert!(forward_socket.recv_from(recv_buffer).is_err());

        // Packet is valid, so it should be sent
        forwarding_stage.update_data_budget();
        forwarding_stage.forward_batch(
            BankingPacketBatch::new((vec![PacketBatch::new(vec![packet.clone()])], None)),
            true,
            forward_addr,
        );
        assert_eq!(forward_socket.recv_from(recv_buffer).unwrap().0, NUM_BYTES);

        // Packet is not from staked node, so it should not be sent
        forwarding_stage.update_data_budget();
        packet.meta_mut().set_from_staked_node(false);
        forwarding_stage.forward_batch(
            BankingPacketBatch::new((vec![PacketBatch::new(vec![packet.clone()])], None)),
            true,
            forward_addr,
        );
        assert!(forward_socket.recv_from(recv_buffer).is_err());

        // Packet is already forwarded, so it should not be sent
        forwarding_stage.update_data_budget();
        packet.meta_mut().set_from_staked_node(true);
        packet.meta_mut().flags |= PacketFlags::FORWARDED;
        forwarding_stage.forward_batch(
            BankingPacketBatch::new((vec![PacketBatch::new(vec![packet.clone()])], None)),
            true,
            forward_addr,
        );
        assert!(forward_socket.recv_from(recv_buffer).is_err());
    }
}
