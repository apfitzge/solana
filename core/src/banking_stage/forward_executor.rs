use {
    super::{BankingStageStats, ForwardOption},
    crate::next_leader,
    solana_client::{connection_cache::ConnectionCache, tpu_connection::TpuConnection},
    solana_gossip::cluster_info::ClusterInfo,
    solana_measure::measure::Measure,
    solana_perf::{data_budget::DataBudget, packet::Packet},
    solana_poh::poh_recorder::PohRecorder,
    solana_sdk::{pubkey::Pubkey, transport::TransportError},
    solana_streamer::sendmmsg::batch_send,
    std::{
        iter::repeat,
        net::UdpSocket,
        sync::{atomic::Ordering, Arc, RwLock},
    },
};

pub struct ForwardExecutor {
    poh_recorder: Arc<RwLock<PohRecorder>>,
    socket: UdpSocket,
    cluster_info: Arc<ClusterInfo>,
    connection_cache: Arc<ConnectionCache>,
    data_budget: Arc<DataBudget>,
}

impl ForwardExecutor {
    pub fn new(
        poh_recorder: Arc<RwLock<PohRecorder>>,
        socket: UdpSocket,
        cluster_info: Arc<ClusterInfo>,
        connection_cache: Arc<ConnectionCache>,
        data_budget: Arc<DataBudget>,
    ) -> Self {
        Self {
            poh_recorder,
            socket,
            cluster_info,
            connection_cache,
            data_budget,
        }
    }

    /// Forwards all valid, unprocessed packets in the buffer, up to a rate limit. Returns
    /// the number of successfully forwarded packets in second part of tuple
    pub fn forward_buffered_packets<'a>(
        &self,
        forward_option: &ForwardOption,
        forwardable_packets: impl Iterator<Item = &'a Packet>,
        banking_stage_stats: &BankingStageStats,
    ) -> (
        std::result::Result<(), TransportError>,
        usize,
        Option<Pubkey>,
    ) {
        let leader_and_addr = match forward_option {
            ForwardOption::NotForward => return (Ok(()), 0, None),
            ForwardOption::ForwardTransaction => {
                next_leader::next_leader_tpu_forwards(&self.cluster_info, &self.poh_recorder)
            }

            ForwardOption::ForwardTpuVote => {
                next_leader::next_leader_tpu_vote(&self.cluster_info, &self.poh_recorder)
            }
        };
        let (leader_pubkey, addr) = match leader_and_addr {
            Some(leader_and_addr) => leader_and_addr,
            None => return (Ok(()), 0, None),
        };

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

        let packet_vec: Vec<_> = forwardable_packets
            .filter_map(|p| {
                if !p.meta.forwarded() && self.data_budget.take(p.meta.size) {
                    Some(p.data(..)?.to_vec())
                } else {
                    None
                }
            })
            .collect();

        let packet_vec_len = packet_vec.len();
        // TODO: see https://github.com/solana-labs/solana/issues/23819
        // fix this so returns the correct number of succeeded packets
        // when there's an error sending the batch. This was left as-is for now
        // in favor of shipping Quic support, which was considered higher-priority
        if !packet_vec.is_empty() {
            inc_new_counter_info!("banking_stage-forwarded_packets", packet_vec_len);

            let mut measure = Measure::start("banking_stage-forward-us");

            let res = if let ForwardOption::ForwardTpuVote = forward_option {
                // The vote must be forwarded using only UDP.
                banking_stage_stats
                    .forwarded_vote_count
                    .fetch_add(packet_vec_len, Ordering::Relaxed);
                let pkts: Vec<_> = packet_vec.into_iter().zip(repeat(addr)).collect();
                batch_send(&self.socket, &pkts).map_err(|err| err.into())
            } else {
                // All other transactions can be forwarded using QUIC, get_connection() will use
                // system wide setting to pick the correct connection object.
                banking_stage_stats
                    .forwarded_transaction_count
                    .fetch_add(packet_vec_len, Ordering::Relaxed);
                let conn = self.connection_cache.get_connection(&addr);
                conn.send_wire_transaction_batch_async(packet_vec)
            };

            measure.stop();
            inc_new_counter_info!(
                "banking_stage-forward-us",
                measure.as_us() as usize,
                1000,
                1000
            );

            if let Err(err) = res {
                inc_new_counter_info!("banking_stage-forward_packets-failed-batches", 1);
                return (Err(err), 0, Some(leader_pubkey));
            }
        }

        (Ok(()), packet_vec_len, Some(leader_pubkey))
    }
}
