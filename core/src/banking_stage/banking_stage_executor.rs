use {
    crate::qos_service::QosService,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_perf::data_budget::DataBudget,
    solana_runtime::vote_sender_types::ReplayVoteSender,
    solana_tpu_client::connection_cache::ConnectionCache,
    std::{net::UdpSocket, sync::Arc},
};

/// Stores the state for executing packets in banking stage
pub struct BankingStageExecutor {
    pub socket: UdpSocket,
    pub cluster_info: Arc<ClusterInfo>,
    pub transaction_status_sender: Option<TransactionStatusSender>,
    pub gossip_vote_sender: ReplayVoteSender,
    pub data_budget: Arc<DataBudget>,
    pub qos_service: QosService,
    pub log_messages_bytes_limit: Option<usize>,
    pub connection_cache: Arc<ConnectionCache>,
}

impl BankingStageExecutor {
    pub fn new(
        cluster_info: Arc<ClusterInfo>,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        data_budget: Arc<DataBudget>,
        qos_service: QosService,
        log_messages_bytes_limit: Option<usize>,
        connection_cache: Arc<ConnectionCache>,
    ) -> Self {
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        Self {
            socket,
            cluster_info,
            transaction_status_sender,
            gossip_vote_sender,
            data_budget,
            qos_service,
            log_messages_bytes_limit,
            connection_cache,
        }
    }
}
