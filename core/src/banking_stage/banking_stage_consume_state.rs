use {
    crate::qos_service::QosService, solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_runtime::vote_sender_types::ReplayVoteSender,
};

pub struct BankingStageConsumeState {
    pub transaction_status_sender: Option<TransactionStatusSender>,
    pub gossip_vote_sender: ReplayVoteSender,
    pub qos_service: QosService,
    pub log_messages_bytes_limit: Option<usize>,
}

impl BankingStageConsumeState {
    pub fn new(
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        qos_service: QosService,
        log_messages_bytes_limit: Option<usize>,
    ) -> Self {
        Self {
            transaction_status_sender,
            gossip_vote_sender,
            qos_service,
            log_messages_bytes_limit,
        }
    }
}
