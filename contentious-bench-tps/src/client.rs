use {
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::commitment_config::CommitmentConfig,
    solana_tpu_client::{
        connection_cache::ConnectionCache, nonblocking::tpu_client::TpuClient,
        tpu_client::TpuClientConfig, tpu_connection_cache::DEFAULT_TPU_CONNECTION_POOL_SIZE,
    },
    std::sync::Arc,
};

/// Wrapper for TpuClient for sending and verifying transactions.
pub struct Client {
    /// The TPU client - sends transactions to the network.
    client: TpuClient,
}

impl Client {
    pub async fn new(rpc_url: String, websocket_url: &str) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url,
            CommitmentConfig::processed(),
        ));
        let connection_cache = Arc::new(ConnectionCache::new(DEFAULT_TPU_CONNECTION_POOL_SIZE));
        let client = TpuClient::new_with_connection_cache(
            rpc_client,
            websocket_url,
            TpuClientConfig::default(),
            connection_cache,
        )
        .await
        .unwrap();

        Self { client }
    }
}
