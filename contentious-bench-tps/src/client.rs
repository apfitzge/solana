use {
    crate::accounts::Accounts,
    solana_rpc_client::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig, hash::Hash, signature::Keypair, signer::Signer,
    },
    solana_tpu_client::{
        connection_cache::ConnectionCache,
        tpu_client::{TpuClient, TpuClientConfig},
        tpu_connection_cache::DEFAULT_TPU_CONNECTION_POOL_SIZE,
    },
    std::sync::Arc,
};

/// Wrapper for TpuClient for sending and verifying transactions.
pub struct Client {
    /// The TPU client - sends transactions to the network.
    client: TpuClient,
}

impl Client {
    /// Creates a new client.
    pub fn new(rpc_url: String, websocket_url: &str) -> Self {
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
        .unwrap();

        Self { client }
    }

    /// Funds the given accounts with enough lamports for the benchmark.
    ///     - Sends in chunks to avoid the blockhash getting stale.
    pub fn fund_accounts(&self, accounts: &Accounts) {
        const ACCOUNT_CHUNK_SIZE: usize = 100;

        accounts
            .contentious_accounts
            .chunks(ACCOUNT_CHUNK_SIZE)
            .for_each(|accounts| self.fund_account_chunk(accounts));
        accounts
            .regular_accounts
            .chunks(ACCOUNT_CHUNK_SIZE)
            .for_each(|accounts| self.fund_account_chunk(accounts));
    }

    /// Fund a chunk of accounts
    fn fund_account_chunk(&self, accounts: &[Keypair]) {
        let recent_blockhash = self.get_recent_blockhash();
        accounts
            .iter()
            .for_each(|account| self.fund_account(account, &recent_blockhash));
    }

    /// Fund an individual account
    fn fund_account(&self, account: &Keypair, recent_blockhash: &Hash) {
        const LAMPORTS_PER_ACCOUNT: u64 = 1_000_000_000;

        self.client.rpc_client().request_airdrop_with_blockhash(
            &account.pubkey(),
            LAMPORTS_PER_ACCOUNT,
            recent_blockhash,
        );
    }

    /// Gets a recent blockhash to use for sending transactions.
    fn get_recent_blockhash(&self) -> Hash {
        self.client
            .rpc_client()
            .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
            .unwrap()
            .0
    }
}
