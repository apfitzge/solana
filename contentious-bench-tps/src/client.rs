use {
    crate::accounts::Accounts,
    log::info,
    solana_rpc_client::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig, hash::Hash, signature::Keypair, signer::Signer,
        transaction::Transaction, transport::TransportError,
    },
    solana_tpu_client::{
        connection_cache::ConnectionCache,
        tpu_client::{TpuClient, TpuClientConfig},
        tpu_connection_cache::DEFAULT_TPU_CONNECTION_POOL_SIZE,
    },
    std::{sync::Arc, time::Duration},
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
        let contentious_chunks = accounts.contentious_accounts.chunks(ACCOUNT_CHUNK_SIZE);
        let regular_chunks = accounts.regular_accounts.chunks(ACCOUNT_CHUNK_SIZE);

        let mut num_funded = 0;
        contentious_chunks
            .chain(regular_chunks)
            .for_each(|accounts| {
                self.fund_account_chunk(accounts);
                num_funded += accounts.len();
                info!("funded {num_funded} accounts...");
            });
    }

    /// Gets a recent blockhash to use for sending transactions.
    pub fn get_recent_blockhash(&self) -> Hash {
        self.client
            .rpc_client()
            .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
            .unwrap()
            .0
    }

    /// Send transactions to the network.
    pub fn send_transactions(&self, transactions: &[Transaction]) -> Result<(), TransportError> {
        self.client.try_send_transaction_batch(transactions)
    }

    /// Get the number of transactions on the cluster
    pub fn get_num_transactions(&self) -> u64 {
        self.client
            .rpc_client()
            .get_transaction_count_with_commitment(CommitmentConfig::confirmed())
            .unwrap()
    }

    /// Fund a chunk of accounts
    fn fund_account_chunk(&self, accounts: &[Keypair]) {
        std::thread::sleep(Duration::from_millis(1));
        let recent_blockhash = self.get_recent_blockhash();
        accounts
            .iter()
            .for_each(|account| self.fund_account(account, &recent_blockhash));
    }

    /// Fund an individual account
    fn fund_account(&self, account: &Keypair, recent_blockhash: &Hash) {
        const LAMPORTS_PER_ACCOUNT: u64 = 1_000_000_000;

        self.client
            .rpc_client()
            .request_airdrop_with_blockhash(
                &account.pubkey(),
                LAMPORTS_PER_ACCOUNT,
                recent_blockhash,
            )
            .unwrap();
    }
}
