use {
    crate::accounts::Accounts,
    log::info,
    solana_rpc_client::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig, hash::Hash, pubkey::Pubkey, signature::Keypair,
        signer::Signer, transaction::Transaction, transport::TransportError,
    },
    solana_tpu_client::{
        connection_cache::ConnectionCache,
        tpu_client::{TpuClient, TpuClientConfig},
        tpu_connection_cache::DEFAULT_TPU_CONNECTION_POOL_SIZE,
    },
    std::{collections::HashSet, sync::Arc, time::Duration},
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
        let num_accounts = accounts.contentious_accounts.len() + accounts.regular_accounts.len();

        const ACCOUNT_CHUNK_SIZE: usize = 100;

        let mut confirmed_funded_accounts = HashSet::with_capacity(num_accounts);
        while confirmed_funded_accounts.len() != num_accounts {
            info!(
                "Funding accounts progress: {}/{}",
                confirmed_funded_accounts.len(),
                num_accounts
            );
            // Do a funding loop
            let contentious_chunks = accounts.contentious_accounts.chunks(ACCOUNT_CHUNK_SIZE);
            let regular_chunks = accounts.regular_accounts.chunks(ACCOUNT_CHUNK_SIZE);
            contentious_chunks
                .chain(regular_chunks)
                .for_each(|accounts| {
                    self.fund_account_chunk(accounts, &confirmed_funded_accounts);
                });

            // Check for confirmation and update the confirmed_funded_accounts
            for pubkey in accounts
                .contentious_accounts
                .iter()
                .chain(accounts.regular_accounts.iter())
                .map(|keypair| keypair.pubkey())
                .filter(|pubkey| !confirmed_funded_accounts.contains(pubkey))
                .collect::<Vec<_>>()
            {
                match self.get_balance(&pubkey) {
                    Ok(balance) => {
                        if balance > 0 {
                            confirmed_funded_accounts.insert(pubkey);
                        }
                    }
                    Err(_) => {}
                };
            }

            std::thread::sleep(Duration::from_millis(100));
        }
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

    /// Get the balance of the given account.
    pub fn get_balance(&self, pubkey: &Pubkey) -> Result<u64, TransportError> {
        self.client
            .rpc_client()
            .get_balance_with_commitment(pubkey, CommitmentConfig::confirmed())
            .map(|res| res.value)
            .map_err(|err| err.into())
    }

    /// Fund a chunk of accounts
    fn fund_account_chunk(
        &self,
        accounts: &[Keypair],
        confirmed_funded_accounts: &HashSet<Pubkey>,
    ) {
        std::thread::sleep(Duration::from_millis(1));
        let recent_blockhash = self.get_recent_blockhash();
        accounts
            .iter()
            .filter(|keypair| !confirmed_funded_accounts.contains(&keypair.pubkey()))
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
