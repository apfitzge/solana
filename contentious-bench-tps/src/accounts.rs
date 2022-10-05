use solana_sdk::signature::Keypair;

/// Store accounts used for generating contentious and regular transfers.
#[derive(Debug)]
pub struct Accounts {
    /// Contentious accounts - used to create transactions that have many conflicts.
    pub contentious_accounts: Vec<Keypair>,
    /// Non-contentious accounts - used to create transactions with few conflicts.
    pub regular_accounts: Vec<Keypair>,
}

impl Accounts {
    /// Generates sets of unique accounts for contentious and non-contentious transfers.
    pub fn new(
        num_contentious_transfer_accounts: usize,
        num_regular_transfer_accounts: usize,
    ) -> Self {
        Self {
            contentious_accounts: Self::generate_accounts(num_contentious_transfer_accounts),
            regular_accounts: Self::generate_accounts(num_regular_transfer_accounts),
        }
    }

    /// Generate a random set of accounts.
    fn generate_accounts(num_accounts: usize) -> Vec<Keypair> {
        (0..num_accounts).map(|_| Keypair::new()).collect()
    }
}
