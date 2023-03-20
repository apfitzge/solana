//! Generators for testing banking stage
//!

use {
    rand::{rngs::ThreadRng, seq::SliceRandom},
    serde::Deserialize,
    solana_runtime::bank::Bank,
    solana_sdk::{
        clock::MAX_PROCESSING_AGE,
        hash::Hash,
        signature::Keypair,
        signer::Signer,
        transaction::{SanitizedTransaction, SanitizedVersionedTransaction, VersionedTransaction},
    },
    std::{path::Path, sync::Arc},
};

pub type TransactionGenerator =
    Box<dyn Send + FnMut(&mut ThreadRng, &Bank) -> Vec<SanitizedTransaction>>;

struct AccountsFile {
    payers: Vec<Keypair>,
    _allocated_accounts: Vec<Keypair>,
}

impl From<AccountsFileRaw> for AccountsFile {
    fn from(raw: AccountsFileRaw) -> Self {
        let payers = raw.payers.into_iter().map(|r| r.into()).collect();
        let allocated_accounts = raw
            .allocated_accounts
            .into_iter()
            .map(|r| r.into())
            .collect();

        Self {
            payers,
            _allocated_accounts: allocated_accounts,
        }
    }
}

#[derive(Deserialize)]
struct AccountsFileRaw {
    payers: Vec<KeypairRaw>,
    #[serde(rename = "allocatedAccounts")]
    allocated_accounts: Vec<KeypairRaw>,
}

#[derive(Deserialize)]
struct KeypairRaw {
    #[serde(rename = "publicKey")]
    pub _pubkey: String,
    #[serde(rename = "secretKey")]
    pub secret_key: Vec<u8>,
}

impl From<KeypairRaw> for Keypair {
    fn from(raw: KeypairRaw) -> Self {
        Self::from_bytes(&raw.secret_key).unwrap()
    }
}

fn get_accounts(
    accounts_path: impl AsRef<Path>,
    starting_keypairs: Arc<Vec<Keypair>>,
) -> Vec<Keypair> {
    debug!(
        "Saving accounts for {} starting keypairs",
        starting_keypairs.len()
    );
    if !starting_keypairs.is_empty() {
        starting_keypairs
            .iter()
            .map(|keypair| Keypair::from_base58_string(&keypair.to_base58_string()))
            .collect()
    } else {
        let accounts_file_string = std::fs::read_to_string(accounts_path).unwrap();
        let accounts: AccountsFile = serde_json::from_str::<AccountsFileRaw>(&accounts_file_string)
            .unwrap()
            .into();
        accounts.payers
    }
}

/// Generates transfers between a set of accounts.
pub fn random_transfer_generator(
    accounts_path: impl AsRef<Path>,
    starting_keypairs: Arc<Vec<Keypair>>,
) -> TransactionGenerator {
    let accounts = get_accounts(accounts_path, starting_keypairs);
    let mut blockhash_buffer = BlockhashCircleBuffer::default();
    Box::new(move |rng: &mut ThreadRng, bank: &Bank| {
        const BATCH_SIZE: usize = 64;
        let mut transactions = vec![];
        blockhash_buffer.check_and_push(bank.last_blockhash());

        let mut accounts = accounts.choose_multiple(rng, BATCH_SIZE * 2);
        for _ in 0..BATCH_SIZE {
            let transaction = solana_sdk::system_transaction::transfer(
                accounts.next().unwrap(),
                &accounts.next().unwrap().pubkey(),
                1,
                *blockhash_buffer.buffer.choose(rng).unwrap(),
            );

            let message_hash = transaction.message().hash();
            let versioned_tx: VersionedTransaction = transaction.into();
            let sanitized_versioned_tx: SanitizedVersionedTransaction =
                versioned_tx.try_into().unwrap();
            let sanitized_tx =
                SanitizedTransaction::try_new(sanitized_versioned_tx, message_hash, false, bank)
                    .unwrap();
            transactions.push(sanitized_tx);
        }
        transactions
    })
}

/// Allocates random large accounts.
pub fn random_allocate_generator(
    accounts_path: impl AsRef<Path>,
    starting_keypairs: Arc<Vec<Keypair>>,
) -> TransactionGenerator {
    let accounts = get_accounts(accounts_path, starting_keypairs);
    let mut blockhash_buffer = BlockhashCircleBuffer::default();
    Box::new(move |rng: &mut ThreadRng, bank: &Bank| {
        const BATCH_SIZE: usize = 64;
        const ACCOUNT_SIZE: u64 = 100 * 1024;
        let mut transactions = vec![];
        blockhash_buffer.check_and_push(bank.last_blockhash());

        let mut accounts = accounts.choose_multiple(rng, BATCH_SIZE * 2);
        for _ in 0..BATCH_SIZE {
            let transaction = solana_sdk::system_transaction::allocate(
                accounts.next().unwrap(),
                &Keypair::new(),
                *blockhash_buffer.buffer.choose(rng).unwrap(),
                ACCOUNT_SIZE,
            );

            let message_hash = transaction.message().hash();
            let versioned_tx: VersionedTransaction = transaction.into();
            let sanitized_versioned_tx: SanitizedVersionedTransaction =
                versioned_tx.try_into().unwrap();
            let sanitized_tx =
                SanitizedTransaction::try_new(sanitized_versioned_tx, message_hash, false, bank)
                    .unwrap();
            transactions.push(sanitized_tx);
        }
        transactions
    })
}

#[derive(Default)]
struct BlockhashCircleBuffer {
    buffer: Vec<Hash>,
    curr: usize,
    prev: usize,
}

impl BlockhashCircleBuffer {
    #[inline]
    fn check_and_push(&mut self, hash: Hash) {
        if self.buffer.len() < MAX_PROCESSING_AGE {
            self.buffer.push(hash);
            return;
        }

        if hash != self.buffer[self.prev] {
            self.buffer[self.curr] = hash;
            self.prev = self.curr;
            self.curr = (self.curr + 1) % MAX_PROCESSING_AGE;
        }
    }
}
