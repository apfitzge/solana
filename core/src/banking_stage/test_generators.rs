//! Generators for testing banking stage
//!

use {
    rand::seq::SliceRandom,
    solana_runtime::bank::Bank,
    solana_sdk::{
        signer::Signer,
        transaction::{SanitizedTransaction, SanitizedVersionedTransaction, VersionedTransaction},
    },
};

pub type TransactionGenerator = Box<dyn Send + FnMut(&Bank) -> Vec<SanitizedTransaction>>;

/// Generates transfers between a set of accounts.
pub fn random_transfer_generator(num_accounts: usize) -> TransactionGenerator {
    let accounts = (0..num_accounts)
        .map(|_| solana_sdk::signer::keypair::Keypair::new())
        .collect::<Vec<_>>();
    Box::new(move |bank: &Bank| {
        let mut transactions = vec![];
        for _ in 0..64 {
            let from = accounts.choose(&mut rand::thread_rng()).unwrap();
            let to = accounts.choose(&mut rand::thread_rng()).unwrap();
            let transaction = solana_sdk::system_transaction::transfer(
                from,
                &to.pubkey(),
                1,
                bank.last_blockhash(),
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
