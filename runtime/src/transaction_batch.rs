use {
    crate::bank::Bank,
    solana_sdk::transaction::{Result, SanitizedTransaction},
    std::borrow::Cow,
};

pub trait TransactionBatch: Sized + Send + Sync {
    fn sanitized_transactions(&self) -> &[SanitizedTransaction];
    fn bank(&self) -> &Bank;
    fn lock_results(&self) -> &[Result<()>];
}

/// A transaction batch that does not use type-safe locking and unlocking.
/// Locks are expected to be handled outside the type, either explictly or
/// logically.
pub struct UnlockedTransactionBatch<'a, 'b> {
    bank: &'a Bank,
    sanitized_txs: Cow<'b, [SanitizedTransaction]>,
    dummy_lock_results: Vec<Result<()>>, // always Ok, here to satisfy trait.
}

impl<'a, 'b> TransactionBatch for UnlockedTransactionBatch<'a, 'b> {
    fn sanitized_transactions(&self) -> &[SanitizedTransaction] {
        &self.sanitized_txs
    }

    fn bank(&self) -> &Bank {
        &self.bank
    }

    fn lock_results(&self) -> &[Result<()>] {
        &self.dummy_lock_results
    }
}

impl<'a, 'b> UnlockedTransactionBatch<'a, 'b> {
    pub fn new(bank: &'a Bank, sanitized_txs: Cow<'b, [SanitizedTransaction]>) -> Self {
        let num_transactions = sanitized_txs.len();
        Self {
            bank,
            sanitized_txs,
            dummy_lock_results: vec![Ok(()); num_transactions],
        }
    }
}

/// A transaction batch that enforces type-safe locking and unlocking.
pub struct LockedTransactionBatch<'a, 'b> {
    bank: &'a Bank,
    sanitized_txs: Cow<'b, [SanitizedTransaction]>,
    lock_results: Vec<Result<()>>,
}

impl<'a, 'b> TransactionBatch for LockedTransactionBatch<'a, 'b> {
    fn sanitized_transactions(&self) -> &[SanitizedTransaction] {
        &self.sanitized_txs
    }

    fn bank(&self) -> &Bank {
        &self.bank
    }

    fn lock_results(&self) -> &[Result<()>] {
        &self.lock_results
    }
}

impl<'a, 'b> Drop for LockedTransactionBatch<'a, 'b> {
    fn drop(&mut self) {
        self.bank
            .rc
            .accounts
            .unlock_accounts(self.sanitized_txs.into_iter(), &self.lock_results)
    }
}

impl<'a, 'b> LockedTransactionBatch<'a, 'b> {
    pub fn new(bank: &'a Bank, sanitized_txs: Cow<'b, [SanitizedTransaction]>) -> Self {
        let lock_results = bank.rc.accounts.lock_accounts(
            sanitized_txs.iter(),
            bank.get_transaction_account_lock_limit(),
        );

        Self {
            bank,
            sanitized_txs,
            lock_results,
        }
    }

    pub fn new_with_results(
        bank: &'a Bank,
        sanitized_txs: Cow<'b, [SanitizedTransaction]>,
        transaction_results: impl Iterator<Item = Result<()>>,
    ) -> Self {
        let lock_results = bank.rc.accounts.lock_accounts_with_results(
            sanitized_txs.iter(),
            transaction_results,
            bank.get_transaction_account_lock_limit(),
        );

        Self {
            bank,
            sanitized_txs,
            lock_results,
        }
    }
}

// impl<'a, 'b> TransactionBatch<'a, 'b> {
//     pub fn new(
//         lock_results: Vec<Result<()>>,
//         bank: &'a Bank,
//         sanitized_txs: Cow<'b, [SanitizedTransaction]>,
//     ) -> Self {
//         assert_eq!(lock_results.len(), sanitized_txs.len());
//         Self {
//             lock_results,
//             bank,
//             sanitized_txs,
//             needs_unlock: true,
//         }
//     }

//     pub fn lock_results(&self) -> &Vec<Result<()>> {
//         &self.lock_results
//     }

//     pub fn sanitized_transactions(&self) -> &[SanitizedTransaction] {
//         &self.sanitized_txs
//     }

//     pub fn bank(&self) -> &Bank {
//         self.bank
//     }

//     pub fn set_needs_unlock(&mut self, needs_unlock: bool) {
//         self.needs_unlock = needs_unlock;
//     }

//     pub fn needs_unlock(&self) -> bool {
//         self.needs_unlock
//     }
// }

// // Unlock all locked accounts in destructor.
// impl<'a, 'b> Drop for TransactionBatch<'a, 'b> {
//     fn drop(&mut self) {
//         self.bank.unlock_accounts(self)
//     }
// }

// #[cfg(test)]
// mod tests {
//     use {
//         super::*,
//         crate::genesis_utils::{create_genesis_config_with_leader, GenesisConfigInfo},
//         solana_sdk::{signature::Keypair, system_transaction},
//     };

//     #[test]
//     fn test_transaction_batch() {
//         let (bank, txs) = setup();

//         // Test getting locked accounts
//         let batch = bank.prepare_sanitized_batch(&txs);

//         // Grab locks
//         assert!(batch.lock_results().iter().all(|x| x.is_ok()));

//         // Trying to grab locks again should fail
//         let batch2 = bank.prepare_sanitized_batch(&txs);
//         assert!(batch2.lock_results().iter().all(|x| x.is_err()));

//         // Drop the first set of locks
//         drop(batch);

//         // Now grabbing locks should work again
//         let batch2 = bank.prepare_sanitized_batch(&txs);
//         assert!(batch2.lock_results().iter().all(|x| x.is_ok()));
//     }

//     #[test]
//     fn test_simulation_batch() {
//         let (bank, txs) = setup();

//         // Prepare batch without locks
//         let batch = bank.prepare_simulation_batch(txs[0].clone());
//         assert!(batch.lock_results().iter().all(|x| x.is_ok()));

//         // Grab locks
//         let batch2 = bank.prepare_sanitized_batch(&txs);
//         assert!(batch2.lock_results().iter().all(|x| x.is_ok()));

//         // Prepare another batch without locks
//         let batch3 = bank.prepare_simulation_batch(txs[0].clone());
//         assert!(batch3.lock_results().iter().all(|x| x.is_ok()));
//     }

//     fn setup() -> (Bank, Vec<SanitizedTransaction>) {
//         let dummy_leader_pubkey = solana_sdk::pubkey::new_rand();
//         let GenesisConfigInfo {
//             genesis_config,
//             mint_keypair,
//             ..
//         } = create_genesis_config_with_leader(500, &dummy_leader_pubkey, 100);
//         let bank = Bank::new_for_tests(&genesis_config);

//         let pubkey = solana_sdk::pubkey::new_rand();
//         let keypair2 = Keypair::new();
//         let pubkey2 = solana_sdk::pubkey::new_rand();

//         let txs = vec![
//             SanitizedTransaction::from_transaction_for_tests(system_transaction::transfer(
//                 &mint_keypair,
//                 &pubkey,
//                 1,
//                 genesis_config.hash(),
//             )),
//             SanitizedTransaction::from_transaction_for_tests(system_transaction::transfer(
//                 &keypair2,
//                 &pubkey2,
//                 1,
//                 genesis_config.hash(),
//             )),
//         ];

//         (bank, txs)
//     }
// }
