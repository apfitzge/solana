//! Account cache for banking stage worker.
//! Keep account state in cache to avoid frequent load and store from accounts db
//! while the account is still used by queued transactions.

use {
    dashmap::DashMap,
    solana_sdk::{account::AccountSharedData, pubkey::Pubkey},
};

pub struct HotAccountCache {
    accounts: DashMap<Pubkey, AccountSharedData>,
}

impl HotAccountCache {
    pub fn insert_account(&self, pubkey: &Pubkey, account: AccountSharedData) {
        self.accounts.insert(*pubkey, account);
    }

    pub fn get_account(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        self.accounts.get(pubkey).map(|a| a.value().clone())
    }

    pub fn remove_account(&self, pubkey: &Pubkey) -> AccountSharedData {
        self.accounts.remove(pubkey).expect("account must exist").1
    }
}
