//! Account cache for banking stage worker.
//! Keep account state in cache to avoid frequent load and store from accounts db
//! while the account is still used by queued transactions.

use {
    dashmap::DashMap,
    solana_sdk::{account::AccountSharedData, clock::Slot, pubkey::Pubkey},
};

pub struct HotAccountCacheEntry {
    pub slot: Slot,
    pub account: AccountSharedData,
    pub written: bool,
}

#[derive(Default)]
pub struct HotAccountCache {
    accounts: DashMap<Pubkey, HotAccountCacheEntry>,
}

impl HotAccountCache {
    pub fn insert_account(
        &self,
        pubkey: Pubkey,
        slot: Slot,
        account: AccountSharedData,
        written: bool,
    ) {
        match self.accounts.entry(pubkey) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                if written {
                    entry.slot = slot;
                }
                entry.account = account;
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(HotAccountCacheEntry {
                    slot,
                    account,
                    written,
                });
            }
        }
    }

    pub fn get_account(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        self.accounts.get(pubkey).map(|a| a.value().account.clone())
    }

    pub fn remove_account(&self, pubkey: &Pubkey) -> HotAccountCacheEntry {
        self.accounts.remove(pubkey).expect("account must exist").1
    }
}
