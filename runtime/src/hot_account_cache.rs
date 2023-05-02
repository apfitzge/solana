//! Account cache for banking stage worker.
//! Keep account state in cache to avoid frequent load and store from accounts db
//! while the account is still used by queued transactions.

use {
    dashmap::DashMap,
    solana_sdk::{account::AccountSharedData, clock::Slot, pubkey::Pubkey},
    std::collections::HashMap,
};

#[derive(Debug)]
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
    /// Flush the hot cache into accounts db - DOES NOT EMPTY THE CACHE
    pub fn flush(&self) -> Vec<(Slot, Vec<(Pubkey, AccountSharedData)>)> {
        let mut slot_to_accounts: HashMap<Slot, Vec<(Pubkey, AccountSharedData)>> = HashMap::new();

        for entry in self.accounts.iter() {
            let slot = entry.slot;
            let account = entry.account.clone();
            let pubkey = entry.key().clone();
            slot_to_accounts
                .entry(slot)
                .or_default()
                .push((pubkey, account));
        }

        slot_to_accounts.into_iter().collect()
    }

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
                eprintln!(
                    "insert_account (existing): {pubkey:?}:  {entry:?} -> {slot:?} {account:?}"
                );
                if written {
                    entry.slot = slot;
                }
                entry.account = account;
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let cached_entry = HotAccountCacheEntry {
                    slot,
                    account,
                    written,
                };
                eprintln!("insert_account (new): {pubkey:?}:  {cached_entry:?}");
                entry.insert(cached_entry);
            }
        }
    }

    pub fn get_account(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        let account = self.accounts.get(pubkey).map(|a| a.value().account.clone());
        eprintln!("get_account: {pubkey:?} -> {account:?}");
        account
    }

    pub fn remove_account(&self, pubkey: &Pubkey) -> Option<HotAccountCacheEntry> {
        let account = self.accounts.remove(pubkey).map(|a| a.1);
        eprintln!("remove_account: {pubkey:?} -> {account:?}");
        account
    }
}
