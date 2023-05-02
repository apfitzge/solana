use {
    solana_poh::leader_bank_notifier::LeaderBankNotifier,
    solana_runtime::{
        accounts::Accounts, accounts_db::IncludeSlotInHash, hot_account_cache::HotAccountCache,
    },
    solana_sdk::{account::AccountSharedData, clock::Slot, pubkey::Pubkey},
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    },
};

pub struct HotCacheFlusher {
    exit: Arc<AtomicBool>,
    hot_account_cache: Arc<HotAccountCache>,
    leader_bank_notifier: Arc<LeaderBankNotifier>,
    accounts: Arc<Accounts>,
    include_slot_in_hash: IncludeSlotInHash,
}

impl HotCacheFlusher {
    pub fn new(
        exit: Arc<AtomicBool>,
        hot_account_cache: Arc<HotAccountCache>,
        leader_bank_notifier: Arc<LeaderBankNotifier>,
        accounts: Arc<Accounts>,
        include_slot_in_hash: IncludeSlotInHash,
    ) -> Self {
        Self {
            exit,
            hot_account_cache,
            leader_bank_notifier,
            accounts,
            include_slot_in_hash,
        }
    }

    pub fn run(self) {
        const WAIT_DURATION: Duration = Duration::from_millis(100);
        while !self.exit.load(Ordering::Relaxed) {
            if self
                .leader_bank_notifier
                .wait_for_completed(WAIT_DURATION)
                .is_some()
            {
                let written_accounts = self.hot_account_cache.flush();
                self.flush_accounts(written_accounts);
            }
        }
    }

    fn flush_accounts(&self, accounts_to_write: Vec<(Slot, Vec<(Pubkey, AccountSharedData)>)>) {
        for (slot, accounts) in accounts_to_write {
            let account_refs = accounts
                .iter()
                .map(|(pubkey, account)| (pubkey, account))
                .collect::<Vec<_>>();

            self.accounts.store_accounts_cached((
                slot,
                &account_refs[..],
                // TODO: This will screw us over epoch boundaries. Best to just wait until the feature is activated.
                self.include_slot_in_hash,
            ));
        }
    }
}
