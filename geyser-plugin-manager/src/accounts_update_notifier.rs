/// Module responsible for notifying plugins of account updates
use {
    crate::geyser_plugin_manager::GeyserPluginManager,
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV4, ReplicaAccountInfoVersions,
    },
    agave_transaction_ffi::TransactionInterface,
    log::*,
    solana_accounts_db::{
        account_storage::meta::StoredAccountMeta,
        accounts_update_notifier_interface::AccountsUpdateNotifierInterface,
    },
    solana_measure::measure::Measure,
    solana_metrics::*,
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        clock::Slot,
        pubkey::Pubkey,
    },
    std::sync::{Arc, RwLock},
};
#[derive(Debug)]
pub(crate) struct AccountsUpdateNotifierImpl {
    plugin_manager: Arc<RwLock<GeyserPluginManager>>,
}

impl AccountsUpdateNotifierInterface for AccountsUpdateNotifierImpl {
    fn notify_account_update(
        &self,
        slot: Slot,
        account: &AccountSharedData,
        txn: Option<&TransactionInterface>,
        pubkey: &Pubkey,
        write_version: u64,
    ) {
        let account_info =
            self.accountinfo_from_shared_account_data(account, txn, pubkey, write_version);
        self.notify_plugins_of_account_update(account_info, slot, false);
    }

    fn notify_account_restore_from_snapshot(&self, slot: Slot, account: &StoredAccountMeta) {
        let mut measure_all = Measure::start("geyser-plugin-notify-account-restore-all");
        let mut measure_copy = Measure::start("geyser-plugin-copy-stored-account-info");

        let account = self.accountinfo_from_stored_account_meta(account);
        measure_copy.stop();

        inc_new_counter_debug!(
            "geyser-plugin-copy-stored-account-info-us",
            measure_copy.as_us() as usize,
            100000,
            100000
        );

        self.notify_plugins_of_account_update(account, slot, true);

        measure_all.stop();

        inc_new_counter_debug!(
            "geyser-plugin-notify-account-restore-all-us",
            measure_all.as_us() as usize,
            100000,
            100000
        );
    }

    fn notify_end_of_restore_from_snapshot(&self) {
        let plugin_manager = self.plugin_manager.read().unwrap();
        if plugin_manager.plugins.is_empty() {
            return;
        }

        for plugin in plugin_manager.plugins.iter() {
            let mut measure = Measure::start("geyser-plugin-end-of-restore-from-snapshot");
            match plugin.notify_end_of_startup() {
                Err(err) => {
                    error!(
                        "Failed to notify the end of restore from snapshot, error: {} to plugin {}",
                        err,
                        plugin.name()
                    )
                }
                Ok(_) => {
                    trace!(
                        "Successfully notified the end of restore from snapshot to plugin {}",
                        plugin.name()
                    );
                }
            }
            measure.stop();
            inc_new_counter_debug!(
                "geyser-plugin-end-of-restore-from-snapshot",
                measure.as_us() as usize
            );
        }
    }
}

impl AccountsUpdateNotifierImpl {
    pub fn new(plugin_manager: Arc<RwLock<GeyserPluginManager>>) -> Self {
        AccountsUpdateNotifierImpl { plugin_manager }
    }

    fn accountinfo_from_shared_account_data<'a>(
        &self,
        account: &'a AccountSharedData,
        txn: Option<&'a TransactionInterface<'a>>,
        pubkey: &'a Pubkey,
        write_version: u64,
    ) -> ReplicaAccountInfoV4<'a> {
        ReplicaAccountInfoV4 {
            pubkey: pubkey.as_ref().as_ptr(),
            lamports: account.lamports(),
            owner: account.owner().as_ref().as_ptr(),
            executable: account.executable(),
            rent_epoch: account.rent_epoch(),
            data_length: account.data().len(),
            data_ptr: account.data().as_ptr(),
            write_version,
            txn,
            _lifetime: core::marker::PhantomData::<&'a ()>,
        }
    }

    fn accountinfo_from_stored_account_meta<'a>(
        &self,
        stored_account_meta: &'a StoredAccountMeta,
    ) -> ReplicaAccountInfoV4<'a> {
        // We do not need to rely on the specific write_version read from the append vec.
        // So, overwrite the write_version with something that works.
        // There is already only entry per pubkey.
        // write_version is only used to order multiple entries with the same pubkey,
        // so it doesn't matter what value it gets here.
        // Passing 0 for everyone's write_version is sufficiently correct.
        let write_version = 0;
        ReplicaAccountInfoV4 {
            pubkey: stored_account_meta.pubkey().as_ref().as_ptr(),
            lamports: stored_account_meta.lamports(),
            owner: stored_account_meta.owner().as_ref().as_ptr(),
            executable: stored_account_meta.executable(),
            rent_epoch: stored_account_meta.rent_epoch(),
            data_length: stored_account_meta.data().len(),
            data_ptr: stored_account_meta.data().as_ptr(),
            write_version,
            txn: None,
            _lifetime: core::marker::PhantomData::<&'a ()>,
        }
    }

    fn notify_plugins_of_account_update(
        &self,
        account: ReplicaAccountInfoV4,
        slot: Slot,
        is_startup: bool,
    ) {
        let mut measure2 = Measure::start("geyser-plugin-notify_plugins_of_account_update");
        let plugin_manager = self.plugin_manager.read().unwrap();

        if plugin_manager.plugins.is_empty() {
            return;
        }
        for plugin in plugin_manager.plugins.iter() {
            let mut measure = Measure::start("geyser-plugin-update-account");
            match plugin.update_account(
                ReplicaAccountInfoVersions::V0_0_4(&account),
                slot,
                is_startup,
            ) {
                Err(err) => {
                    error!(
                        "Failed to update account {} at slot {}, error: {} to plugin {}",
                        bs58::encode(account.pubkey()).into_string(),
                        slot,
                        err,
                        plugin.name()
                    )
                }
                Ok(_) => {
                    trace!(
                        "Successfully updated account {} at slot {} to plugin {}",
                        // SAFETY: The pubkey is valid for lifetime of `account`.
                        bs58::encode(account.pubkey()).into_string(),
                        slot,
                        plugin.name()
                    );
                }
            }
            measure.stop();
            inc_new_counter_debug!(
                "geyser-plugin-update-account-us",
                measure.as_us() as usize,
                100000,
                100000
            );
        }
        measure2.stop();
        inc_new_counter_debug!(
            "geyser-plugin-notify_plugins_of_account_update-us",
            measure2.as_us() as usize,
            100000,
            100000
        );
    }
}
