//! An abstract interface for interacting with a transaction, which can be used
//! to inspect transactions inside plugin functions.
//!

use {
    core::ffi,
    solana_sdk::{hash::Hash, pubkey::Pubkey},
};

#[repr(C)]
pub struct TransactionInterface {
    /// Opaque pointer to the transaction type.
    pub ptr: *const ffi::c_void,

    /// Get the recent blockhash.
    pub get_recent_blockhash: GetRecentBlockhashFn,

    /// Get the account at the given index.
    pub get_account: GetAccountFn,

    /// Iterate over the transaction's accounts.
    pub iterate_accounts: IterateAccountsFn,
}

/// For a given transaction pointer, return the recent blockhash.
pub type GetRecentBlockhashFn = unsafe extern "C" fn(ptr: *const ffi::c_void) -> *const Hash;

/// Returns a pointer to the account at the given index.
/// The account pointer is only valid for the lifetime of the transaction.
/// The account pointer is null if the index is out of bounds.
pub type GetAccountFn = unsafe extern "C" fn(ptr: *const ffi::c_void, index: u32) -> *const Pubkey;

/// The callback function is called for each account in the transaction.
/// If the callback function returns false, the iteration is stopped.
/// If the callback function returns true, the iteration continues.
///
/// * `account` - The account's public key.
/// * `callback_state` - Pointer to arbitrary state passed to the callback function.
pub type AccountCallbackFn =
    unsafe extern "C" fn(account: *const Pubkey, callback_state: *mut core::ffi::c_void) -> bool;

/// An account callback.
#[repr(C)]
pub struct AccountCallback {
    /// Arbitrary state passed to the callback function.
    pub state: *mut ffi::c_void,
    /// The callback function to call for each account.
    pub function: AccountCallbackFn,
}

/// Iterate over the transaction's accounts.
///
/// * `ptr` - Pointer to the transaction.
/// * `cb` - The callback struct holding state and the callback function.
pub type IterateAccountsFn = unsafe extern "C" fn(ptr: *const ffi::c_void, cb: AccountCallback);

pub mod sanitized_transaction {
    //! Implements interface for the `SanitizedTransaction` type.

    use {super::*, solana_sdk::transaction::SanitizedTransaction};

    /// Get pointer to the recent blockhash for the transaction.
    /// The blockhash is only valid for the lifetime of the transaction.
    ///
    /// # Safety
    /// The transaction pointer must be valid.
    #[no_mangle]
    pub unsafe extern "C" fn get_recent_blockhash(ptr: *const ffi::c_void) -> *const Hash {
        let tx = &*(ptr as *const SanitizedTransaction);
        tx.message().recent_blockhash()
    }

    /// Get the account at the given index for the transaction.
    /// The account pointer is only valid for the lifetime of the transaction.
    /// The account pointer is null if the index is out of bounds.
    ///
    /// # Safety
    /// The transaction pointer must be valid.
    #[no_mangle]
    pub unsafe extern "C" fn get_account(ptr: *const ffi::c_void, index: u32) -> *const Pubkey {
        let tx = &*(ptr as *const SanitizedTransaction);
        tx.message()
            .account_keys()
            .get(index as usize)
            .map(|k| k as *const Pubkey)
            .unwrap_or(std::ptr::null())
    }

    /// Iterate over the transaction's accounts.
    /// The callback function is called for each account in the transaction.
    /// If the callback function returns false, the iteration is stopped.
    /// If the callback function returns true, the iteration continues.
    /// The callback function is called with the account's public key.
    ///
    /// # Safety
    /// The transaction pointer must be valid.
    /// The callback function must be valid.
    #[no_mangle]
    pub unsafe extern "C" fn iterate_accounts(ptr: *const ffi::c_void, cb: AccountCallback) {
        let tx = &*(ptr as *const SanitizedTransaction);
        for account in tx.message().account_keys().iter() {
            let account = account as *const Pubkey;
            if !(cb.function)(account, cb.state) {
                break;
            }
        }
    }

    // This function is intended to be called by the rust code in order to set up the
    // transaction interface in order to be passed to plugin functions.
    pub fn create_transaction_interface(
        sanitized_transaction: &SanitizedTransaction,
    ) -> TransactionInterface {
        TransactionInterface {
            ptr: sanitized_transaction as *const SanitizedTransaction as *const ffi::c_void,
            get_recent_blockhash,
            get_account,
            iterate_accounts,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_sdk::{signature::Keypair, system_transaction, transaction::SanitizedTransaction},
    };

    #[test]
    fn test_sanitized_transaction_interface() {
        let simple_transfer =
            SanitizedTransaction::from_transaction_for_tests(system_transaction::transfer(
                &Keypair::new(),
                &Pubkey::new_unique(),
                1,
                Hash::default(),
            ));

        let tx_interface = sanitized_transaction::create_transaction_interface(&simple_transfer);

        let recent_blockhash = unsafe { *(tx_interface.get_recent_blockhash)(tx_interface.ptr) };
        assert_eq!(
            recent_blockhash,
            *simple_transfer.message().recent_blockhash()
        );

        let account = unsafe { *(tx_interface.get_account)(tx_interface.ptr, 0) };
        assert_eq!(account, simple_transfer.message().account_keys()[0]);

        // 3 keys: from, to, system_program
        let out_of_index_account = unsafe { (tx_interface.get_account)(tx_interface.ptr, 3) };
        assert_eq!(out_of_index_account, std::ptr::null());

        struct AccountCallbackState {
            account_count: u64,
        }

        unsafe extern "C" fn account_callback_function(
            _account: *const Pubkey,
            callback_state: *mut ffi::c_void,
        ) -> bool {
            let account_count = unsafe { &mut *(callback_state as *mut AccountCallbackState) };
            account_count.account_count += 1;
            true
        }

        let mut account_callback_state = AccountCallbackState { account_count: 0 };
        let account_callback = AccountCallback {
            state: &mut account_callback_state as *mut AccountCallbackState as *mut ffi::c_void,
            function: account_callback_function,
        };
        unsafe {
            (tx_interface.iterate_accounts)(tx_interface.ptr, account_callback);
        }
        assert_eq!(account_callback_state.account_count, 3);
    }
}
