use {
    criterion::{criterion_group, criterion_main, Criterion},
    solana_sdk::{
        hash::Hash, pubkey::Pubkey, signature::Keypair, system_transaction,
        transaction::SanitizedTransaction,
    },
    solana_transaction_ffi::{
        sanitized_transaction::create_transaction_interface, AccountCallback,
    },
};

fn create_transaction() -> SanitizedTransaction {
    SanitizedTransaction::from_transaction_for_tests(system_transaction::transfer(
        &Keypair::new(),
        &Pubkey::new_unique(),
        1,
        Hash::new_unique(),
    ))
}

struct CallbackState {
    pub num_zeros: u64,
}

// Some random fn that does some operation on an account.
unsafe extern "C" fn per_account_call(key: *const Pubkey, state: *mut core::ffi::c_void) -> bool {
    let key = &unsafe { *key };
    let state = &mut *(state as *mut CallbackState);

    per_account_call_native(key, state);

    true
}

fn per_account_call_native(key: &Pubkey, state: &mut CallbackState) {
    for byte in key.to_bytes().iter() {
        state.num_zeros += u64::from(*byte == 0);
    }
}

fn bench_transaction_interface(c: &mut Criterion) {
    let tx = create_transaction();
    c.bench_function("transaction_ffi", |b| {
        b.iter(|| {
            let tx_interface = create_transaction_interface(&tx);
            let mut state = CallbackState { num_zeros: 0 };
            let callback = AccountCallback {
                state: &mut state as *mut _ as *mut core::ffi::c_void,
                function: per_account_call,
            };
            unsafe { (tx_interface.iterate_accounts)(tx_interface.ptr, callback) };
        })
    });
}

fn bench_transaction_native(c: &mut Criterion) {
    let tx = create_transaction();
    c.bench_function("transaction_native", |b| {
        b.iter(|| {
            let mut state = CallbackState { num_zeros: 0 };
            for account in tx.message().account_keys().iter() {
                per_account_call_native(account, &mut state);
            }
        })
    });
}

criterion_group!(
    benches,
    bench_transaction_interface,
    bench_transaction_native
);
criterion_main!(benches);
