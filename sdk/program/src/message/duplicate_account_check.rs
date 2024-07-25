use {super::AccountKeys, crate::pubkey::Pubkey, ahash::AHashSet, std::cell::RefCell};

thread_local! {
    static SANITIZED_MESSAGE_HAS_DUPLICATES_SET: RefCell<AHashSet<Pubkey>> = RefCell::new(AHashSet::with_capacity(128));
}

pub fn has_duplicates(account_keys: AccountKeys) -> bool {
    SANITIZED_MESSAGE_HAS_DUPLICATES_SET.with_borrow_mut(|set| {
        let mut has_duplicates = false;
        for account_key in account_keys.iter() {
            has_duplicates |= !set.insert(*account_key);
        }
        set.clear();
        has_duplicates
    })
}
