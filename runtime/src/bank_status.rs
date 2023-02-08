//! Simple conditional variable to wait for a bank to be created.
//!

use {
    crate::bank::Bank,
    std::sync::{Arc, Condvar, Mutex, RwLock, Weak},
};

#[derive(Default)]
pub struct BankStatus {
    ready: Mutex<bool>,
    condvar: Condvar,
    bank: RwLock<Option<Weak<Bank>>>,
}

impl BankStatus {
    /// Mark the bank as not ready - may happen multiple times.
    pub fn bank_reached_end_of_slot(&self) {
        let mut ready = self.ready.lock().unwrap();
        self.bank.write().unwrap().take();
        *ready = false;
    }

    /// Mark the bank as created and notify all waiters.
    pub fn bank_created(&self, bank: &Arc<Bank>) {
        let mut ready = self.ready.lock().unwrap();
        self.bank.write().unwrap().replace(Arc::downgrade(bank));
        *ready = true;
        self.condvar.notify_all();
    }

    /// Check or wait for bank to be created.
    pub fn wait_for_bank(&self) -> Option<Weak<Bank>> {
        let ready = self.ready.lock().unwrap();
        if !*ready {
            let lock_result = self.condvar.wait(ready).unwrap();
            drop(lock_result);
        }
        self.bank.read().unwrap().clone()
    }
}
