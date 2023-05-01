use {
    crate::banking_stage::scheduler_messages::TransactionId,
    std::sync::atomic::{AtomicU64, Ordering},
};

pub(crate) struct TransactionIdGenerator {
    index: AtomicU64,
}

impl Default for TransactionIdGenerator {
    fn default() -> Self {
        Self {
            index: AtomicU64::new(u64::MAX),
        }
    }
}

impl TransactionIdGenerator {
    pub(crate) fn next_batch(&self, num: u64) -> impl Iterator<Item = TransactionId> {
        let index = self.index.fetch_sub(num, Ordering::Relaxed);
        (index - num..index).rev().map(TransactionId::new)
    }
}
