use crate::banking_stage::scheduler_messages::TransactionId;

/// Simple reverse-sequential ID generator for `TransactionId`s.
/// These IDs uniquely identify transactions during the scheduling process.
pub struct TransactionIdGenerator {
    next_id: u32,
}

impl Default for TransactionIdGenerator {
    fn default() -> Self {
        Self { next_id: u32::MAX }
    }
}

impl TransactionIdGenerator {
    pub fn next(&mut self) -> TransactionId {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_sub(1);
        id
    }
}
