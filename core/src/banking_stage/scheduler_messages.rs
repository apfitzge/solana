use {
    super::{
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        transaction_scheduler::transaction_priority_id::TransactionPriorityId,
    },
    solana_sdk::{clock::Slot, transaction::SanitizedTransaction},
    std::{
        fmt::{Display, Formatter},
        sync::Arc,
    },
};

/// A unique identifier for a transaction batch.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct TransactionBatchId(u64);

impl TransactionBatchId {
    pub fn new(index: u64) -> Self {
        Self(index)
    }
}

/// A unique identifier for a transaction.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct TransactionId(u64);

impl TransactionId {
    pub fn new(index: u64) -> Self {
        Self(index)
    }
}

impl Display for TransactionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Message: [Scheduler -> Worker]
/// Transactions to be consumed (i.e. executed, recorded, and committed)
pub struct ConsumeWork {
    pub batch_id: TransactionBatchId,
    pub ids: Vec<TransactionPriorityId>,
    pub transactions: Vec<SanitizedTransaction>,
    /// Sentinal value of 0 ON return to indicate the transaction expired
    pub max_age_slots: Vec<Slot>,
}

/// Message: [Scheduler -> Worker]
/// Transactions to be forwarded to the next leader(s)
pub struct ForwardWork {
    pub ids: Vec<TransactionPriorityId>,
    pub packets: Vec<Arc<ImmutableDeserializedPacket>>,
}

/// Message: [Worker -> Scheduler]
/// Processed transactions.
pub struct FinishedConsumeWork {
    pub work: ConsumeWork,
    pub retryable_indexes: Vec<usize>,
}

/// Message: [Worker -> Scheduler]
/// Forwarded transactions.
pub struct FinishedForwardWork {
    pub work: ForwardWork,
    pub successful: bool,
}
